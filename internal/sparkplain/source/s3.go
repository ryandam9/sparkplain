package source

import (
	"context"
	"errors"
	"fmt"
	"io"
	"sort"
	"strings"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/feature/s3/manager"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"
	"github.com/aws/smithy-go"
)

// S3API is the part of the S3 client sparkplain uses: reads only. Tests
// stub it.
type S3API interface {
	ListObjectsV2(ctx context.Context, in *s3.ListObjectsV2Input, opts ...func(*s3.Options)) (*s3.ListObjectsV2Output, error)
	GetObject(ctx context.Context, in *s3.GetObjectInput, opts ...func(*s3.Options)) (*s3.GetObjectOutput, error)
}

// S3Store reads one bucket.
type S3Store struct {
	api    S3API
	bucket string
}

// NewS3Store wraps a client for one bucket.
func NewS3Store(api S3API, bucket string) *S3Store { return &S3Store{api: api, bucket: bucket} }

// LoadAWS loads credentials and region for a named profile ("" for the
// default chain), with region overriding the profile's when set.
func LoadAWS(ctx context.Context, profile, region string) (aws.Config, error) {
	var opts []func(*config.LoadOptions) error
	if profile != "" {
		opts = append(opts, config.WithSharedConfigProfile(profile))
	}
	if region != "" {
		opts = append(opts, config.WithRegion(region))
	}
	return config.LoadDefaultConfig(ctx, opts...)
}

// OpenS3 returns a store for bucket, with a client in the bucket's own
// region (found with HeadBucket), so a profile in another region still
// works.
func OpenS3(ctx context.Context, cfg aws.Config, bucket string) (*S3Store, error) {
	client := s3.NewFromConfig(cfg)
	region, err := manager.GetBucketRegion(ctx, client, bucket)
	if err != nil {
		return nil, classify(bucket, err)
	}
	if region != cfg.Region {
		client = s3.NewFromConfig(cfg, func(o *s3.Options) { o.Region = region })
	}
	return NewS3Store(client, bucket), nil
}

func (s *S3Store) Location(key string) string { return "s3://" + s.bucket + "/" + key }

func (s *S3Store) List(ctx context.Context, prefix string) ([]Object, error) {
	var out []Object
	p := s3.NewListObjectsV2Paginator(s.api, &s3.ListObjectsV2Input{
		Bucket: aws.String(s.bucket), Prefix: aws.String(prefix),
		OptionalObjectAttributes: []types.OptionalObjectAttributes{types.OptionalObjectAttributesRestoreStatus},
	})
	for p.HasMorePages() {
		page, err := p.NextPage(ctx)
		if err != nil {
			return out, classify(s.Location(prefix), err)
		}
		for _, o := range page.Contents {
			obj := Object{Key: aws.ToString(o.Key), Size: aws.ToInt64(o.Size), ETag: aws.ToString(o.ETag),
				Modified: aws.ToTime(o.LastModified), StorageClass: string(o.StorageClass)}
			switch o.StorageClass {
			case types.ObjectStorageClassGlacier, types.ObjectStorageClassDeepArchive:
				restored := o.RestoreStatus != nil && !aws.ToBool(o.RestoreStatus.IsRestoreInProgress) && o.RestoreStatus.RestoreExpiryDate != nil
				obj.Archived = !restored
			}
			out = append(out, obj)
		}
	}
	sort.Slice(out, func(i, j int) bool { return out[i].Key < out[j].Key })
	return out, nil
}

func (s *S3Store) Open(ctx context.Context, obj Object) (io.ReadCloser, error) {
	if obj.Archived {
		return nil, &Error{Class: ClassArchived, Key: s.Location(obj.Key), Err: errors.New("archived in " + obj.StorageClass + " and not restored")}
	}
	in := &s3.GetObjectInput{Bucket: aws.String(s.bucket), Key: aws.String(obj.Key)}
	if obj.ETag != "" {
		in.IfMatch = aws.String(obj.ETag)
	}
	out, err := s.api.GetObject(ctx, in)
	if err != nil {
		return nil, classify(s.Location(obj.Key), err)
	}
	return out.Body, nil
}

// classify maps an AWS error to a Sources-panel class.
func classify(where string, err error) error {
	class := ClassOther
	var api smithy.APIError
	switch {
	case errors.Is(err, context.DeadlineExceeded):
		class = ClassTimeout
	case IsNoAccess(err):
		class = ClassAccessDenied
	case errors.As(err, &api):
		switch code := api.ErrorCode(); {
		case code == "AccessDenied" || code == "Forbidden" || code == "AllAccessDisabled" || strings.HasPrefix(code, "InvalidAccessKey") || code == "ExpiredToken":
			class = ClassAccessDenied
		case code == "NoSuchKey" || code == "NoSuchBucket" || code == "NotFound":
			class = ClassNotFound
		case code == "SlowDown" || code == "Throttling" || code == "ThrottlingException" || code == "RequestLimitExceeded":
			class = ClassThrottled
		case code == "PreconditionFailed":
			class = ClassChanged
		case code == "InvalidObjectState":
			class = ClassArchived
		}
	}
	return &Error{Class: class, Key: where, Err: fmt.Errorf("%w", err)}
}
