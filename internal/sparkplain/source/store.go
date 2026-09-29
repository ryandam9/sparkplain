// Package source lists and reads the logs sparkplain works from, in S3 or in
// a local folder laid out like S3 (SPEC §2). Every AWS call it makes is
// read-only: ListObjectsV2, GetObject and HeadBucket.
package source

import (
	"context"
	"errors"
	"fmt"
	"io"
	"strings"
	"time"

	"github.com/aws/smithy-go"
)

// Object is one listed log file.
type Object struct {
	Key          string
	Size         int64
	ETag         string
	Modified     time.Time
	StorageClass string
	// Archived is true for Glacier objects that have not been restored:
	// reading them fails until someone restores them.
	Archived bool
}

// Store lists and opens objects under a root.
type Store interface {
	// List returns every object under prefix, in key order.
	List(ctx context.Context, prefix string) ([]Object, error)
	// Head returns the object named exactly key, and false when there is
	// none, without listing anything under it.
	Head(ctx context.Context, key string) (Object, bool, error)
	// Open reads obj, failing with ClassChanged if it changed since it was
	// listed.
	Open(ctx context.Context, obj Object) (io.ReadCloser, error)
	// Location names a key for people: s3://bucket/key or a local path.
	Location(key string) string
}

// Sampler lists at most n objects under prefix: enough to tell a readable
// prefix from an empty or refused one without listing all of it.
type Sampler interface {
	Sample(ctx context.Context, prefix string, n int) ([]Object, error)
}

// Sample lists at most n objects under prefix, with st's own Sample when
// it has one (the access check, SPEC §2).
func Sample(ctx context.Context, st Store, prefix string, n int) ([]Object, error) {
	if s, ok := st.(Sampler); ok {
		return s.Sample(ctx, prefix, n)
	}
	objs, err := st.List(ctx, prefix)
	if len(objs) > n {
		objs = objs[:n]
	}
	return objs, err
}

// Error classes shown in the report's Sources panel (SPEC §2).
const (
	ClassAccessDenied = "accessDenied"
	ClassNotFound     = "notFound"
	ClassThrottled    = "throttled"
	ClassTimeout      = "timeout"
	ClassArchived     = "archivedUnavailable"
	ClassCorrupt      = "corrupt"
	ClassTooLarge     = "tooLarge"
	ClassChanged      = "changed" // the object changed between listing and reading
	ClassOther        = "other"
)

// Error is a failure with its class.
type Error struct {
	Class string
	Key   string
	Err   error
}

func (e *Error) Error() string { return fmt.Sprintf("%s: %s: %v", e.Key, e.Class, e.Err) }
func (e *Error) Unwrap() error { return e.Err }

// ClassOf returns an error's class, or ClassOther.
func ClassOf(err error) string {
	var e *Error
	if errors.As(err, &e) {
		return e.Class
	}
	if errors.Is(err, context.DeadlineExceeded) {
		return ClassTimeout
	}
	if IsNoAccess(err) {
		return ClassAccessDenied
	}
	return ClassOther
}

// noAccessCodes are AWS error codes that mean the caller may not do this,
// or its credentials are missing, expired or wrong.
var noAccessCodes = map[string]bool{"AccessDenied": true, "AccessDeniedException": true, "Forbidden": true, "AllAccessDisabled": true,
	"UnauthorizedOperation": true, "AuthFailure": true, "ExpiredToken": true, "ExpiredTokenException": true, "InvalidClientTokenId": true,
	"UnrecognizedClientException": true, "InvalidAccessKeyId": true, "SignatureDoesNotMatch": true, "MissingAuthenticationToken": true}

// noAccessText is what the SDK says, without an AWS error code, when it
// has no usable credentials at all.
var noAccessText = []string{"failed to retrieve credentials", "no EC2 IMDS role found", "failed to refresh cached credentials",
	"failed to get shared config profile", "SharedConfigProfileNotExist", "the SSO session has expired", "token has expired", "no valid providers in chain"}

// IsNoAccess reports whether err means sparkplain was not allowed to make
// a call: a refused permission, or credentials that are missing, expired
// or wrong.
func IsNoAccess(err error) bool {
	if err == nil {
		return false
	}
	var ae smithy.APIError
	if errors.As(err, &ae) && noAccessCodes[ae.ErrorCode()] {
		return true
	}
	msg := err.Error()
	for _, t := range noAccessText {
		if strings.Contains(msg, t) {
			return true
		}
	}
	return false
}

// ParseS3 splits s3://bucket/key (or s3a://) into bucket and key.
func ParseS3(u string) (bucket, key string, ok bool) {
	for _, scheme := range []string{"s3://", "s3a://", "s3n://"} {
		if rest, found := strings.CutPrefix(u, scheme); found {
			bucket, key, _ = strings.Cut(rest, "/")
			return bucket, key, bucket != ""
		}
	}
	return "", "", false
}
