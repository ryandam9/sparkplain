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
	// Open reads obj, failing with ClassChanged if it changed since it was
	// listed.
	Open(ctx context.Context, obj Object) (io.ReadCloser, error)
	// Location names a key for people: s3://bucket/key or a local path.
	Location(key string) string
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
	return ClassOther
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
