package source

import (
	"context"
	"errors"
	"io"
	"io/fs"
	"os"
	"path/filepath"
	"sort"
	"strings"
)

// LocalStore reads a folder laid out like the S3 log root (for -from, and
// for tests). Keys use forward slashes relative to the root.
type LocalStore struct{ root string }

// NewLocalStore reads the folder at root.
func NewLocalStore(root string) *LocalStore { return &LocalStore{root: root} }

func (l *LocalStore) Location(key string) string {
	return filepath.Join(l.root, filepath.FromSlash(key))
}

func (l *LocalStore) List(ctx context.Context, prefix string) ([]Object, error) {
	var out []Object
	err := filepath.WalkDir(l.root, func(p string, d fs.DirEntry, err error) error {
		if err != nil {
			if errors.Is(err, fs.ErrNotExist) {
				return nil
			}
			return err
		}
		if ctx.Err() != nil {
			return ctx.Err()
		}
		if d.IsDir() {
			return nil
		}
		rel, _ := filepath.Rel(l.root, p)
		key := filepath.ToSlash(rel)
		if !strings.HasPrefix(key, prefix) {
			return nil
		}
		info, err := d.Info()
		if err != nil {
			return nil
		}
		out = append(out, Object{Key: key, Size: info.Size(), Modified: info.ModTime()})
		return nil
	})
	if err != nil {
		class := ClassOther
		if errors.Is(err, fs.ErrPermission) {
			class = ClassAccessDenied
		}
		return out, &Error{Class: class, Key: l.Location(prefix), Err: err}
	}
	sort.Slice(out, func(i, j int) bool { return out[i].Key < out[j].Key })
	return out, nil
}

func (l *LocalStore) Open(ctx context.Context, obj Object) (io.ReadCloser, error) {
	f, err := os.Open(l.Location(obj.Key))
	if err != nil {
		class := ClassOther
		switch {
		case errors.Is(err, fs.ErrNotExist):
			class = ClassNotFound
		case errors.Is(err, fs.ErrPermission):
			class = ClassAccessDenied
		}
		return nil, &Error{Class: class, Key: l.Location(obj.Key), Err: err}
	}
	return f, nil
}
