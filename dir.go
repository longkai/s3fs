package s3fs

import (
	"context"
	"io"
	"io/fs"
	"os"
	"path/filepath"
)

var _ NamespacedFS = (*dirFs)(nil)

type dirFs struct {
	dir string
}

// DirFS returns a dir based fs.
func DirFS(dir string) FS {
	return &dirFs{
		dir: dir,
	}
}

// Delete implements NamespacedFS.
func (d *dirFs) Delete(ctx context.Context, name string) error {
	fname := filepath.Join(d.dir, name)
	return os.Remove(fname)
}

// Namespace implements NamespacedFS.
func (d *dirFs) Namespace(dir string) FS {
	return &dirFs{
		dir: dir,
	}
}

// Open implements NamespacedFS.
func (d *dirFs) Open(name string) (fs.File, error) {
	return d.OpenWithContext(context.Background(), name)
}

// OpenWithContext implements NamespacedFS.
func (d *dirFs) OpenWithContext(ctx context.Context, name string) (fs.File, error) {
	return os.Open(filepath.Join(d.dir, name))
}

// Put implements NamespacedFS.
func (d *dirFs) Put(ctx context.Context, name string, reader io.Reader) error {
	fname := filepath.Join(d.dir, name)
	if err := os.MkdirAll(filepath.Dir(fname), 0755); err != nil {
		return err
	}
	f, err := os.Create(fname)
	if err != nil {
		return err
	}
	defer f.Close()
	_, err = io.Copy(f, reader)
	return err
}

// ReadFile implements NamespacedFS.
func (d *dirFs) ReadFile(name string) ([]byte, error) {
	return os.ReadFile(filepath.Join(d.dir, name))
}

// ReadFileWithContext implements NamespacedFS.
func (d *dirFs) ReadFileWithContext(ctx context.Context, name string) ([]byte, error) {
	return d.ReadFile(name)
}
