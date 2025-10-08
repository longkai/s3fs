// Package s3fs provides the Go 1.16 fs api for aws s3 compatible object storage services.
// which supporting buffering, seeking aws s3 compatible object storages service just like read a local file.
package s3fs

import (
	"context"
	"io"
	"io/fs"

	"github.com/aws/aws-sdk-go-v2/service/s3"
)

// FS
type FS interface {
	fs.ReadFileFS
	ContextualReadFileFS

	WriteFileFS

	PresignFS
}

// NamespacedFS clones the current FS with the new namespace, e.g., bucket from s3 or container from azure blob.
type NamespacedFS interface {
	// Namespace lets you change the namespace(bucket or container), like `cd` in shell.
	Namespace(string) FS
	FS
}

// ContextualReadFileFS like fs.ReadFileFS, but with an additional ctx param.
type ContextualReadFileFS interface {
	// OpenWithContext opens the file with the context.
	OpenWithContext(ctx context.Context, name string) (fs.File, error)

	// ReadFileWithContext reads the file with the context.
	ReadFileWithContext(ctx context.Context, name string) ([]byte, error)
}

// WriteFileFS lets you write, delete aws s3
type WriteFileFS interface {
	// Put creates a new file whose content reads from the reader
	// Note: we may provides var arg s3 options if necessary
	Put(ctx context.Context, name string, reader io.Reader) error

	// Delete removes the file with the given name
	Delete(ctx context.Context, name string) error
}

// PresignFS creates url links to access the fs.
type PresignFS interface {
	// PresignGet generates a presigned HTTP url to get the object.
	PresignGet(ctx context.Context, name string, optFns ...func(*s3.PresignOptions)) (string, error)

	// PresignPut generates a presigned HTTP url to put the object.
	PresignPut(ctx context.Context, name string, optFns ...func(*s3.PresignOptions)) (string, error)
}
