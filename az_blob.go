package s3fs

import (
	"context"
	"io"
	"io/fs"

	"github.com/Azure/azure-sdk-for-go/sdk/storage/azblob"
	"github.com/aws/aws-sdk-go-v2/service/s3"
)

var _ NamespacedFS = (*azBlobFs)(nil)

type azBlobFs struct {
	container string
	client    *azblob.Client

	bufLen int64 // optional
}

// Namespace implements NamespacedFS.
func (a *azBlobFs) Namespace(container string) FS {
	if container == "" {
		panic("blobfs: with empty container")
	}
	tmp := *a
	tmp.container = container
	return &tmp
}

// Delete implements FS.
func (a *azBlobFs) Delete(ctx context.Context, name string) error {
	_, err := a.client.DeleteBlob(ctx, a.container, name, nil)
	return err
}

// Open implements FS.
func (a *azBlobFs) Open(name string) (fs.File, error) {
	return a.OpenWithContext(context.Background(), name)
}

// OpenWithContext implements FS.
func (a *azBlobFs) OpenWithContext(ctx context.Context, name string) (fs.File, error) {
	obj := &object{
		ctx:    ctx,
		client: newBlobClient(a.client, a.container),
		bufLen: a.bufLen,
		name:   name,
	}
	return obj, obj.fillChunk(false)
}

// PresignGet implements FS.
func (a *azBlobFs) PresignGet(ctx context.Context, name string, optFns ...func(*s3.PresignOptions)) (string, error) {
	panic("unimplemented")
}

// PresignPut implements FS.
func (a *azBlobFs) PresignPut(ctx context.Context, name string, optFns ...func(*s3.PresignOptions)) (string, error) {
	panic("unimplemented")
}

// Put implements FS.
func (a *azBlobFs) Put(ctx context.Context, name string, reader io.Reader) error {
	_, err := a.client.UploadStream(ctx, a.container, name, reader, nil)
	return err
}

// ReadFile implements FS.
func (a *azBlobFs) ReadFile(name string) ([]byte, error) {
	return a.ReadFileWithContext(context.Background(), name)
}

// ReadFileWithContext implements FS.
func (a *azBlobFs) ReadFileWithContext(ctx context.Context, name string) ([]byte, error) {
	obj := &object{
		ctx:    ctx,
		client: newBlobClient(a.client, a.container),
		bufLen: a.bufLen,
		name:   name,
	}
	if err := obj.dl(); err != nil {
		return nil, err
	}
	return obj.buf.Bytes(), nil
}
