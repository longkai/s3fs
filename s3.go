package s3fs

import (
	"context"
	"fmt"
	"io"
	"io/fs"
	"strings"

	"github.com/Azure/azure-sdk-for-go/sdk/storage/azblob"
	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/feature/s3/manager"
	"github.com/aws/aws-sdk-go-v2/service/s3"
)

var (
	_ FS = (*awsS3)(nil)
)

// New creates a new s3 fs implement, one bucket per fs.
func New(options ...Option) (NamespacedFS, error) {
	fs := &awsS3{}
	// Set options.
	for _, op := range options {
		op(fs)
	}

	// fill default options
	if fs.region == "" {
		fs.region = "us-east-1" // see General endpoints in https://docs.aws.amazon.com/general/latest/gr/rande.html
	}

	if strings.Contains(fs.endpoint, "blob.core") {
		// it's auzre blob
		if fs.sk != "" {
			cred, err := azblob.NewSharedKeyCredential(fs.ak, fs.sk)
			if err != nil {
				return nil, err
			}
			cli, err := azblob.NewClientWithSharedKeyCredential(fs.endpoint, cred, nil)
			if err != nil {
				return nil, err
			}
			return &azBlobFs{
				client:    cli,
				container: *fs.bucket,
				bufLen:    fs.bufLen,
			}, nil
		}
		// sas token
		cli, err := azblob.NewClientWithNoCredential(fs.endpoint, nil)
		if err != nil {
			return nil, err
		}
		return &azBlobFs{
			client:    cli,
			container: *fs.bucket,
			bufLen:    fs.bufLen,
		}, nil
	}

	// init s3 client, optFns lets you customize everything!
	fs.client = s3.New(s3.Options{
		Region:       fs.region,
		BaseEndpoint: aws.String(fs.endpoint), // https://aws.github.io/aws-sdk-go-v2/docs/configuring-sdk/endpoints/
		Credentials:  credentials.NewStaticCredentialsProvider(fs.ak, fs.sk, ""),
	}, fs.optFns...)
	fs.presignClient = s3.NewPresignClient(fs.client)
	return fs, nil
}

type awsS3 struct {
	// optional
	bufLen int64
	bucket *string

	// facade, most common usage
	ak, sk   string
	region   string
	endpoint string

	// custom everything
	optFns []func(*s3.Options)

	client        *s3.Client
	presignClient *s3.PresignClient
}

// Client returns the underlying s3 client for advanced usages.
func (a *awsS3) Client() *s3.Client {
	return a.client
}

// PresignGet implements FS.
func (a *awsS3) PresignGet(ctx context.Context, name string, optFns ...func(*s3.PresignOptions)) (string, error) {
	rsp, err := a.presignClient.PresignGetObject(ctx, &s3.GetObjectInput{
		Bucket: a.bucket,
		Key:    aws.String(name),
	}, optFns...)
	if err != nil {
		return "", err
	}
	return rsp.URL, nil
}

// PresignPut implements FS.
func (a *awsS3) PresignPut(ctx context.Context, name string, optFns ...func(*s3.PresignOptions)) (string, error) {
	rsp, err := a.presignClient.PresignPutObject(ctx, &s3.PutObjectInput{
		Bucket: a.bucket,
		Key:    aws.String(name),
	}, optFns...)
	if err != nil {
		return "", err
	}
	return rsp.URL, nil
}

// String implements fmt.Stringer.
func (a *awsS3) String() string {
	return fmt.Sprintf(`{
  "bucket": %q,
  "client_ptr": "%p"
}`, *a.bucket, a.client)
}

// WithBucket implements BucketFS.
func (a *awsS3) WithBucket(bucket string) FS {
	if bucket == "" {
		panic("s3fs: with empty bucket")
	}
	tmp := *a
	tmp.bucket = &bucket
	return &tmp
}

// Namespace implements BucketableFS.
func (a *awsS3) Namespace(bucket string) FS {
	return a.WithBucket(bucket)
}

// Delete implements FS.
func (a *awsS3) Delete(ctx context.Context, name string) error {
	_, err := a.client.DeleteObject(ctx, &s3.DeleteObjectInput{
		Bucket: a.bucket,
		Key:    aws.String(name),
	})
	return err
}

// Open implements FS.
func (a *awsS3) Open(name string) (fs.File, error) {
	return a.OpenWithContext(context.Background(), name)
}

// OpenWithContext implements FS.
func (a *awsS3) OpenWithContext(ctx context.Context, name string) (fs.File, error) {
	obj := &object{
		ctx:    ctx,
		client: newS3Client(a.client, *a.bucket),
		bufLen: a.bufLen,
		name:   name,
	}
	return obj, obj.fillChunk(false) // first chunk contains metadata
}

// Put implements FS.
func (a *awsS3) Put(ctx context.Context, name string, reader io.Reader) error {
	uploader := manager.NewUploader(a.client)
	input := &s3.PutObjectInput{
		Bucket: a.bucket,
		Key:    aws.String(name),
		Body:   reader,
	}
	_, err := uploader.Upload(ctx, input)
	return err
}

// ReadFile implements FS.
func (a *awsS3) ReadFile(name string) ([]byte, error) {
	return a.ReadFileWithContext(context.Background(), name)
}

// ReadFileWithContext implements FS.
func (a *awsS3) ReadFileWithContext(ctx context.Context, name string) ([]byte, error) {
	obj := &object{
		ctx:    ctx,
		client: newS3Client(a.client, *a.bucket),
		bufLen: a.bufLen,
		name:   name,
	}
	if err := obj.dl(); err != nil {
		return nil, err
	}
	return obj.buf.Bytes(), nil
}
