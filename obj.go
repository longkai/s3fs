package s3fs

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"time"

	"github.com/Azure/azure-sdk-for-go/sdk/storage/azblob"
	"github.com/Azure/azure-sdk-for-go/sdk/storage/azblob/blob"
	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
)

var (
	_ fs.FileInfo = (*object)(nil)
	_ fs.File     = (*object)(nil)
	_ io.Seeker   = (*object)(nil)
)

// object represents a s3 object which implements fs.File.
type object struct {
	ctx context.Context

	client client

	buf    bytes.Buffer
	bufLen int64

	name     string
	dlOffset int64 // dl offset, downloaded bytes offset.
	size     int64
	modTime  time.Time

	rOffset int // read offset

	completelyLoaded bool
}

// Seek implements io.Seeker.
func (obj *object) Seek(offset int64, whence int) (int64, error) {
	var newOffset int64
	switch whence {
	case io.SeekStart:
		newOffset = offset
	case io.SeekCurrent:
		newOffset = int64(obj.rOffset) + offset
	case io.SeekEnd:
		newOffset = obj.size + offset // offset should be nagetive, or read will EOF
	default:
		return 0, fmt.Errorf("s3fs.object.Seek: invalid whence")
	}
	if newOffset < 0 {
		return 0, errors.New("s3fs.object.Seek: negative position")
	}

	obj.rOffset = int(newOffset)
	return newOffset, nil
}

// Close implements fs.File.
func (o *object) Close() error { return nil }

// Read implements fs.File.
func (obj *object) Read(b []byte) (int, error) {
	if obj.rOffset >= int(obj.size) {
		return 0, io.EOF
	}

	if !obj.completelyLoaded {
		if obj.rOffset > int(obj.dlOffset) {
			// read all
			if err := obj.fillChunk(true); err != nil {
				return 0, err
			}
		} else if (obj.buf.Len() - obj.rOffset) < len(b) {
			// fill next chunk
			if err := obj.fillChunk(false); err != nil {
				return 0, err
			}
		}
	}

	n := copy(b, obj.buf.Bytes()[obj.rOffset:])
	obj.rOffset += n
	return n, nil
}

// Stat implements fs.File.
func (o *object) Stat() (fs.FileInfo, error) { return o, nil }

// IsDir implements fs.FileInfo.
func (o *object) IsDir() bool { return false /* s3 object has no dir */ }

// ModTime implements fs.FileInfo.
func (o *object) ModTime() time.Time { return o.modTime }

// Mode implements fs.FileInfo.
func (o *object) Mode() fs.FileMode { return fs.ModePerm }

// Name implements fs.FileInfo.
func (o *object) Name() string { return o.name }

// Size implements fs.FileInfo.
func (o *object) Size() int64 { return o.size }

// Sys implements fs.FileInfo.
func (o *object) Sys() any { return nil }

type blobClient struct {
	container string
	blob      *azblob.Client
}

func (b *blobClient) getObject(ctx context.Context, key string, offset, count int64) (*getObjectResponse, error) {
	var _range blob.HTTPRange
	if offset > -1 {
		_range = blob.HTTPRange{
			Offset: offset,
			Count:  count,
		}
	}
	rsp, err := b.blob.DownloadStream(ctx, b.container, key, &blob.DownloadStreamOptions{
		Range: _range,
	})
	if err != nil {
		return nil, err
	}
	ret := &getObjectResponse{
		body:          rsp.Body,
		contentLength: *rsp.ContentLength,
		contentRange:  rsp.ContentRange,
		lastModified:  *rsp.LastModified,
	}
	return ret, nil
}

func newS3Client(s3 *s3.Client, bucket string) client {
	return &s3Client{
		bucket: bucket,
		s3:     s3,
	}
}

func newBlobClient(blob *azblob.Client, container string) client {
	return &blobClient{
		container: container,
		blob:      blob,
	}
}

type s3Client struct {
	bucket string
	s3     *s3.Client
}

func (s *s3Client) getObject(ctx context.Context, key string, offset, count int64) (*getObjectResponse, error) {
	var _range *string
	if offset > -1 {
		_range = aws.String(fmt.Sprintf("bytes=%d-%d", offset, count))
	}
	rsp, err := s.s3.GetObject(ctx, &s3.GetObjectInput{
		Bucket: aws.String(s.bucket),
		Key:    aws.String(key),
		Range:  _range,
	})
	if err != nil {
		return nil, err
	}
	ret := &getObjectResponse{
		body:          rsp.Body,
		contentLength: *rsp.ContentLength,
		contentRange:  rsp.ContentRange,
		lastModified:  *rsp.LastModified,
	}
	return ret, nil
}

type client interface {
	getObject(ctx context.Context, key string, offset, count int64) (*getObjectResponse, error)
}

type getObjectResponse struct {
	body          io.ReadCloser
	contentLength int64
	contentRange  *string
	lastModified  time.Time
}

// dl downloads all the bytes, this is a fallback of fillChunk.
func (obj *object) dl() error {
	rsp, err := obj.client.getObject(obj.ctx, obj.name, -1, 0)
	if err != nil {
		return err
	}
	defer func() { _ = rsp.body.Close() }()
	return obj.parseFullResponse(rsp)
}

func (obj *object) parseFullResponse(rsp *getObjectResponse) error {
	switch {
	default:
		fallthrough
	case obj.modTime.IsZero():
		obj.modTime = rsp.lastModified
	case !obj.modTime.Equal(rsp.lastModified):
		return fmt.Errorf("Last-Modified changed, before %s, now %s", obj.modTime, rsp.lastModified)
	}

	if _, err := io.Copy(&obj.buf, rsp.body); err != nil {
		return err
	}

	obj.size = rsp.contentLength
	obj.completelyLoaded = true
	return nil
}

// fillChunk downloads next chunk of bytes from s3 for obj.
func (obj *object) fillChunk(full bool) error {
	if obj.bufLen == 0 {
		return obj.dl()
	}
	end := obj.dlOffset + obj.bufLen - 1
	if full {
		end = obj.size - 1
	}
	rsp, err := obj.client.getObject(obj.ctx, obj.name, obj.dlOffset, end)
	if err != nil {
		// If it's the first try got HTTP 416, then fallback get.
		// It's rare. This only happens when the file is empty, i.e. zero bytes file.
		if obj.dlOffset == 0 {
			return obj.dl()
		}
		return err
	}
	defer func() { _ = rsp.body.Close() }() // body is never nil, the cos code is ugly.

	return obj.parsePartialResponse(rsp)
}

func (obj *object) parsePartialResponse(rsp *getObjectResponse) error {
	obj.modTime = rsp.lastModified
	if _, err := io.Copy(&obj.buf, rsp.body); err != nil {
		return err
	}

	_, end, size, ok := parseContentRange(rsp.contentRange)
	if !ok {
		return fmt.Errorf("parse content-range: %v", rsp.contentRange)
	}

	obj.size = size
	obj.dlOffset = end + 1               // http range is inclusive
	obj.completelyLoaded = end == size-1 // range offset starts as 0
	return nil
}

func parseContentRange(s *string) (start, end, total int64, ok bool) {
	if s == nil {
		ok = false
		return
	}
	n, err := fmt.Sscanf(*s, "bytes %d-%d/%d", &start, &end, &total)
	if err != nil {
		ok = false
		return
	}
	ok = n == 3
	return
}
