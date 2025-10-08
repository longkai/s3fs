package s3fs

import (
	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
)

// Option is a function that sets a configuration option.
type Option func(fs *awsS3)

// WithCredential sets the access key, secret key.
func WithCredential(ak, sk string) Option {
	return func(fs *awsS3) {
		fs.ak = ak
		fs.sk = sk
	}
}

// WithRegion sets the region.
func WithRegion(region string) Option {
	return func(fs *awsS3) {
		fs.region = region
	}
}

// WithEndpoint sets the endpoint.
//
// e.g.
//   - https://cos.ap-guangzhou.myqcloud.com
//   - https://s3.us-west-2.amazonaws.com
//   - http://1.2.3.4:1234
//   - https://<blob-account>.blob.core.chinacloudapi.cn?<sas-token>
//
// Note: Don't include the bucket name in the given URL.
func WithEndpoint(endpoint string) Option {
	return func(fs *awsS3) {
		fs.endpoint = endpoint
	}
}

// WithBucket sets the bucket.
func WithBucket(bucket string) Option {
	return func(fs *awsS3) {
		fs.bucket = aws.String(bucket)
	}
}

// WithBufferSize sets the chunk size when doing multipart downloading, defaults to zero buffering, i.e., full download before process.
func WithBufferSize(bufferSize int64) Option {
	return func(fs *awsS3) {
		fs.bufLen = bufferSize
	}
}

// WithOptFns customizes everything if you familiar with aws s3.
func WithOptFns(optFns ...func(*s3.Options)) Option {
	return func(fs *awsS3) {
		fs.optFns = optFns
	}
}
