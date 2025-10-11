package s3fs_test

import (
	"bufio"
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"reflect"
	"strings"
	"testing"
	"time"

	awshttp "github.com/aws/aws-sdk-go-v2/aws/transport/http"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"
	"github.com/johannesboyne/gofakes3"
	"github.com/johannesboyne/gofakes3/backend/s3mem"
	"github.com/longkai/s3fs"
)

func Example() {
	// 0. Create an fs.
	fs, fn := newTestFs()
	defer fn()
	// fs := s3fs.DirFS(os.TempDir())

	name := "path/to/file"

	// 1. Create a file.
	fs.Put(context.TODO(), name, strings.NewReader("hello\nworld"))

	// 2. Open the file.
	file, _ := fs.Open(name)
	defer file.Close()
	b, _ := io.ReadAll(file)
	fmt.Printf("%s\n", b)

	// (optional) Seek to the file position.
	seeker := file.(io.Seeker)
	seeker.Seek(1024, io.SeekStart) // Read the file starts from byte 1024.

	// (optional) Read all file content, if the file size is large, don't do it.
	body, _ := fs.ReadFile(name)
	fmt.Fprintf(io.Discard, "file content: %s", body)

	// 3. Read file content line by line, with buffering.
	scanner := bufio.NewScanner(file)
	for lineNo := 1; scanner.Scan(); lineNo++ {
		fmt.Fprintf(io.Discard, "%3d: %s\n", lineNo, scanner.Text())
	}

	// 4. upload strings
	fs.Put(context.TODO(), name, strings.NewReader("hello, world"))

	// 5. upload file
	file, _ = os.Open(name)
	fs.Put(context.TODO(), name, file)

	// 6. presign url with 15min expiration
	if fs, ok := fs.(s3fs.PresignFS); ok {
		fs.PresignGet(context.TODO(), name, s3.WithPresignExpires(time.Minute*15))
	}

	// 7. delete a file
	if err := fs.Delete(context.TODO(), name); err != nil {
		panic(err)
	}

	// Output: hello
	// world
}

func TestPut(t *testing.T) {
	fs, fn := newTestFs()
	defer fn()
	ctx, f := context.WithTimeout(context.TODO(), time.Second*5)
	defer f()
	err := fs.Put(ctx, "hello.txt", strings.NewReader("hello"))
	if err != nil {
		t.Fatal(err)
	}
	// Output: hello
}

func newTestFs() (s3fs.FS, func()) {
	backend := s3mem.New()
	faker := gofakes3.New(backend, gofakes3.WithAutoBucket(true))
	ts := httptest.NewServer(faker.Server())
	fn := func() {
		ts.Close()
	}
	fs, _ := s3fs.New(
		s3fs.WithCredential("AK******", "SK******"),
		s3fs.WithBucket("test-bucket"),
		s3fs.WithBufferSize(1),
		// s3fs.WithEndpoint(ts.URL),
		s3fs.WithOptFns(func(o *s3.Options) {
			o.BaseEndpoint = &ts.URL // override endpoint above
			o.HTTPClient = awshttp.NewBuildableClient().WithTransportOptions(func(t *http.Transport) {
				t.TLSClientConfig.InsecureSkipVerify = true
			})
		}),
	)
	return fs, fn
}

func TestBaiscS3Operations(t *testing.T) {
	fs, fn := newTestFs()
	defer fn()

	key := "hello.txt"
	content := "hello, world"

	_, err := fs.ReadFileWithContext(context.TODO(), key)
	var noSushKey *types.NoSuchKey
	if !errors.As(err, &noSushKey) {
		t.Fatalf("should not found")
	}

	if err := fs.Put(context.TODO(), key, strings.NewReader(content)); err != nil {
		t.Fatalf("Put: %+v", err)
	}
	b, err := fs.ReadFileWithContext(context.TODO(), key)
	if err != nil {
		t.Fatalf("Read after Put: %+v", err)
	}

	if got := string(b); got != content {
		t.Fatalf("read(%q) = %q, want %q", key, got, content)
	}

	u, err := fs.(s3fs.PresignFS).PresignGet(context.TODO(), key)
	if err != nil {
		t.Fatalf("Presign: %+v", err)
	}
	if _, err := url.Parse(u); err != nil {
		t.Fatalf("marlformed presign url %q: %+v", u, err)
	}

	if err := fs.Delete(context.TODO(), key); err != nil {
		t.Fatalf("Delete: %+v", err)
	}

	if _, err := fs.ReadFileWithContext(context.TODO(), key); err == nil {
		t.Fatalf("Read after Delete should fail")
	}
}

func TestScanner(t *testing.T) {
	content := `line1
line2
`
	want := []string{
		"line1",
		"line2",
	}
	fs, fn := newTestFs()
	defer fn()
	const key = "hello.txt"
	if err := fs.Put(context.TODO(), key, strings.NewReader(content)); err != nil {
		t.Fatal(err)
	}

	f, err := fs.Open(key)
	if err != nil {
		t.Fatal(err)
	}
	defer f.Close()

	sca := bufio.NewScanner(f)
	var arr []string
	for sca.Scan() {
		arr = append(arr, sca.Text())
	}
	if !reflect.DeepEqual(arr, want) {
		t.Fatalf("scan = %v, want %v", arr, want)
	}
}

func TestSeek0(t *testing.T) {
	r := strings.NewReader("hello")
	r.Seek(1, io.SeekStart)
	b, _ := io.ReadAll(r)
	if string(b) != "ello" {
		t.Fatalf("seek 1 got %s, want ello", b)
	}

	buf := bytes.NewBuffer([]byte("hello"))
	bb := make([]byte, 1)
	buf.Read(bb)

	if buf.Len() != 4 {
		t.Fatalf("read 1 byte, buf length = %d, want 4", buf.Len())
	}

	b, _ = io.ReadAll(buf)
	if string(b) != "ello" {
		t.Fatalf("read all byte = %s, want ello", b)
	}
}

func TestCopy(t *testing.T) {
	src := []int{1, 2, 3}
	dst := make([]int, 4)
	copy(dst, src)
	if !reflect.DeepEqual(dst, []int{1, 2, 3, 0}) {
		t.Fatalf("unexpect equal, got %v, want [1, 2, 3, 0]", dst)
	}
}

func TestSeek(t *testing.T) {
	fs, fn := newTestFs()
	defer fn()

	content := "hello, world"
	key := "hello.txt"
	if err := fs.Put(context.TODO(), key, strings.NewReader(content)); err != nil {
		t.Fatal(err)
	}

	f, err := fs.Open(key)
	if err != nil {
		t.Fatal(err)
	}
	defer f.Close()

	seeker, ok := f.(io.Seeker)
	if !ok {
		t.Fatal("read should be an io.Seeker")
	}
	offset, err := seeker.Seek(7, io.SeekStart)
	if err != nil {
		t.Fatal(err)
	}
	if offset != 7 {
		t.Fatalf("seek 7 bytes offset %d, want %d", offset, 7)
	}
	b, err := io.ReadAll(f)
	if err != nil {
		t.Fatal(err)
	}
	if got := string(b); got != "world" {
		t.Fatalf("read after seek, got %q, want %q", got, "world")
	}
}

func TestNativeClient(t *testing.T) {
	fs, fn := newTestFs()
	defer fn()
	type s3Client interface {
		Client() *s3.Client
	}
	if iface, ok := fs.(s3Client); ok {
		cli := iface.Client()
		_, err := cli.ListBuckets(context.TODO(), &s3.ListBucketsInput{})
		if err != nil {
			t.Fatal(err)
		}
	}
}

func TestDlLargeFile(t *testing.T) {
	fs, fn := newTestFs()
	defer fn()

	var buf bytes.Buffer

	// write file first
	for i := range 100000 {
		fmt.Fprintf(&buf, "this is line %d\n", i)
	}
	size := buf.Len()
	name := "path/to/large-file"
	if err := fs.Put(context.TODO(), name, &buf); err != nil {
		t.Fatal(err)
	}

	f, err := fs.OpenWithContext(context.TODO(), name)
	if err != nil {
		t.Fatal(err)
	}

	st, err := f.Stat()
	if err != nil {
		t.Fatal(err)
	}
	if st.Size() != int64(size) {
		t.Fatalf("got file size = %d, want %d", st.Size(), size)
	}
}

func TestPresignPut(t *testing.T) {
	fs, fn := newTestFs()
	defer fn()
	url, err := fs.(s3fs.PresignFS).PresignGet(context.TODO(), "aaa")
	if err != nil {
		t.Fatal(err)
	}
	t.Log(url)
}
