package s3store_test

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"sort"
	"strconv"
	"strings"
	"sync"
	"testing"

	"github.com/dimeken95/test_task/internal/config"
	s3store "github.com/dimeken95/test_task/internal/storage/s3"
)

// fakeS3 implements just enough of the S3 REST API to exercise the real upload
// paths in Store. Running in-process keeps the test fast and hermetic while
// still covering the multipart state machine end to end.
type fakeS3 struct {
	mu      sync.Mutex
	objects map[string][]byte
	parts   map[string]map[int32][]byte // uploadID -> partNumber -> bytes
	aborted map[string]bool
	nextID  int

	failPart int32 // when > 0, UploadPart for this number returns 500
}

func newFakeS3() *fakeS3 {
	return &fakeS3{
		objects: map[string][]byte{},
		parts:   map[string]map[int32][]byte{},
		aborted: map[string]bool{},
	}
}

func (f *fakeS3) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	// Path style: /<bucket>/<key...>
	trimmed := strings.TrimPrefix(r.URL.Path, "/")
	bucket, key, _ := strings.Cut(trimmed, "/")
	_ = bucket
	q := r.URL.Query()

	switch {
	case r.Method == http.MethodHead:
		w.WriteHeader(http.StatusOK)

	case r.Method == http.MethodPost && q.Has("uploads"):
		f.createMultipart(w, key)

	case r.Method == http.MethodPut && q.Get("uploadId") != "":
		f.uploadPart(w, r, q)

	case r.Method == http.MethodPost && q.Get("uploadId") != "":
		f.completeMultipart(w, r, key, q.Get("uploadId"))

	case r.Method == http.MethodDelete && q.Get("uploadId") != "":
		f.mu.Lock()
		f.aborted[q.Get("uploadId")] = true
		f.mu.Unlock()
		w.WriteHeader(http.StatusNoContent)

	case r.Method == http.MethodPut:
		body, _ := io.ReadAll(r.Body)
		f.mu.Lock()
		f.objects[key] = body
		f.mu.Unlock()
		w.Header().Set("ETag", `"single"`)
		w.WriteHeader(http.StatusOK)

	case r.Method == http.MethodDelete:
		f.mu.Lock()
		delete(f.objects, key)
		f.mu.Unlock()
		w.WriteHeader(http.StatusNoContent)

	default:
		w.WriteHeader(http.StatusNotImplemented)
	}
}

func (f *fakeS3) createMultipart(w http.ResponseWriter, key string) {
	f.mu.Lock()
	f.nextID++
	uploadID := fmt.Sprintf("upload-%d", f.nextID)
	f.parts[uploadID] = map[int32][]byte{}
	f.mu.Unlock()

	w.Header().Set("Content-Type", "application/xml")
	fmt.Fprintf(w, `<?xml version="1.0" encoding="UTF-8"?>
<InitiateMultipartUploadResult><Bucket>b</Bucket><Key>%s</Key><UploadId>%s</UploadId></InitiateMultipartUploadResult>`,
		key, uploadID)
}

func (f *fakeS3) uploadPart(w http.ResponseWriter, r *http.Request, q map[string][]string) {
	num, _ := strconv.Atoi(r.URL.Query().Get("partNumber"))
	uploadID := r.URL.Query().Get("uploadId")
	_ = q

	if f.failPart > 0 && int32(num) == f.failPart {
		io.Copy(io.Discard, r.Body)
		w.WriteHeader(http.StatusInternalServerError)
		return
	}

	body, _ := io.ReadAll(r.Body)
	f.mu.Lock()
	if f.parts[uploadID] == nil {
		f.parts[uploadID] = map[int32][]byte{}
	}
	f.parts[uploadID][int32(num)] = body
	f.mu.Unlock()

	w.Header().Set("ETag", fmt.Sprintf("%q", "etag-"+strconv.Itoa(num)))
	w.WriteHeader(http.StatusOK)
}

func (f *fakeS3) completeMultipart(w http.ResponseWriter, r *http.Request, key, uploadID string) {
	body, _ := io.ReadAll(r.Body)

	// The manifest must list parts in ascending order; assert it here because
	// S3 itself rejects an unordered manifest and MinIO is lenient about it.
	if nums := partNumbersInManifest(string(body)); !sort.IntsAreSorted(nums) {
		w.WriteHeader(http.StatusBadRequest)
		fmt.Fprint(w, `<Error><Code>InvalidPartOrder</Code></Error>`)
		return
	}

	f.mu.Lock()
	stored := f.parts[uploadID]
	nums := make([]int, 0, len(stored))
	for n := range stored {
		nums = append(nums, int(n))
	}
	sort.Ints(nums)
	var assembled []byte
	for _, n := range nums {
		assembled = append(assembled, stored[int32(n)]...)
	}
	f.objects[key] = assembled
	f.mu.Unlock()

	w.Header().Set("Content-Type", "application/xml")
	fmt.Fprintf(w, `<?xml version="1.0" encoding="UTF-8"?>
<CompleteMultipartUploadResult><Bucket>b</Bucket><Key>%s</Key><ETag>"final"</ETag></CompleteMultipartUploadResult>`, key)
}

func partNumbersInManifest(xml string) []int {
	var out []int
	for _, chunk := range strings.Split(xml, "<PartNumber>")[1:] {
		numStr, _, _ := strings.Cut(chunk, "</PartNumber>")
		if n, err := strconv.Atoi(strings.TrimSpace(numStr)); err == nil {
			out = append(out, n)
		}
	}
	return out
}

func (f *fakeS3) object(key string) ([]byte, bool) {
	f.mu.Lock()
	defer f.mu.Unlock()
	b, ok := f.objects[key]
	return b, ok
}

func (f *fakeS3) partCount(uploadID string) int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return len(f.parts[uploadID])
}

func newStore(t *testing.T, partSize int64) (*s3store.Store, *fakeS3) {
	t.Helper()
	fake := newFakeS3()
	srv := httptest.NewServer(fake)
	t.Cleanup(srv.Close)

	cfg := config.Config{
		S3Endpoint:           srv.URL,
		S3PresignEndpoint:    srv.URL,
		S3Region:             "us-east-1",
		S3Bucket:             "payloads",
		S3AccessKey:          "test",
		S3SecretKey:          "test",
		S3UsePathStyle:       true,
		S3PartSize:           partSize,
		S3MultipartThreshold: partSize * 2,
		S3PartConcurrency:    4,
	}
	store, err := s3store.New(cfg)
	if err != nil {
		t.Fatalf("new store: %v", err)
	}
	return store, fake
}

func payload(n int) []byte {
	b := make([]byte, n)
	for i := range b {
		b[i] = byte('a' + i%26)
	}
	return b
}

// A payload smaller than one part must take the single PutObject path and
// still report its true size, since multipart form parts have no length.
func TestPutSmallUnknownSize(t *testing.T) {
	store, fake := newStore(t, 5<<20)
	data := payload(1024)

	written, err := store.Put(context.Background(), "jobs/1/a.png", bytes.NewReader(data), 0, "image/png")
	if err != nil {
		t.Fatalf("put: %v", err)
	}
	if written != int64(len(data)) {
		t.Fatalf("written=%d want %d", written, len(data))
	}

	got, ok := fake.object("jobs/1/a.png")
	if !ok {
		t.Fatal("object not stored")
	}
	if !bytes.Equal(got, data) {
		t.Fatalf("stored %d bytes, want %d", len(got), len(data))
	}
}

// The important case: an unknown-size body larger than one part must switch to
// multipart, reassemble byte-for-byte, and report the exact size.
func TestPutLargeUnknownSizeUsesMultipart(t *testing.T) {
	const partSize = 5 << 20
	store, fake := newStore(t, partSize)
	data := payload(partSize*3 + 12345) // 3 full parts + remainder

	written, err := store.Put(context.Background(), "jobs/2/clip.mp4", bytes.NewReader(data), 0, "video/mp4")
	if err != nil {
		t.Fatalf("put: %v", err)
	}
	if written != int64(len(data)) {
		t.Fatalf("written=%d want %d", written, len(data))
	}
	if n := fake.partCount("upload-1"); n != 4 {
		t.Fatalf("parts=%d want 4", n)
	}

	got, ok := fake.object("jobs/2/clip.mp4")
	if !ok {
		t.Fatal("object not stored")
	}
	if !bytes.Equal(got, data) {
		t.Fatalf("reassembled object differs: got %d bytes want %d", len(got), len(data))
	}
}

// A payload that is an exact multiple of the part size must not emit a
// trailing zero-length part.
func TestPutExactPartMultiple(t *testing.T) {
	const partSize = 5 << 20
	store, fake := newStore(t, partSize)
	data := payload(partSize * 2)

	written, err := store.Put(context.Background(), "jobs/3/doc.pdf", bytes.NewReader(data), 0, "application/pdf")
	if err != nil {
		t.Fatalf("put: %v", err)
	}
	if written != int64(len(data)) {
		t.Fatalf("written=%d want %d", written, len(data))
	}
	if n := fake.partCount("upload-1"); n != 2 {
		t.Fatalf("parts=%d want 2", n)
	}
	if got, _ := fake.object("jobs/3/doc.pdf"); !bytes.Equal(got, data) {
		t.Fatal("content mismatch")
	}
}

// A failing part must abort the upload rather than leave it dangling and
// billing storage until lifecycle rules clean it up.
func TestPutAbortsMultipartOnPartFailure(t *testing.T) {
	const partSize = 5 << 20
	store, fake := newStore(t, partSize)
	fake.failPart = 2

	_, err := store.Put(context.Background(), "jobs/4/clip.mp4", bytes.NewReader(payload(partSize*3)), 0, "video/mp4")
	if err == nil {
		t.Fatal("expected error")
	}

	fake.mu.Lock()
	aborted := fake.aborted["upload-1"]
	fake.mu.Unlock()
	if !aborted {
		t.Fatal("multipart upload was not aborted")
	}
	if _, ok := fake.object("jobs/4/clip.mp4"); ok {
		t.Fatal("object should not exist after failed upload")
	}
}

// A reader that fails mid-stream (the size cap tripping, a client hanging up)
// must also abort.
func TestPutAbortsOnReaderFailure(t *testing.T) {
	const partSize = 5 << 20
	store, fake := newStore(t, partSize)

	body := io.MultiReader(
		bytes.NewReader(payload(partSize+10)),
		failingReader{},
	)
	_, err := store.Put(context.Background(), "jobs/5/x.mp4", body, 0, "video/mp4")
	if err == nil {
		t.Fatal("expected error")
	}

	fake.mu.Lock()
	aborted := fake.aborted["upload-1"]
	fake.mu.Unlock()
	if !aborted {
		t.Fatal("multipart upload was not aborted after reader failure")
	}
}

type failingReader struct{}

func (failingReader) Read([]byte) (int, error) { return 0, io.ErrUnexpectedEOF }

func TestPresignGetProducesSignedURL(t *testing.T) {
	store, _ := newStore(t, 5<<20)

	url, err := store.PresignGet(context.Background(), "jobs/6/a.png", 15*60)
	if err != nil {
		t.Fatalf("presign: %v", err)
	}
	if !strings.Contains(url, "X-Amz-Signature") || !strings.Contains(url, "jobs/6/a.png") {
		t.Fatalf("unexpected presigned url: %s", url)
	}
}
