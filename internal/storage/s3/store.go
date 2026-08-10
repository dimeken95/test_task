package s3store

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"net/url"
	"slices"
	"sync"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	v4 "github.com/aws/aws-sdk-go-v2/aws/signer/v4"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"
	"golang.org/x/sync/errgroup"

	"github.com/dimeken95/test_task/internal/config"
	"github.com/dimeken95/test_task/internal/observability"
)

type Store struct {
	client             *s3.Client
	presignClient      *s3.PresignClient
	bucket             string
	multipartThreshold int64
	partSize           int64
	partConcurrency    int
	bufPool            *sync.Pool
}

func New(cfg config.Config) (*Store, error) {
	if _, err := url.Parse(cfg.S3Endpoint); err != nil {
		return nil, fmt.Errorf("parse s3 endpoint: %w", err)
	}

	awsCfg := aws.Config{
		Region: cfg.S3Region,
		Credentials: credentials.NewStaticCredentialsProvider(
			cfg.S3AccessKey,
			cfg.S3SecretKey,
			"",
		),
		// Default checksums wrap the body in aws-chunked framing, which several
		// S3-compatible stores handle poorly. We stream large media, so the
		// per-part ETag is the integrity check we rely on.
		RequestChecksumCalculation: aws.RequestChecksumCalculationWhenRequired,
	}

	client := s3.NewFromConfig(awsCfg, func(o *s3.Options) {
		o.BaseEndpoint = aws.String(cfg.S3Endpoint)
		o.UsePathStyle = cfg.S3UsePathStyle
	})

	presignClient := s3.NewPresignClient(s3.NewFromConfig(awsCfg, func(o *s3.Options) {
		o.BaseEndpoint = aws.String(cfg.S3PresignEndpoint)
		o.UsePathStyle = cfg.S3UsePathStyle
	}))

	partSize := cfg.S3PartSize
	return &Store{
		client:             client,
		presignClient:      presignClient,
		bucket:             cfg.S3Bucket,
		multipartThreshold: cfg.S3MultipartThreshold,
		partSize:           partSize,
		partConcurrency:    cfg.S3PartConcurrency,
		bufPool: &sync.Pool{
			New: func() any {
				b := make([]byte, partSize)
				return &b
			},
		},
	}, nil
}

func (s *Store) EnsureBucket(ctx context.Context) error {
	_, err := s.client.HeadBucket(ctx, &s3.HeadBucketInput{Bucket: aws.String(s.bucket)})
	if err == nil {
		return nil
	}
	_, err = s.client.CreateBucket(ctx, &s3.CreateBucketInput{Bucket: aws.String(s.bucket)})
	if err != nil {
		return fmt.Errorf("create bucket: %w", err)
	}
	return nil
}

// Put streams body into the bucket and reports the number of bytes written.
// multipart/form-data gives no reliable per-part length, so the unknown-size
// path is the normal one: read one part first, then decide.
func (s *Store) Put(ctx context.Context, key string, body io.Reader, size int64, contentType string) (int64, error) {
	start := time.Now()

	var (
		written int64
		err     error
		mode    string
	)
	switch {
	case size > 0 && size < s.multipartThreshold:
		mode = "put"
		written, err = s.putObject(ctx, key, body, size, contentType)
	case size > 0:
		mode = "multipart"
		written, err = s.putMultipartFromFirst(ctx, key, nil, 0, body, contentType)
	default:
		mode = "auto"
		written, err = s.putAuto(ctx, key, body, contentType)
	}

	observability.ObserveS3Upload(mode, time.Since(start), err == nil)
	if err == nil {
		observability.S3UploadBytes.Add(float64(written))
	}
	return written, err
}

func (s *Store) putObject(ctx context.Context, key string, body io.Reader, size int64, contentType string) (int64, error) {
	input := &s3.PutObjectInput{
		Bucket:      aws.String(s.bucket),
		Key:         aws.String(key),
		Body:        body,
		ContentType: aws.String(contentType),
	}
	if size > 0 {
		input.ContentLength = aws.Int64(size)
	}
	_, err := s.client.PutObject(ctx, input, s3.WithAPIOptions(
		v4.SwapComputePayloadSHA256ForUnsignedPayloadMiddleware,
	))
	if err != nil {
		return 0, fmt.Errorf("put object: %w", err)
	}
	return size, nil
}

// putAuto peeks one part. Small payloads become a single PutObject; anything
// that fills the buffer continues as a multipart upload reusing that buffer.
func (s *Store) putAuto(ctx context.Context, key string, body io.Reader, contentType string) (int64, error) {
	bufPtr := s.getBuf()
	buf := (*bufPtr)[:s.partSize]

	n, err := fill(body, buf)
	if errors.Is(err, io.EOF) {
		defer s.bufPool.Put(bufPtr)
		return s.putObject(ctx, key, bytes.NewReader(buf[:n]), int64(n), contentType)
	}
	if err != nil {
		s.bufPool.Put(bufPtr)
		return 0, fmt.Errorf("read first part: %w", err)
	}
	return s.putMultipartFromFirst(ctx, key, bufPtr, n, body, contentType)
}

type partJob struct {
	num int32
	buf *[]byte
	n   int
}

func (s *Store) putMultipartFromFirst(
	ctx context.Context,
	key string,
	firstBuf *[]byte,
	firstN int,
	body io.Reader,
	contentType string,
) (int64, error) {
	created, err := s.client.CreateMultipartUpload(ctx, &s3.CreateMultipartUploadInput{
		Bucket:      aws.String(s.bucket),
		Key:         aws.String(key),
		ContentType: aws.String(contentType),
	})
	if err != nil {
		if firstBuf != nil {
			s.bufPool.Put(firstBuf)
		}
		return 0, fmt.Errorf("create multipart: %w", err)
	}
	uploadID := aws.ToString(created.UploadId)

	abort := func() {
		// Detached context: the request context is usually already dead by the
		// time we abort, and a dangling upload costs storage until lifecycle GC.
		abortCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), 10*time.Second)
		defer cancel()
		_, _ = s.client.AbortMultipartUpload(abortCtx, &s3.AbortMultipartUploadInput{
			Bucket:   aws.String(s.bucket),
			Key:      aws.String(key),
			UploadId: aws.String(uploadID),
		})
	}

	g, gctx := errgroup.WithContext(ctx)
	jobs := make(chan partJob, s.partConcurrency)
	var (
		mu    sync.Mutex
		parts []types.CompletedPart
		total int64
	)

	for range s.partConcurrency {
		g.Go(func() error {
			for job := range jobs {
				etag, upErr := s.uploadPart(gctx, key, uploadID, job.num, (*job.buf)[:job.n])
				s.bufPool.Put(job.buf)
				if upErr != nil {
					return upErr
				}
				mu.Lock()
				parts = append(parts, types.CompletedPart{
					ETag:       aws.String(etag),
					PartNumber: aws.Int32(job.num),
				})
				total += int64(job.n)
				mu.Unlock()
				observability.S3MultipartParts.Inc()
			}
			return nil
		})
	}

	produceErr := s.produceParts(gctx, jobs, firstBuf, firstN, body)
	close(jobs)
	workersErr := g.Wait()
	// Drain buffers the producer queued but nobody consumed after a failure.
	for job := range jobs {
		s.bufPool.Put(job.buf)
	}

	if produceErr != nil || workersErr != nil {
		abort()
		if produceErr != nil {
			return 0, produceErr
		}
		return 0, workersErr
	}

	// S3 requires the completion manifest sorted by part number; parts finish
	// out of order because they upload concurrently.
	slices.SortFunc(parts, func(a, b types.CompletedPart) int {
		return int(aws.ToInt32(a.PartNumber) - aws.ToInt32(b.PartNumber))
	})

	_, err = s.client.CompleteMultipartUpload(ctx, &s3.CompleteMultipartUploadInput{
		Bucket:          aws.String(s.bucket),
		Key:             aws.String(key),
		UploadId:        aws.String(uploadID),
		MultipartUpload: &types.CompletedMultipartUpload{Parts: parts},
	})
	if err != nil {
		abort()
		return 0, fmt.Errorf("complete multipart: %w", err)
	}
	return total, nil
}

// produceParts slices body into pooled buffers and feeds the upload workers.
// It runs on the caller goroutine so backpressure from a full channel throttles
// the reader instead of buffering the whole payload.
func (s *Store) produceParts(ctx context.Context, jobs chan<- partJob, firstBuf *[]byte, firstN int, body io.Reader) error {
	partNum := int32(1)

	if firstBuf != nil {
		select {
		case <-ctx.Done():
			s.bufPool.Put(firstBuf)
			return ctx.Err()
		case jobs <- partJob{num: partNum, buf: firstBuf, n: firstN}:
			partNum++
		}
	}

	for {
		bufPtr := s.getBuf()
		buf := (*bufPtr)[:s.partSize]
		n, rerr := fill(body, buf)

		if n > 0 {
			select {
			case <-ctx.Done():
				s.bufPool.Put(bufPtr)
				return ctx.Err()
			case jobs <- partJob{num: partNum, buf: bufPtr, n: n}:
				partNum++
			}
		} else {
			s.bufPool.Put(bufPtr)
		}

		switch {
		case errors.Is(rerr, io.EOF):
			return nil
		case rerr != nil:
			return fmt.Errorf("read part: %w", rerr)
		}
	}
}

// getBuf hands out a part-sized buffer, keeping the pool's type assertion in
// one checked place.
func (s *Store) getBuf() *[]byte {
	if buf, ok := s.bufPool.Get().(*[]byte); ok && buf != nil {
		return buf
	}
	b := make([]byte, s.partSize)
	return &b
}

// fill reads until buf is full and reports io.EOF only for a genuine
// end of stream. io.ReadFull collapses "short read at EOF" and "the reader
// failed" into ErrUnexpectedEOF, which would silently truncate an upload when
// a client hangs up mid-body.
func fill(r io.Reader, buf []byte) (int, error) {
	var n int
	for n < len(buf) {
		m, err := r.Read(buf[n:])
		n += m
		if err != nil {
			if errors.Is(err, io.EOF) {
				return n, io.EOF
			}
			return n, err
		}
	}
	return n, nil
}

func (s *Store) uploadPart(ctx context.Context, key, uploadID string, partNum int32, data []byte) (string, error) {
	out, err := s.client.UploadPart(ctx, &s3.UploadPartInput{
		Bucket:        aws.String(s.bucket),
		Key:           aws.String(key),
		UploadId:      aws.String(uploadID),
		PartNumber:    aws.Int32(partNum),
		Body:          bytes.NewReader(data),
		ContentLength: aws.Int64(int64(len(data))),
	}, s3.WithAPIOptions(
		v4.SwapComputePayloadSHA256ForUnsignedPayloadMiddleware,
	))
	if err != nil {
		return "", fmt.Errorf("upload part %d: %w", partNum, err)
	}
	return aws.ToString(out.ETag), nil
}

func (s *Store) Delete(ctx context.Context, key string) error {
	if key == "" {
		return nil
	}
	_, err := s.client.DeleteObject(ctx, &s3.DeleteObjectInput{
		Bucket: aws.String(s.bucket),
		Key:    aws.String(key),
	})
	if err != nil {
		return fmt.Errorf("delete object: %w", err)
	}
	return nil
}

func (s *Store) PresignGet(ctx context.Context, key string, expiry time.Duration) (string, error) {
	out, err := s.presignClient.PresignGetObject(ctx, &s3.GetObjectInput{
		Bucket: aws.String(s.bucket),
		Key:    aws.String(key),
	}, s3.WithPresignExpires(expiry))
	if err != nil {
		return "", fmt.Errorf("presign get: %w", err)
	}
	return out.URL, nil
}

func (s *Store) Ping(ctx context.Context) error {
	_, err := s.client.HeadBucket(ctx, &s3.HeadBucketInput{Bucket: aws.String(s.bucket)})
	if err != nil {
		return fmt.Errorf("s3 ping: %w", err)
	}
	return nil
}
