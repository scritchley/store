package store

import (
	// "github.com/crowdmob/goamz/aws"
	// "github.com/crowdmob/goamz/s3"
	// "io"
	"io"
	"strconv"
	"time"

	s3 "github.com/rlmcpherson/s3gof3r"
)

var (
	Concurrency = 32
)

const (
	_        = iota
	kb int64 = 1 << (10 * iota)
	mb
	gb
	tb
	pb
	eb
)

type S3Store struct {
	bucket *s3.Bucket
}

func NewS3Store(accesskey, secret, bucket string) Store {
	client := s3.New("s3-eu-west-1.amazonaws.com", s3.Keys{
		AccessKey: accesskey,
		SecretKey: secret,
	})
	return &S3Store{
		bucket: client.Bucket(bucket),
	}
}

func (s S3Store) Get(path string) (io.ReadCloser, int, error) {
	r, h, err := s.bucket.GetReader(path, &s3.Config{
		Concurrency: Concurrency,
		PartSize:    10 * mb,
		NTry:        10,
		Md5Check:    false,
		Scheme:      "http",
		Client:      s3.ClientWithTimeout(5 * time.Second),
	})
	if err != nil {
		return nil, 0, err
	}
	cl := h.Get("Content-length")
	cls, err := strconv.Atoi(cl)
	if err != nil {
		return nil, 0, err
	}
	return r, cls, nil
}

func (s S3Store) Put(path string, r io.Reader) error {
	// Open a PutWriter for upload
	w, err := s.bucket.PutWriter(path, nil, nil)
	if err != nil {
		return err
	}
	if _, err = io.Copy(w, r); err != nil {
		return err
	}
	if err = w.Close(); err != nil {
		return err
	}
	return nil
}
