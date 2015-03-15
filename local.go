package store

import (
	"io"
	"os"
)

type LocalStore struct {
}

func NewLocalStore() Store {
	return &LocalStore{}
}

type LocalObject *os.File

func (l *LocalStore) Get(path string) (io.ReadCloser, int, error) {
	f, err := os.Open(path)
	if err != nil {
		return f, 0, err
	}
	stats, err := f.Stat()
	if err != nil {
		return f, 0, err
	}
	return f, int(stats.Size()), nil
}

func (l *LocalStore) Put(path string, r io.Reader) error {
	f, err := os.Create(path)
	if err != nil {
		return err
	}
	_, err = io.Copy(f, r)
	return err
}
