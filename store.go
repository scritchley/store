package store

import (
	"fmt"
	"io"
	"net/url"
)

// Store implements an interface for accessing Collections
type Store interface {
	// Get retrieves a single Object interface
	Get(string) (io.ReadCloser, int, error)
	// Put takes an io.Reader and stores an object to the given location returning an error
	Put(string, io.Reader) error
}

func parseURI(uri string) *url.URL {
	u, err := url.Parse(uri)
	if err != nil {
		return nil
	}
	return u
}

func getStore(u *url.URL) (Store, error) {
	switch u.Scheme {
	case "s3n":
		usr := u.User.Username()
		pwd, _ := u.User.Password()
		return NewS3Store(usr, pwd, u.Host), nil
	case "file":
		return NewLocalStore(), nil
	default:
		return nil, fmt.Errorf("unsupported scheme '%s'", u.Scheme)
	}
}

func Get(path string) (io.ReadCloser, int, error) {
	u := parseURI(path)
	s, err := getStore(u)
	if err != nil {
		return nil, 0, err
	}
	return s.Get(u.Path)
}

func Put(path string, r io.Reader) error {
	u := parseURI(path)
	s, err := getStore(u)
	if err != nil {
		return err
	}
	return s.Put(u.Path, r)
}
