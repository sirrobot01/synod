package repository

import (
	"bytes"
	"context"
	"errors"
	"io"
	"os"
	"path"
	"path/filepath"
	"syscall"

	"github.com/kopia/kopia/repo/blob"
	"github.com/kopia/kopia/repo/blob/sharded"
)

type localOptions struct {
	Path string `json:"path"`
}

// localStorage keeps the file layout of Kopia's filesystem storage. It confines each access to
// the repository directory and writes files like the device blob service.
type localStorage struct {
	sharded.Storage
	blob.DefaultProviderImplementation
	options localOptions
	root    *os.Root
}

func init() { blob.AddSupportedStorage("synod-local", localOptions{}, newLocal) }

func newLocal(ctx context.Context, options *localOptions, create bool) (blob.Storage, error) {
	if !filepath.IsAbs(options.Path) || filepath.Clean(options.Path) != options.Path {
		return nil, ErrLocation
	}
	if create {
		if err := os.MkdirAll(options.Path, 0700); err != nil {
			return nil, err
		}
	}
	root, err := os.OpenRoot(options.Path)
	if err != nil {
		return nil, err
	}
	s := &localStorage{options: *options, root: root}
	s.Storage = sharded.New(s, ".", sharded.Options{}, create)
	return s, nil
}

func (s *localStorage) ConnectionInfo() blob.ConnectionInfo {
	return blob.ConnectionInfo{Type: "synod-local", Config: &s.options}
}
func (s *localStorage) DisplayName() string         { return "Synod local repository" }
func (s *localStorage) Close(context.Context) error { return s.root.Close() }

func (s *localStorage) GetBlobFromPath(ctx context.Context, _, file string, offset, length int64, out blob.OutputBuffer) error {
	f, err := s.root.OpenFile(path.Clean(file), os.O_RDONLY|syscall.O_NOFOLLOW, 0)
	if errors.Is(err, os.ErrNotExist) {
		return blob.ErrBlobNotFound
	}
	if err != nil {
		return err
	}
	defer f.Close()
	info, err := f.Stat()
	if err != nil {
		return err
	}
	if length < 0 {
		offset, length = 0, info.Size()
	}
	if offset < 0 || offset > info.Size() || length > info.Size()-offset {
		return blob.ErrInvalidRange
	}
	out.Reset()
	if _, err := io.Copy(out, io.NewSectionReader(f, offset, length)); err != nil {
		return err
	}
	return blob.EnsureLengthExactly(out.Length(), length)
}

func (s *localStorage) GetMetadataFromPath(ctx context.Context, _, file string) (blob.Metadata, error) {
	info, err := s.root.Lstat(path.Clean(file))
	if errors.Is(err, os.ErrNotExist) {
		return blob.Metadata{}, blob.ErrBlobNotFound
	}
	if err != nil {
		return blob.Metadata{}, err
	}
	return blob.Metadata{Length: info.Size(), Timestamp: info.ModTime()}, nil
}

func (s *localStorage) PutBlobInPath(ctx context.Context, _, file string, data blob.Bytes, options blob.PutOptions) error {
	if options.HasRetentionOptions() || options.DoNotRecreate {
		return blob.ErrUnsupportedPutBlobOption
	}
	var buffer bytes.Buffer
	if _, err := data.WriteTo(&buffer); err != nil {
		return err
	}
	info, err := WriteBlobFile(s.root, path.Clean(file), &buffer, int64(data.Length()), options.SetModTime)
	if err != nil {
		return err
	}
	if options.GetModTime != nil {
		*options.GetModTime = info.ModTime()
	}
	return nil
}

func (s *localStorage) DeleteBlobInPath(ctx context.Context, _, file string) error {
	return RemoveBlobFile(s.root, path.Clean(file))
}

func (s *localStorage) ReadDir(ctx context.Context, dir string) ([]os.FileInfo, error) {
	f, err := s.root.Open(path.Clean(dir))
	if err != nil {
		return nil, err
	}
	defer f.Close()
	entries, err := f.ReadDir(-1)
	if err != nil {
		return nil, err
	}
	result := make([]os.FileInfo, 0, len(entries))
	for _, entry := range entries {
		info, err := entry.Info()
		if errors.Is(err, os.ErrNotExist) {
			continue
		}
		if err != nil {
			return nil, err
		}
		result = append(result, info)
	}
	return result, nil
}
