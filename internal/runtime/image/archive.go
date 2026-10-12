package image

import (
	"context"
	"crypto/sha256"
	"fmt"
	"io"
	"os"

	v1 "github.com/google/go-containerregistry/pkg/v1"
	"github.com/google/go-containerregistry/pkg/v1/tarball"
	"github.com/google/go-containerregistry/pkg/v1/validate"
)

// StageArchive keeps image bytes in a private unlinked file. The caller closes it.
func StageArchive(ctx context.Context, write func(io.Writer) error) (*os.File, string, int64, error) {
	file, err := os.CreateTemp("", "synod-image-*")
	if err != nil {
		return nil, "", 0, err
	}
	if err := os.Remove(file.Name()); err != nil {
		file.Close()
		return nil, "", 0, err
	}
	ready := false
	defer func() {
		if !ready {
			file.Close()
		}
	}()
	digest := sha256.New()
	if err := write(io.MultiWriter(file, digest)); err != nil {
		return nil, "", 0, err
	}
	if err := ctx.Err(); err != nil {
		return nil, "", 0, err
	}
	info, err := file.Stat()
	if err != nil {
		return nil, "", 0, err
	}
	ready = true
	return file, fmt.Sprintf("sha256:%x", digest.Sum(nil)), info.Size(), nil
}

// CheckArchive checks the complete image, including each uncompressed layer's digest.
// The returned image reads only from file. It must not outlive file or ctx.
func CheckArchive(ctx context.Context, file *os.File, id, architecture string) (v1.Image, error) {
	stop := context.AfterFunc(ctx, func() { file.Close() })
	defer stop()
	info, err := file.Stat()
	if err != nil {
		return nil, err
	}
	img, err := tarball.Image(func() (io.ReadCloser, error) {
		return &archiveSection{io.NewSectionReader(file, 0, info.Size())}, ctx.Err()
	}, nil)
	if err != nil {
		return nil, &Error{Code: InvalidManifest, Cause: err}
	}
	configID, err := img.ConfigName()
	if err != nil || configID.String() != id {
		return nil, &Error{Code: InvalidManifest, Cause: err}
	}
	config, err := img.ConfigFile()
	if err != nil {
		return nil, &Error{Code: InvalidManifest, Cause: err}
	}
	if config.OS != "linux" || config.Architecture != architecture || (architecture != "amd64" && architecture != "arm64") || (config.Variant != "" && !(architecture == "arm64" && config.Variant == "v8") && !(architecture == "amd64" && config.Variant == "v1")) {
		return nil, &Error{Code: UnsupportedPlatform}
	}
	if err := validate.Image(img); err != nil {
		return nil, &Error{Code: InvalidManifest, Cause: err}
	}
	return img, ctx.Err()
}

// ArchiveConfig returns the config digest of the one image in a docker save archive.
func ArchiveConfig(ctx context.Context, file *os.File) (string, error) {
	info, err := file.Stat()
	if err != nil {
		return "", err
	}
	img, err := tarball.Image(func() (io.ReadCloser, error) {
		return &archiveSection{io.NewSectionReader(file, 0, info.Size())}, ctx.Err()
	}, nil)
	if err != nil {
		return "", &Error{Code: InvalidManifest, Cause: err}
	}
	config, err := img.ConfigName()
	if err != nil {
		return "", &Error{Code: InvalidManifest, Cause: err}
	}
	return config.String(), ctx.Err()
}

// Keep Seek visible so tar readers skip layer bodies when they read metadata.
type archiveSection struct{ *io.SectionReader }

func (*archiveSection) Close() error { return nil }
