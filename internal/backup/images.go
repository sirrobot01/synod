package backup

import (
	"encoding/hex"
	"slices"
	"strings"
)

// ImageVariant names one complete image archive in the snapshot tree.
type ImageVariant struct {
	Architecture string `json:"architecture"`
	ID           string `json:"id"`
	Archive      string `json:"archive"`
	Digest       string `json:"digest"`
	Size         int64  `json:"size"`
}

func (v ImageVariant) Validate() error {
	if v.Architecture != "amd64" && v.Architecture != "arm64" {
		return ErrInvalid
	}
	for _, value := range []string{v.ID, v.Digest} {
		raw, ok := strings.CutPrefix(value, "sha256:")
		if !ok || len(raw) != 64 || strings.ToLower(raw) != raw {
			return ErrInvalid
		}
		if _, err := hex.DecodeString(raw); err != nil {
			return ErrInvalid
		}
	}
	if v.Archive != "images/"+strings.TrimPrefix(v.ID, "sha256:")+".tar" || v.Size <= 0 {
		return ErrInvalid
	}
	return nil
}

func (image Image) Validate() error {
	if image.ContainerID == "" || image.Reference == "" || len(image.Variants) == 0 || len(image.Variants) > 2 || len(image.Architectures) != len(image.Variants) {
		return ErrInvalid
	}
	seen := make(map[string]bool)
	for _, variant := range image.Variants {
		if variant.Validate() != nil || seen[variant.Architecture] || !slices.Contains(image.Architectures, variant.Architecture) {
			return ErrInvalid
		}
		seen[variant.Architecture] = true
	}
	// The containerd image store gives SourceID an index or manifest digest, so the
	// running image is found by its platform, not by its ID.
	if !seen[image.SourceArchitecture] {
		return ErrInvalid
	}
	return nil
}
