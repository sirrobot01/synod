// Package secret encrypts credential values with external, versioned wrapping keys.
package secret

import (
	"crypto/aes"
	"crypto/cipher"
	"encoding/binary"
	"errors"
	"strings"

	"github.com/google/uuid"
)

const MaxPlaintextBytes = 1 << 20
const FormatVersion = 1

var ErrInvalidBinding = errors.New("invalid secret binding")
var ErrInvalidEnvelope = errors.New("invalid secret envelope")
var ErrKeyUnavailable = errors.New("secret wrapping key unavailable")
var ErrDecryption = errors.New("secret authentication failed")

// Binding comes from the authorized metadata record, never from the ciphertext.
type Binding struct {
	InstallationID uuid.UUID
	OwnerType      string
	OwnerID        uuid.UUID
	CredentialID   uuid.UUID
	Version        int64
}

// Envelope contains ciphertext and its wrapping key identity. It contains no plaintext.
type Envelope struct {
	FormatVersion int       `json:"format_version"`
	KeyID         uuid.UUID `json:"key_id"`
	Sealed        []byte    `json:"sealed"`
}

// Keyring is immutable and safe for concurrent use. Retain old keys for recovery.
type Keyring struct {
	active uuid.UUID
	keys   map[uuid.UUID]cipher.AEAD
}

// NewKeyring builds a keyring from 32-byte wrapping keys and selects the active key.
func NewKeyring(active uuid.UUID, keys map[uuid.UUID][]byte) (*Keyring, error) {
	if active == uuid.Nil || len(keys) == 0 {
		return nil, ErrKeyUnavailable
	}
	ring := &Keyring{active: active, keys: make(map[uuid.UUID]cipher.AEAD, len(keys))}
	for id, key := range keys {
		if id == uuid.Nil || len(key) != 32 {
			return nil, ErrKeyUnavailable
		}
		block, err := aes.NewCipher(key)
		if err != nil {
			return nil, ErrKeyUnavailable
		}
		aead, err := cipher.NewGCMWithRandomNonce(block)
		if err != nil {
			return nil, ErrKeyUnavailable
		}
		ring.keys[id] = aead
	}
	if ring.keys[active] == nil {
		return nil, ErrKeyUnavailable
	}
	return ring, nil
}

// Seal encrypts plaintext with the active key and binds it to the metadata.
// The binding is authenticated data, so an envelope cannot be moved to another owner.
func (k *Keyring) Seal(binding Binding, plaintext []byte) (Envelope, error) {
	if k == nil || k.keys[k.active] == nil {
		return Envelope{}, ErrKeyUnavailable
	}
	if len(plaintext) == 0 || len(plaintext) > MaxPlaintextBytes {
		return Envelope{}, ErrInvalidEnvelope
	}
	associated, err := binding.associatedData(k.active)
	if err != nil {
		return Envelope{}, err
	}
	// The standard library generates a fresh 96-bit nonce and prefixes it to ciphertext.
	sealed := k.keys[k.active].Seal(nil, nil, plaintext, associated)
	return Envelope{FormatVersion: FormatVersion, KeyID: k.active, Sealed: sealed}, nil
}

// Open decrypts an envelope and checks the same binding.
// A wrong binding fails as ErrDecryption and never reports which part did not match.
func (k *Keyring) Open(binding Binding, envelope Envelope) ([]byte, error) {
	if envelope.FormatVersion != FormatVersion || envelope.KeyID == uuid.Nil || len(envelope.Sealed) > MaxPlaintextBytes+28 {
		return nil, ErrInvalidEnvelope
	}
	associated, err := binding.associatedData(envelope.KeyID)
	if err != nil {
		return nil, err
	}
	if k == nil || k.keys[envelope.KeyID] == nil {
		return nil, ErrKeyUnavailable
	}
	aead := k.keys[envelope.KeyID]
	if len(envelope.Sealed) <= aead.Overhead() {
		return nil, ErrInvalidEnvelope
	}
	plaintext, err := aead.Open(nil, nil, envelope.Sealed, associated)
	if err != nil {
		return nil, ErrDecryption
	}
	return plaintext, nil
}

// Length-prefix the owner type so different bindings cannot share an encoding.
func (b Binding) associatedData(keyID uuid.UUID) ([]byte, error) {
	if b.InstallationID == uuid.Nil || b.OwnerID == uuid.Nil || b.CredentialID == uuid.Nil || b.Version < 1 || len(b.OwnerType) < 1 || len(b.OwnerType) > 64 || strings.ContainsFunc(b.OwnerType, func(r rune) bool { return (r < 'a' || r > 'z') && r != '_' }) {
		return nil, ErrInvalidBinding
	}
	data := []byte("synod.secret.v1\x00")
	data = append(data, keyID[:]...)
	data = append(data, b.InstallationID[:]...)
	data = append(data, byte(len(b.OwnerType)))
	data = append(data, b.OwnerType...)
	data = append(data, b.OwnerID[:]...)
	data = append(data, b.CredentialID[:]...)
	data = binary.BigEndian.AppendUint64(data, uint64(b.Version))
	return data, nil
}
