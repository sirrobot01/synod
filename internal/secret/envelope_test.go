package secret

import (
	"bytes"
	"crypto/rand"
	"encoding/json"
	"errors"
	"sync"
	"testing"

	"github.com/google/uuid"
)

func TestEnvelopeIsolationAndRotation(t *testing.T) {
	oldID, newID := uuid.New(), uuid.New()
	oldKey, newKey := make([]byte, 32), make([]byte, 32)
	rand.Read(oldKey)
	rand.Read(newKey)
	inputKeys := map[uuid.UUID][]byte{oldID: oldKey}
	old, err := NewKeyring(oldID, inputKeys)
	if err != nil {
		t.Fatal(err)
	}
	binding := Binding{InstallationID: uuid.New(), OwnerType: "backup_target", OwnerID: uuid.New(), CredentialID: uuid.New(), Version: 1}
	plaintext := []byte("private-credential-value")
	first, err := old.Seal(binding, plaintext)
	if err != nil {
		t.Fatal(err)
	}
	encoded, err := json.Marshal(first)
	if err != nil {
		t.Fatal(err)
	}
	if bytes.Contains(encoded, plaintext) {
		t.Fatal("serialized envelope contains plaintext")
	}
	var decoded Envelope
	if err := json.Unmarshal(encoded, &decoded); err != nil {
		t.Fatal(err)
	}
	got, err := old.Open(binding, decoded)
	if err != nil || !bytes.Equal(got, plaintext) {
		t.Fatalf("round trip: %v", err)
	}
	next, err := NewKeyring(newID, map[uuid.UUID][]byte{oldID: oldKey, newID: newKey})
	if err != nil {
		t.Fatal(err)
	}
	if got, err := next.Open(binding, first); err != nil || !bytes.Equal(got, plaintext) {
		t.Fatalf("old key recovery: %v", err)
	}
	second, err := next.Seal(binding, plaintext)
	if err != nil || second.KeyID != newID {
		t.Fatalf("new key selection: %v", err)
	}
	if _, err := old.Open(binding, second); !errors.Is(err, ErrKeyUnavailable) {
		t.Fatalf("missing key: %v", err)
	}
	wrong, err := NewKeyring(oldID, map[uuid.UUID][]byte{oldID: newKey})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := wrong.Open(binding, first); !errors.Is(err, ErrDecryption) {
		t.Fatalf("wrong key: %v", err)
	}
	for _, test := range []struct {
		name   string
		change func(*Binding)
	}{
		{"installation", func(b *Binding) { b.InstallationID = uuid.New() }},
		{"owner type", func(b *Binding) { b.OwnerType = "cluster" }},
		{"owner", func(b *Binding) { b.OwnerID = uuid.New() }},
		{"credential", func(b *Binding) { b.CredentialID = uuid.New() }},
		{"version", func(b *Binding) { b.Version++ }},
	} {
		t.Run(test.name, func(t *testing.T) {
			changed := binding
			test.change(&changed)
			if got, err := next.Open(changed, first); !errors.Is(err, ErrDecryption) || got != nil {
				t.Fatalf("changed binding accepted: %v", err)
			}
		})
	}
	// The key ID is authenticated even if an operator duplicates key bytes.
	aliases, err := NewKeyring(oldID, map[uuid.UUID][]byte{oldID: oldKey, newID: oldKey})
	if err != nil {
		t.Fatal(err)
	}
	retagged := first
	retagged.KeyID = newID
	if _, err := aliases.Open(binding, retagged); !errors.Is(err, ErrDecryption) {
		t.Fatalf("changed key identity: %v", err)
	}
	// The keyring owns cipher state. Changes to caller buffers cannot change it.
	clear(oldKey)
	delete(inputKeys, oldID)
	if got, err := old.Open(binding, first); err != nil || !bytes.Equal(got, plaintext) {
		t.Fatalf("caller changed keyring: %v", err)
	}
}

func TestConcurrentNonceIndependence(t *testing.T) {
	key := make([]byte, 32)
	rand.Read(key)
	id := uuid.New()
	ring, err := NewKeyring(id, map[uuid.UUID][]byte{id: key})
	if err != nil {
		t.Fatal(err)
	}
	binding := Binding{uuid.New(), "cluster", uuid.New(), uuid.New(), 1}
	results := make(chan Envelope, 128)
	var workers sync.WaitGroup
	for range 128 {
		workers.Go(func() {
			sealed, err := ring.Seal(binding, []byte("same secret"))
			if err != nil {
				t.Error(err)
				return
			}
			if got, err := ring.Open(binding, sealed); err != nil || string(got) != "same secret" {
				t.Errorf("concurrent round trip: %v", err)
				return
			}
			results <- sealed
		})
	}
	workers.Wait()
	close(results)
	nonces := make(map[string]bool)
	for result := range results {
		nonce := string(result.Sealed[:12])
		if nonces[nonce] {
			t.Fatal("repeated nonce")
		}
		nonces[nonce] = true
	}
	if len(nonces) != 128 {
		t.Fatalf("completed encryptions=%d", len(nonces))
	}
}

func TestRejectMalformedEnvelope(t *testing.T) {
	id := uuid.New()
	ring, err := NewKeyring(id, map[uuid.UUID][]byte{id: bytes.Repeat([]byte{7}, 32)})
	if err != nil {
		t.Fatal(err)
	}
	binding := Binding{uuid.New(), "cluster", uuid.New(), uuid.New(), 1}
	envelope, err := ring.Seal(binding, []byte("value"))
	if err != nil {
		t.Fatal(err)
	}
	for _, test := range []struct {
		name   string
		change func(*Envelope)
	}{
		{"format", func(e *Envelope) { e.FormatVersion++ }},
		{"key ID", func(e *Envelope) { e.KeyID = uuid.Nil }},
		{"short", func(e *Envelope) { e.Sealed = e.Sealed[:12] }},
		{"empty", func(e *Envelope) { e.Sealed = nil }},
		{"large", func(e *Envelope) { e.Sealed = make([]byte, MaxPlaintextBytes+29) }},
		{"corrupt nonce", func(e *Envelope) { e.Sealed[0] ^= 1 }},
		{"corrupt tag", func(e *Envelope) { e.Sealed[len(e.Sealed)-1] ^= 1 }},
		{"corrupt ciphertext", func(e *Envelope) { e.Sealed[12] ^= 1 }},
	} {
		t.Run(test.name, func(t *testing.T) {
			changed := envelope
			changed.Sealed = bytes.Clone(envelope.Sealed)
			test.change(&changed)
			if got, err := ring.Open(binding, changed); err == nil || got != nil {
				t.Fatal("accepted invalid ciphertext")
			}
		})
	}
	for _, input := range [][]byte{nil, {}, make([]byte, MaxPlaintextBytes+1)} {
		if _, err := ring.Seal(binding, input); !errors.Is(err, ErrInvalidEnvelope) {
			t.Fatalf("invalid plaintext: %v", err)
		}
	}
	largest := bytes.Repeat([]byte{3}, MaxPlaintextBytes)
	sealed, err := ring.Seal(binding, largest)
	if err != nil {
		t.Fatal(err)
	}
	if got, err := ring.Open(binding, sealed); err != nil || !bytes.Equal(got, largest) {
		t.Fatalf("maximum size round trip: %v", err)
	}
	for _, owner := range []string{"", "Cluster", "cluster:node", "bad owner", "bad\x00owner"} {
		invalid := binding
		invalid.OwnerType = owner
		if _, err := ring.Seal(invalid, []byte("value")); !errors.Is(err, ErrInvalidBinding) {
			t.Fatalf("owner type %q: %v", owner, err)
		}
	}
	var absent *Keyring
	if _, err := absent.Open(binding, envelope); !errors.Is(err, ErrKeyUnavailable) {
		t.Fatalf("absent keyring: %v", err)
	}
}

func TestRejectInvalidKeyring(t *testing.T) {
	id := uuid.New()
	for _, test := range []struct {
		active uuid.UUID
		keys   map[uuid.UUID][]byte
	}{
		{uuid.Nil, nil},
		{id, nil},
		{id, map[uuid.UUID][]byte{uuid.New(): make([]byte, 32)}},
		{id, map[uuid.UUID][]byte{id: make([]byte, 16)}},
		{id, map[uuid.UUID][]byte{id: make([]byte, 32), uuid.Nil: make([]byte, 32)}},
	} {
		if _, err := NewKeyring(test.active, test.keys); !errors.Is(err, ErrKeyUnavailable) {
			t.Fatalf("invalid keyring: %v", err)
		}
	}
}
