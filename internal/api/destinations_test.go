package api

import (
	"bytes"
	"encoding/json"
	"testing"

	"github.com/google/uuid"
	"github.com/sirrobot01/synod/internal/destination"
	"github.com/sirrobot01/synod/internal/repository"
	"github.com/sirrobot01/synod/internal/secret"
)

func TestDestinationAPIKeepsCredentialsPrivate(t *testing.T) {
	server, _, _ := identityServer(t)
	keyID := uuid.New()
	ring, err := secret.NewKeyring(keyID, map[uuid.UUID][]byte{keyID: bytes.Repeat([]byte{9}, 32)})
	if err != nil {
		t.Fatal(err)
	}
	server.CredentialKeyring = ring
	entry := destination.Entry{Destination: destination.Destination{ID: uuid.New(), Name: "Cloud", Location: repository.Location{Kind: "s3", S3: &repository.S3Location{Endpoint: "https://storage.example", Bucket: "backup"}}}, Credentials: repository.Credentials{Password: "private-password", AccessKeyID: "private-access", SecretAccessKey: "private-secret"}}
	raw, err := json.Marshal(entry)
	if err != nil {
		t.Fatal(err)
	}
	if response := call(t, server, "POST", "/v1/destinations", string(raw)); response.Code != 401 {
		t.Fatalf("anonymous add: %d", response.Code)
	}
	if response := call(t, server, "GET", "/v1/destinations", ""); response.Code != 401 {
		t.Fatalf("anonymous list: %d", response.Code)
	}
	for range 2 {
		response := call(t, server, "POST", "/v1/destinations", string(raw), withBootstrap)
		var saved destination.Destination
		if response.Code != 200 || json.Unmarshal(response.Body.Bytes(), &saved) != nil || saved.ID != entry.Destination.ID {
			t.Fatalf("add: %d", response.Code)
		}
		if bytes.Contains(response.Body.Bytes(), []byte("private-")) || bytes.Contains(response.Body.Bytes(), []byte("credentials")) {
			t.Fatal("add leaked credentials")
		}
	}
	response := call(t, server, "GET", "/v1/destinations", "", withBootstrap)
	var listing destination.Listing
	if response.Code != 200 || json.Unmarshal(response.Body.Bytes(), &listing) != nil || len(listing.Destinations) != 1 || listing.Revision != 2 {
		t.Fatalf("list: %d", response.Code)
	}
	if bytes.Contains(response.Body.Bytes(), []byte("private-")) {
		t.Fatal("list leaked credentials")
	}
	path := "/v1/destinations/" + entry.Destination.ID.String()
	if response := call(t, server, "DELETE", path, ""); response.Code != 401 {
		t.Fatalf("anonymous remove: %d", response.Code)
	}
	for range 2 {
		if response := call(t, server, "DELETE", path, "", withBootstrap); response.Code != 204 {
			t.Fatalf("remove: %d", response.Code)
		}
	}
	response = call(t, server, "GET", "/v1/destinations", "", withBootstrap)
	if response.Code != 200 || json.Unmarshal(response.Body.Bytes(), &listing) != nil || len(listing.Destinations) != 0 || listing.Revision != 3 {
		t.Fatalf("removed list: %d", response.Code)
	}
	if response := call(t, server, "POST", "/v1/destinations", string(raw), withBootstrap); response.Code != 409 {
		t.Fatalf("removed replay: %d", response.Code)
	}
}
