package auth

import (
	"errors"
	"net/http/httptest"
	"strings"
	"testing"
)

func TestAdminTokenAuthentication(t *testing.T) {
	token := strings.Repeat("s", 32)
	authenticator, err := NewAdminToken(token, "bootstrap-admin")
	if err != nil {
		t.Fatal(err)
	}
	for _, test := range []struct {
		name    string
		headers []string
		query   string
		allowed bool
	}{
		{name: "valid", headers: []string{"Bearer " + token}, allowed: true},
		{name: "case insensitive scheme", headers: []string{"bEaReR " + token}, allowed: true},
		{name: "missing"},
		{name: "query only", query: "?access_token=" + token},
		{name: "duplicate", headers: []string{"Bearer " + token, "Bearer " + token}},
		{name: "wrong scheme", headers: []string{"Basic " + token}},
		{name: "wrong token", headers: []string{"Bearer " + strings.Repeat("x", 32)}},
		{name: "missing separator", headers: []string{"Bearer" + token}},
		{name: "empty token", headers: []string{"Bearer "}},
		{name: "extra space", headers: []string{"Bearer  " + token}},
		{name: "trailing space", headers: []string{"Bearer " + token + " "}},
		{name: "comma joined", headers: []string{"Bearer " + token + ", Bearer " + token}},
		{name: "oversized", headers: []string{"Bearer " + strings.Repeat("s", 257)}},
	} {
		t.Run(test.name, func(t *testing.T) {
			request := httptest.NewRequest("GET", "https://synod.example/v1/apps"+test.query, nil)
			for _, header := range test.headers {
				request.Header.Add("Authorization", header)
			}
			actorID, err := authenticator.Authenticate(request)
			if test.allowed {
				if err != nil || actorID != "bootstrap-admin" {
					t.Fatalf("actor = %q, error = %v", actorID, err)
				}
				return
			}
			if !errors.Is(err, ErrUnauthenticated) || actorID != "" {
				t.Fatalf("rejected request returned actor = %q, error = %v", actorID, err)
			}
		})
	}
}

func TestAdminTokenConfiguration(t *testing.T) {
	for _, test := range []struct {
		name  string
		token string
		actor string
	}{
		{name: "short token", token: strings.Repeat("s", 31), actor: "admin"},
		{name: "long token", token: strings.Repeat("s", 257), actor: "admin"},
		{name: "token whitespace", token: strings.Repeat("s", 31) + " ", actor: "admin"},
		{name: "token newline", token: strings.Repeat("s", 31) + "\n", actor: "admin"},
		{name: "unicode token", token: strings.Repeat("é", 32), actor: "admin"},
		{name: "empty actor", token: strings.Repeat("s", 32)},
		{name: "long actor", token: strings.Repeat("s", 32), actor: strings.Repeat("a", 257)},
		{name: "actor control", token: strings.Repeat("s", 32), actor: "admin\n"},
	} {
		t.Run(test.name, func(t *testing.T) {
			value, err := NewAdminToken(test.token, test.actor)
			if err == nil || value != nil {
				t.Fatal("invalid configuration was accepted")
			}
			if strings.Contains(err.Error(), test.token) {
				t.Fatal("configuration error contains token material")
			}
		})
	}
}
