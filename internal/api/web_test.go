package api

import (
	"io"
	"io/fs"
	"log/slog"
	"net/http"
	"regexp"
	"strings"
	"testing"
	"testing/fstest"

	"github.com/google/uuid"
	"github.com/sirrobot01/synod/internal/auth"
	"github.com/sirrobot01/synod/internal/web"
)

func appServer(t *testing.T) *Server {
	t.Helper()
	admin, err := auth.NewAdminToken(bootstrapToken, "bootstrap-admin")
	if err != nil {
		t.Fatal(err)
	}
	server, err := NewServer(apiTestStore(t), admin, uuid.New(), slog.New(slog.NewTextHandler(io.Discard, nil)))
	if err != nil {
		t.Fatal(err)
	}
	server.SetAssets(fstest.MapFS{
		"index.html":           &fstest.MapFile{Data: []byte("<!DOCTYPE html><title>Synod</title>")},
		"assets/index-abc.js":  &fstest.MapFile{Data: []byte("export const a = 1;")},
		"assets/index-abc.css": &fstest.MapFile{Data: []byte(":root{}")},
	})
	return server
}

func TestApplicationIsServedFromTheSameOrigin(t *testing.T) {
	server := appServer(t)
	rec := call(t, server, "GET", "/", "")
	if rec.Code != 200 || !strings.Contains(rec.Body.String(), "<title>Synod</title>") {
		t.Fatalf("root: %d %s", rec.Code, rec.Body)
	}
	if got := rec.Header().Get("Content-Type"); !strings.HasPrefix(got, "text/html") {
		t.Fatalf("a document was served as %q", got)
	}
	// A browser route that the server does not know still loads the application.
	if rec := call(t, server, "GET", "/apps/photos", ""); rec.Code != 200 || !strings.Contains(rec.Body.String(), "Synod") {
		t.Fatalf("browser route: %d %s", rec.Code, rec.Body)
	}
	// A hashed asset is immutable. The index never caches.
	asset := call(t, server, "GET", "/assets/index-abc.js", "")
	if asset.Code != 200 || !strings.Contains(asset.Header().Get("Cache-Control"), "immutable") {
		t.Fatalf("asset cache: %d %q", asset.Code, asset.Header().Get("Cache-Control"))
	}
	if got := call(t, server, "GET", "/", "").Header().Get("Cache-Control"); got != "no-store" {
		t.Fatalf("index cache %q", got)
	}
}

func TestApplicationCarriesItsSecurityHeaders(t *testing.T) {
	server := appServer(t)
	header := call(t, server, "GET", "/", "").Header()
	policy := header.Get("Content-Security-Policy")
	for _, want := range []string{"default-src 'self'", "script-src 'self'", "style-src 'self'", "frame-ancestors 'none'", "base-uri 'none'"} {
		if !strings.Contains(policy, want) {
			t.Fatalf("policy %q lacks %q", policy, want)
		}
	}
	// Inline code must be impossible, whether it is script or style.
	if strings.Contains(policy, "unsafe-inline") || strings.Contains(policy, "unsafe-eval") {
		t.Fatalf("policy permits inline code: %q", policy)
	}
	if strings.Contains(policy, "*") {
		t.Fatalf("policy permits a foreign origin: %q", policy)
	}
	if header.Get("X-Frame-Options") != "DENY" || header.Get("X-Content-Type-Options") != "nosniff" {
		t.Fatalf("framing headers: %+v", header)
	}
}

// The application must never shadow the API, including its failures.
func TestAPIKeepsItsJSONAnswers(t *testing.T) {
	server := appServer(t)
	missing := call(t, server, "GET", "/v1/nope", "")
	if missing.Code != 404 || !strings.Contains(missing.Body.String(), `"not_found"`) {
		t.Fatalf("unknown API path: %d %s", missing.Code, missing.Body)
	}
	if got := missing.Header().Get("Content-Type"); !strings.HasPrefix(got, "application/json") {
		t.Fatalf("an API failure was served as %q", got)
	}
	wrong := call(t, server, "DELETE", "/v1/nodes", "", withBootstrap)
	if wrong.Code != 405 || wrong.Header().Get("Allow") == "" {
		t.Fatalf("method not allowed: %d allow=%q", wrong.Code, wrong.Header().Get("Allow"))
	}
	// The application answers reads only.
	if rec := call(t, server, "POST", "/anything", ""); rec.Code != 405 || rec.Header().Get("Allow") != "GET, HEAD" {
		t.Fatalf("a write to the application: %d %q", rec.Code, rec.Header().Get("Allow"))
	}
}

// looseFS answers any name, including one that escapes its root.
// io/fs requires Open to refuse such a name, but SetAssets accepts any
// implementation, so the handler checks the name itself.
type looseFS struct{ inner fstest.MapFS }

func (l looseFS) Open(name string) (fs.File, error) {
	if strings.Contains(name, "..") {
		// A conforming file system refuses this. This one answers, which is the
		// whole point: the handler must not depend on the implementation behaving.
		escaped := fstest.MapFS{"escaped": &fstest.MapFile{Data: []byte("module github.com/sirrobot01/synod")}}
		return escaped.Open("escaped")
	}
	return l.inner.Open(name)
}

// A traversal must not reach outside the built application.
func TestApplicationRefusesEscapingPaths(t *testing.T) {
	server := appServer(t)
	for _, path := range []string{"/../go.mod", "/assets/../../go.mod", "/%2e%2e/go.mod"} {
		rec := call(t, server, "GET", path, "")
		if rec.Code == 200 && strings.Contains(rec.Body.String(), "module github.com/sirrobot01/synod") {
			t.Fatalf("%s escaped the application", path)
		}
	}
	// A file system that does not honour the io/fs contract must not defeat the handler.
	server.SetAssets(looseFS{inner: fstest.MapFS{"index.html": &fstest.MapFile{Data: []byte("<title>Synod</title>")}}})
	file, name, err := server.openAsset("../go.mod")
	if err != nil {
		t.Fatal(err)
	}
	defer file.Close()
	if name != "index.html" {
		t.Fatalf("the handler opened %q outside the application", name)
	}
}

// A server built without assets still serves its API.
func TestServerWithoutAssetsStillServesTheAPI(t *testing.T) {
	admin, err := auth.NewAdminToken(bootstrapToken, "bootstrap-admin")
	if err != nil {
		t.Fatal(err)
	}
	server, err := NewServer(apiTestStore(t), admin, uuid.New(), slog.New(slog.NewTextHandler(io.Discard, nil)))
	if err != nil {
		t.Fatal(err)
	}
	if rec := call(t, server, "GET", "/", ""); rec.Code != http.StatusNotFound {
		t.Fatalf("no assets: %d", rec.Code)
	}
	if rec := call(t, server, "GET", "/v1/nope", ""); rec.Code != 404 || !strings.Contains(rec.Body.String(), "not_found") {
		t.Fatalf("API without assets: %d %s", rec.Code, rec.Body)
	}
}

// The shipped index must reference its assets absolutely.
// A relative reference resolves against the current route, so a deep link would
// request assets that do not exist and the page would never boot.
func TestShippedIndexReferencesAbsoluteAssets(t *testing.T) {
	assets, err := web.Assets()
	if err != nil {
		t.Fatal(err)
	}
	index, err := fs.ReadFile(assets, "index.html")
	if err != nil {
		t.Fatal(err)
	}
	references := regexp.MustCompile(`(?:src|href)="([^"]+)"`).FindAllStringSubmatch(string(index), -1)
	if len(references) == 0 {
		t.Fatal("the index references no assets")
	}
	for _, reference := range references {
		target := reference[1]
		if !strings.HasPrefix(target, "/") {
			t.Fatalf("the index references %q, which a deep link cannot resolve", target)
		}
		if _, err := fs.Stat(assets, strings.TrimPrefix(target, "/")); err != nil {
			t.Fatalf("the index references %q, which the bundle does not carry", target)
		}
	}
}

// A missing asset is a miss. Answering with the index returns HTML where a script
// is expected, which a browser reports as a parse error instead of a plain 404.
func TestMissingAssetDoesNotReturnTheApplication(t *testing.T) {
	server := appServer(t)
	rec := call(t, server, "GET", "/assets/index-gone.js", "")
	if rec.Code != 404 {
		t.Fatalf("a stale asset answered %d %s", rec.Code, rec.Body)
	}
	if strings.Contains(rec.Body.String(), "<title>") {
		t.Fatal("a stale asset answered with the application document")
	}
	// A browser route that is not an asset still loads the application.
	if rec := call(t, server, "GET", "/operations/op-1", ""); rec.Code != 200 {
		t.Fatalf("browser route: %d", rec.Code)
	}
}

// A browser route that collides with a directory in the bundle still loads the app.
func TestDirectoryPathStillLoadsTheApplication(t *testing.T) {
	server := appServer(t)
	rec := call(t, server, "GET", "/assets", "")
	if rec.Code != 200 || !strings.Contains(rec.Body.String(), "<title>Synod</title>") {
		t.Fatalf("a directory path answered %d %s", rec.Code, rec.Body)
	}
}
