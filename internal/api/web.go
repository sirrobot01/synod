package api

import (
	"io"
	"io/fs"
	"net/http"
	"strings"

	"github.com/google/uuid"
)

// contentPolicy forbids inline code and every foreign origin.
// The application is served from one origin and fetches nothing from anywhere else,
// so a policy this narrow costs nothing and removes a large class of injection.
// Every screen styles itself from the built stylesheet and none writes a style
// attribute, so inline style stays forbidden too. A component that positions
// itself with a style attribute would need this widened, and the change belongs
// with the code that needs it.
const contentPolicy = "default-src 'self'; script-src 'self'; style-src 'self'; img-src 'self' data:; " +
	"font-src 'self'; connect-src 'self'; frame-ancestors 'none'; base-uri 'none'; form-action 'self'"

// serveApp answers a browser request with the built application.
//
// Vite writes a content hash into every asset name, so an asset is immutable and
// caches for a year. index.html carries no hash, so it is never cached. Any other
// path returns index.html, because the application routes in the browser.
func (s *Server) serveApp(w http.ResponseWriter, r *http.Request) {
	if s.assets == nil {
		s.writeError(w, r, http.StatusNotFound, "not_found", "This server carries no web application.", uuid.Nil)
		return
	}
	if r.Method != http.MethodGet && r.Method != http.MethodHead {
		w.Header().Set("Allow", "GET, HEAD")
		s.writeError(w, r, http.StatusMethodNotAllowed, "method_not_allowed", "The endpoint does not support this method.", uuid.Nil)
		return
	}
	name := strings.TrimPrefix(r.URL.Path, "/")
	// A build writes a content hash into every asset name, so a missing asset is a
	// real miss. Answering it with the index would return HTML where a script is
	// expected, and the browser would report a parse error instead of a plain 404.
	if strings.HasPrefix(name, assetPrefix) && !s.hasAsset(name) {
		s.writeError(w, r, http.StatusNotFound, "not_found", "The asset was not found.", uuid.Nil)
		return
	}
	file, actual, err := s.openAsset(name)
	if err != nil {
		s.logger.Warn("open web asset failed", "path", name, "error", err)
		s.writeError(w, r, http.StatusNotFound, "not_found", "The application is unavailable.", uuid.Nil)
		return
	}
	defer file.Close()
	info, err := file.Stat()
	reader, seekable := file.(io.ReadSeeker)
	if err != nil || !seekable {
		s.logger.Warn("read web asset failed", "path", actual, "seekable", seekable, "error", err)
		s.writeError(w, r, http.StatusNotFound, "not_found", "The application is unavailable.", uuid.Nil)
		return
	}
	header := w.Header()
	// begin sets the API content type. A document is not JSON, so remove it and
	// let ServeContent choose from the file name.
	header.Del("Content-Type")
	header.Set("Content-Security-Policy", contentPolicy)
	header.Set("X-Frame-Options", "DENY")
	header.Set("Referrer-Policy", "no-referrer")
	if strings.HasPrefix(actual, "assets/") {
		header.Set("Cache-Control", "public, max-age=31536000, immutable")
	} else {
		header.Set("Cache-Control", "no-store")
	}
	http.ServeContent(w, r, actual, info.ModTime(), reader)
}

const assetPrefix = "assets/"

// hasAsset reports a readable file, so a miss under the asset prefix stays a miss.
func (s *Server) hasAsset(name string) bool {
	file, actual, err := s.openAsset(name)
	if err != nil {
		return false
	}
	defer file.Close()
	return actual == name
}

// openAsset returns the named file, or the application index when it does not exist.
// It reports which of the two it opened, so the caller can set the right cache rule.
// A path that escapes the built application never reaches the file system.
// A directory is not a document, so a browser route that collides with one still
// receives the application.
func (s *Server) openAsset(name string) (fs.File, string, error) {
	if name != "" && fs.ValidPath(name) && !strings.HasSuffix(name, "/") {
		if file, err := s.assets.Open(name); err == nil {
			if info, err := file.Stat(); err == nil && !info.IsDir() {
				return file, name, nil
			}
			file.Close()
		}
	}
	file, err := s.assets.Open("index.html")
	return file, "index.html", err
}
