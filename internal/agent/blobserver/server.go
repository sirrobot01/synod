// Package blobserver serves encrypted Kopia objects from configured agent directories.
package blobserver

import (
	"context"
	"crypto/sha256"
	"crypto/x509"
	"encoding/hex"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"os"
	"path"
	"path/filepath"
	"strconv"
	"strings"
	"syscall"
	"time"

	"github.com/google/uuid"
	"github.com/sirrobot01/synod/internal/destination"
	"github.com/sirrobot01/synod/internal/pki"
	"github.com/sirrobot01/synod/internal/repository"
)

// Server reads the durable plan on each request, including on an existing TLS connection.
type Server struct {
	InstallationID      uuid.UUID
	NodeID              uuid.UUID
	Plan                func(context.Context) (destination.Plan, error)
	slots               chan struct{}
	certificateSHA256   string
	notBefore, notAfter time.Time
}

func New(installationID, nodeID uuid.UUID, certificate *x509.Certificate, plan func(context.Context) (destination.Plan, error)) (*Server, error) {
	if installationID == uuid.Nil || nodeID == uuid.Nil || certificate == nil || plan == nil {
		return nil, destination.ErrInvalid
	}
	hash := sha256.Sum256(certificate.Raw)
	return &Server{InstallationID: installationID, NodeID: nodeID, Plan: plan, certificateSHA256: hex.EncodeToString(hash[:]), notBefore: certificate.NotBefore, notAfter: certificate.NotAfter, slots: make(chan struct{}, 8)}, nil
}

func (s *Server) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Cache-Control", "no-store")
	w.Header().Set("X-Content-Type-Options", "nosniff")
	if r.TLS == nil {
		http.Error(w, "mutual TLS required", http.StatusUnauthorized)
		return
	}
	now := time.Now()
	if now.Before(s.notBefore) || !now.Before(s.notAfter) {
		http.Error(w, "destination certificate expired", http.StatusForbidden)
		return
	}
	identity, err := pki.PeerIdentity(*r.TLS, s.InstallationID, pki.Agent)
	if err != nil || now.Before(r.TLS.PeerCertificates[0].NotBefore) || !now.Before(r.TLS.PeerCertificates[0].NotAfter) {
		http.Error(w, "agent identity refused", http.StatusForbidden)
		return
	}
	select {
	case s.slots <- struct{}{}:
		defer func() { <-s.slots }()
	default:
		http.Error(w, "destination busy", http.StatusTooManyRequests)
		return
	}
	plan, err := s.Plan(r.Context())
	if err != nil || plan.Validate(s.NodeID) != nil {
		http.Error(w, "destination settings unavailable", http.StatusServiceUnavailable)
		return
	}
	hash := sha256.Sum256(r.TLS.PeerCertificates[0].Raw)
	fingerprint := hex.EncodeToString(hash[:])
	allowed, ownerAllowed := false, false
	for _, peer := range plan.Peers {
		if peer.NodeID == s.NodeID && peer.CertificateSHA256 == s.certificateSHA256 {
			ownerAllowed = true
		}
		if peer.NodeID == identity.SubjectID && peer.CertificateSHA256 == fingerprint {
			allowed = true
		}
	}
	if !allowed || !ownerAllowed {
		http.Error(w, "agent is not approved", http.StatusForbidden)
		return
	}
	idText, ok := strings.CutPrefix(r.URL.Path, "/v1/repositories/")
	id, err := uuid.Parse(idText)
	if !ok || err != nil || id.String() != idText {
		http.NotFound(w, r)
		return
	}
	directory := ""
	for _, entry := range plan.Destinations {
		d := entry.Destination
		if d.ID == id && d.NodeID == s.NodeID && d.Location.Kind == "device" {
			directory = d.Location.Path
			break
		}
	}
	if directory == "" {
		http.NotFound(w, r)
		return
	}
	file := r.URL.Query().Get("path")
	listing := r.Method == http.MethodGet && r.URL.Query().Get("list") == "1"
	if file != "" && !validPath(file, listing) {
		http.Error(w, "invalid blob path", http.StatusBadRequest)
		return
	}
	if file == "" && r.Method != http.MethodGet {
		http.Error(w, "invalid blob method", http.StatusMethodNotAllowed)
		return
	}
	root, err := openDirectory(directory)
	if err != nil {
		http.Error(w, "destination directory unavailable", http.StatusServiceUnavailable)
		return
	}
	defer root.Close()
	if file == "" {
		w.WriteHeader(http.StatusNoContent)
		return
	}
	switch r.Method {
	case http.MethodGet, http.MethodHead:
		if listing {
			s.list(w, r, root, file)
			return
		}
		f, err := root.OpenFile(file, os.O_RDONLY|syscall.O_NOFOLLOW|syscall.O_NONBLOCK, 0)
		if err != nil {
			s.fileError(w, r, err)
			return
		}
		defer f.Close()
		info, err := f.Stat()
		if err != nil || !info.Mode().IsRegular() {
			s.fileError(w, r, os.ErrPermission)
			return
		}
		w.Header().Set("X-Synod-Modified", info.ModTime().UTC().Format(time.RFC3339Nano))
		if r.Method == http.MethodHead {
			w.Header().Set("Content-Length", strconv.FormatInt(info.Size(), 10))
			return
		}
		offset, length := int64(0), int64(-1)
		if text := r.URL.Query().Get("offset"); text != "" {
			offset, err = strconv.ParseInt(text, 10, 64)
			if err != nil {
				http.Error(w, "invalid blob range", 416)
				return
			}
		}
		if text := r.URL.Query().Get("length"); text != "" {
			length, err = strconv.ParseInt(text, 10, 64)
			if err != nil {
				http.Error(w, "invalid blob range", 416)
				return
			}
		}
		if length < 0 {
			offset = 0
			length = info.Size()
		}
		if offset < 0 || offset > info.Size() || length > info.Size()-offset {
			http.Error(w, "invalid blob range", 416)
			return
		}
		if _, err := f.Seek(offset, io.SeekStart); err != nil {
			s.fileError(w, r, err)
			return
		}
		w.Header().Set("Content-Type", "application/octet-stream")
		w.Header().Set("Content-Length", strconv.FormatInt(length, 10))
		// Content-Length makes a failed copy observable as a truncated response.
		io.CopyN(w, f, length)
	case http.MethodPut:
		s.put(w, r, root, file)
	case http.MethodDelete:
		err := root.Remove(file)
		if err != nil && !errors.Is(err, os.ErrNotExist) {
			s.fileError(w, r, err)
			return
		}
		parent, err := root.Open(path.Dir(file))
		if err != nil && !errors.Is(err, os.ErrNotExist) {
			s.fileError(w, r, err)
			return
		}
		if err == nil {
			err = errors.Join(parent.Sync(), parent.Close())
			if err != nil {
				s.fileError(w, r, err)
				return
			}
		}
		w.WriteHeader(http.StatusNoContent)
	default:
		w.Header().Set("Allow", "GET, HEAD, PUT, DELETE")
		http.Error(w, "invalid blob method", http.StatusMethodNotAllowed)
	}
}

func validPath(file string, directory bool) bool {
	if file == ".shards" && !directory {
		return true
	}
	if directory && file == "." {
		return true
	}
	if len(file) > 1024 || path.Clean(file) != file || strings.HasPrefix(file, "/") {
		return false
	}
	for part := range strings.SplitSeq(file, "/") {
		if strings.HasPrefix(part, ".") || len(part) == 0 || len(part) > 255 || strings.ContainsFunc(part, func(r rune) bool {
			return !(r >= 'a' && r <= 'z' || r >= 'A' && r <= 'Z' || r >= '0' && r <= '9' || r == '-' || r == '_' || r == '.')
		}) {
			return false
		}
	}
	if directory {
		return !strings.HasPrefix(path.Base(file), ".")
	}
	return file == ".shards" || (!strings.HasPrefix(path.Base(file), ".") && strings.HasSuffix(file, ".f"))
}

// openDirectory refuses a non-repository directory before it creates the service marker.
func openDirectory(directory string) (*os.Root, error) {
	if !filepath.IsAbs(directory) || filepath.Clean(directory) != directory || directory == "/" {
		return nil, os.ErrPermission
	}
	if err := os.MkdirAll(directory, 0700); err != nil {
		return nil, err
	}
	root, err := os.OpenRoot(directory)
	if err != nil {
		return nil, err
	}
	ready := false
	defer func() {
		if !ready {
			root.Close()
		}
	}()
	markerFile, err := root.OpenFile(".synod-repository", os.O_RDONLY|syscall.O_NOFOLLOW|syscall.O_NONBLOCK, 0)
	if err == nil {
		info, statErr := markerFile.Stat()
		if statErr != nil || !info.Mode().IsRegular() || info.Size() > 64 {
			markerFile.Close()
			return nil, os.ErrPermission
		}
		marker, readErr := io.ReadAll(io.LimitReader(markerFile, 65))
		if err := errors.Join(readErr, markerFile.Close()); err != nil {
			return nil, err
		}
		if string(marker) != "synod-device-repository-v1\n" {
			return nil, os.ErrPermission
		}
		ready = true
		return root, nil
	}
	if !errors.Is(err, os.ErrNotExist) {
		return nil, err
	}
	existing, err := root.Open(".")
	if err != nil {
		return nil, err
	}
	entries, readErr := existing.ReadDir(1)
	if err := errors.Join(existing.Close(), readErr); err != nil && !errors.Is(err, io.EOF) {
		return nil, err
	}
	if len(entries) > 0 {
		for _, name := range []string{".shards", "kopia.repository.f"} {
			info, err := root.Lstat(name)
			if err != nil || !info.Mode().IsRegular() {
				return nil, os.ErrPermission
			}
		}
	}
	f, err := root.OpenFile(".synod-repository", os.O_WRONLY|os.O_CREATE|os.O_EXCL|syscall.O_NOFOLLOW, 0600)
	if err != nil {
		if errors.Is(err, os.ErrExist) {
			return nil, errors.New("repository directory setup is in progress")
		}
		return nil, err
	}
	_, writeErr := f.WriteString("synod-device-repository-v1\n")
	if err := errors.Join(writeErr, f.Sync(), f.Close()); err != nil {
		return nil, err
	}
	parent, err := root.Open(".")
	if err != nil {
		return nil, err
	}
	if err := errors.Join(parent.Sync(), parent.Close()); err != nil {
		return nil, err
	}
	ready = true
	return root, nil
}

func (s *Server) list(w http.ResponseWriter, r *http.Request, root *os.Root, file string) {
	f, err := root.Open(file)
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			w.Header().Set("Content-Type", "application/json")
			io.WriteString(w, "[]\n")
			return
		}
		s.fileError(w, r, err)
		return
	}
	defer f.Close()
	entries, err := f.ReadDir(20001)
	if err != nil && !errors.Is(err, io.EOF) {
		s.fileError(w, r, err)
		return
	}
	if len(entries) > 20000 {
		http.Error(w, "directory exceeds its listing limit", http.StatusInsufficientStorage)
		return
	}
	result := make([]repository.DeviceFile, 0, len(entries))
	for _, entry := range entries {
		if entry.Type()&os.ModeSymlink != 0 {
			continue
		}
		if !validPath(entry.Name(), entry.IsDir()) {
			continue
		}
		info, err := entry.Info()
		if err != nil {
			s.fileError(w, r, err)
			return
		}
		if !info.IsDir() && !info.Mode().IsRegular() {
			continue
		}
		result = append(result, repository.DeviceFile{Filename: entry.Name(), SizeBytes: info.Size(), ModifiedAt: info.ModTime().UTC(), Directory: info.IsDir()})
	}
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(result)
}

func (s *Server) put(w http.ResponseWriter, r *http.Request, root *os.Root, file string) {
	if r.ContentLength < 0 || r.ContentLength > 64<<20 {
		http.Error(w, "blob needs a length of at most 64 MiB", http.StatusRequestEntityTooLarge)
		return
	}
	modified := time.Time{}
	if text := r.URL.Query().Get("modified"); text != "" {
		var err error
		modified, err = time.Parse(time.RFC3339Nano, text)
		if err != nil {
			http.Error(w, "invalid blob time", http.StatusBadRequest)
			return
		}
	}
	directory := path.Dir(file)
	if err := root.MkdirAll(directory, 0700); err != nil {
		s.fileError(w, r, err)
		return
	}
	temporary := path.Join(directory, ".synod-"+uuid.NewString()+".tmp")
	f, err := root.OpenFile(temporary, os.O_CREATE|os.O_EXCL|os.O_WRONLY|syscall.O_NOFOLLOW, 0600)
	if err != nil {
		s.fileError(w, r, err)
		return
	}
	defer root.Remove(temporary)
	written, writeErr := io.Copy(f, http.MaxBytesReader(w, r.Body, 64<<20))
	if writeErr != nil || written != r.ContentLength || r.Context().Err() != nil {
		f.Close()
		http.Error(w, "blob upload incomplete", http.StatusBadRequest)
		return
	}
	if !modified.IsZero() {
		timestamp := syscall.NsecToTimeval(int64(modified.Nanosecond()))
		timestamp.Sec = modified.Unix()
		if err := syscall.Futimes(int(f.Fd()), []syscall.Timeval{timestamp, timestamp}); err != nil {
			f.Close()
			s.fileError(w, r, err)
			return
		}
	}
	if err := errors.Join(f.Sync(), f.Close()); err != nil {
		s.fileError(w, r, err)
		return
	}
	if err := root.Rename(temporary, file); err != nil {
		s.fileError(w, r, err)
		return
	}
	parent, err := root.Open(directory)
	if err != nil {
		s.fileError(w, r, err)
		return
	}
	if err := errors.Join(parent.Sync(), parent.Close()); err != nil {
		s.fileError(w, r, err)
		return
	}
	info, err := root.Stat(file)
	if err != nil {
		s.fileError(w, r, err)
		return
	}
	w.Header().Set("X-Synod-Modified", info.ModTime().UTC().Format(time.RFC3339Nano))
	w.WriteHeader(http.StatusNoContent)
}

func (s *Server) fileError(w http.ResponseWriter, r *http.Request, err error) {
	status := http.StatusServiceUnavailable
	if errors.Is(err, os.ErrNotExist) {
		status = http.StatusNotFound
	}
	if errors.Is(err, os.ErrPermission) {
		status = http.StatusForbidden
	}
	http.Error(w, "destination file request failed", status)
}
