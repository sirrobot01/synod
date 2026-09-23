package api

import (
	"encoding/json"
	"net/http"
	"net/url"
	"strconv"
	"strings"

	"github.com/google/uuid"
	"github.com/sirrobot01/synod/internal/cluster"
)

func (s *Server) listClusters(w http.ResponseWriter, r *http.Request) {
	query, err := url.ParseQuery(r.URL.RawQuery)
	if err != nil || strings.HasPrefix(r.URL.RawQuery, "&") || strings.HasSuffix(r.URL.RawQuery, "&") || strings.Contains(r.URL.RawQuery, "&&") {
		s.writeError(w, r, http.StatusBadRequest, "invalid_request", "Use valid cluster list parameters.", uuid.Nil)
		return
	}
	options := cluster.ListOptions{Limit: 50}
	for name, values := range query {
		if len(values) != 1 || values[0] == "" {
			s.writeError(w, r, http.StatusBadRequest, "invalid_request", "Use one nonempty value for each list parameter.", uuid.Nil)
			return
		}
		value := values[0]
		switch name {
		case "limit":
			limit, err := strconv.Atoi(value)
			if err != nil || strings.IndexFunc(value, func(r rune) bool { return r < '0' || r > '9' }) >= 0 || limit < 1 || limit > 200 {
				s.writeError(w, r, http.StatusBadRequest, "invalid_request", "Use a decimal limit from 1 through 200.", uuid.Nil)
				return
			}
			options.Limit = limit
		case "include_deleted":
			if value != "true" && value != "false" {
				s.writeError(w, r, http.StatusBadRequest, "invalid_request", "Use true or false for include_deleted.", uuid.Nil)
				return
			}
			options.IncludeDeleted = value == "true"
		case "cursor":
			if len(value) > 1024 {
				s.writeError(w, r, http.StatusBadRequest, "invalid_request", "The list cursor is too long.", uuid.Nil)
				return
			}
			options.Cursor = value
		default:
			s.writeError(w, r, http.StatusBadRequest, "invalid_request", "The list parameter is not supported.", uuid.Nil)
			return
		}
	}
	page, err := s.store.ListClusters(r.Context(), s.installationID, options)
	if err != nil {
		s.storeError(w, r, err)
		return
	}
	if r.Method == http.MethodHead {
		w.WriteHeader(http.StatusOK)
		return
	}
	if err := json.NewEncoder(w).Encode(page); err != nil {
		s.logger.Warn("write cluster list response failed", "request_id", r.Context().Value(identityKey{}).(requestIdentity).requestID, "error", err)
	}
}
