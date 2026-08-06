// Package httpserver assembles the HTTP routing for the whole homeserver:
// Client-Server API, Server-Server API, media, admin and the embedded SPA. API
// prefixes take priority; unmatched paths fall back to the web panel's
// index.html for client-side routing.
package httpserver

import (
	"io"
	"io/fs"
	"net/http"
	"strings"

	"github.com/AkagiYui/katrix/internal/csapi"
	"github.com/AkagiYui/katrix/internal/federation"
	"github.com/AkagiYui/katrix/internal/homeserver"
	"github.com/AkagiYui/katrix/internal/httpx"
	"github.com/AkagiYui/katrix/internal/media"
	"github.com/AkagiYui/katrix/internal/metrics"
	"github.com/AkagiYui/katrix/internal/webui"
)

// Server is the assembled HTTP handler with access to the constructed CS and
// federation APIs (for background workers started by cmd/katrix, e.g. the
// delayed-event worker and the outbound EDU delivery worker).
type Server struct {
	http.Handler
	cs  *csapi.API
	fed *federation.API
}

// CSAPI returns the constructed CS API.
func (s *Server) CSAPI() *csapi.API { return s.cs }

// Federation returns the constructed federation API.
func (s *Server) Federation() *federation.API { return s.fed }

// New builds the top-level HTTP handler.
func New(hs *homeserver.HS) (*Server, error) {
	mux := http.NewServeMux()

	cs := csapi.New(hs)
	fed := federation.New(hs)
	// The CS API needs the outbound federation client for remote room joins and
	// alias resolution; the federation API is constructed first and handed over.
	cs.SetFederation(fed)
	cs.Register(mux)
	fed.Register(mux)

	med := media.New(hs, fed.Client())
	// URL preview stores og:image blobs through the content repository.
	cs.SetMediaBackend(med.Backend())
	med.Register(mux)

	// Metrics endpoint (Prometheus text format).
	if hs.Config.Metrics.Enabled {
		mux.HandleFunc("GET /metrics", metrics.Handler())
	}

	// Health endpoint (also used by the healthcheck subcommand).
	mux.HandleFunc("GET /health", func(w http.ResponseWriter, r *http.Request) {
		if err := hs.Store.Ping(r.Context()); err != nil {
			httpx.WriteJSON(w, http.StatusServiceUnavailable, map[string]string{"status": "error"})
			return
		}
		httpx.WriteJSON(w, http.StatusOK, map[string]string{"status": "ok"})
	})

	// SPA fallback for non-API paths. Registered on a separate mux so it never
	// shadows the API mux's method-mismatch 405s: Go's ServeMux returns 405
	// for a known path with an unsupported method only when no catch-all "/"
	// pattern is registered on the same mux. (The API mux has none.)
	spa, err := spaHandler()
	if err != nil {
		return nil, err
	}
	spaMux := http.NewServeMux()
	spaMux.Handle("/", spa)

	// Route by prefix: API paths go to the API mux (with the Matrix error-body
	// interceptor), everything else to the SPA.
	apiMux := mux
	root := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		for _, p := range apiPrefixes {
			if strings.HasPrefix(r.URL.Path, p) {
				// Federation requests carry their origin in the X-Matrix
				// Authorization header. A malformed origin (e.g. a server name
				// with a non-numeric port) is refused with 400 M_INVALID_PARAM
				// before any handler runs — mirror of Synapse, whose
				// BaseFederationServlet parses the origin server name and
				// rejects an invalid one (spec §Server names; sytest's
				// "Non-numeric ports in server names are rejected").
				if strings.HasPrefix(r.URL.Path, "/_matrix/federation") {
					if origin := fedOriginFrom(r); origin != "" && !validServerName(origin) {
						httpx.WriteError(w, httpx.NewError(http.StatusBadRequest, "M_INVALID_PARAM",
							"invalid server name in Authorization header"))
						return
					}
				}
				mw := &matrixErrorWriter{ResponseWriter: w}
				apiMux.ServeHTTP(mw, r)
				return
			}
		}
		spaMux.ServeHTTP(w, r)
	})

	return &Server{Handler: withMiddleware(root), cs: cs, fed: fed}, nil
}

// apiPrefixes route to the API mux; the SPA never sees them.
var apiPrefixes = []string{"/_matrix", "/_synapse", "/.well-known", "/health", "/metrics"}

// spaHandler serves embedded static assets, falling back to index.html for
// non-asset paths so client-side routing works. It is only reached for
// non-API paths (the router sends API prefixes to the API mux).
func spaHandler() (http.Handler, error) {
	sub, err := webui.FS()
	if err != nil {
		return nil, err
	}
	fileServer := http.FileServer(http.FS(sub))
	index, err := fs.ReadFile(sub, "index.html")
	if err != nil {
		return nil, err
	}

	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		// If the requested asset exists, serve it; else serve index.html.
		p := strings.TrimPrefix(r.URL.Path, "/")
		if p == "" {
			serveIndex(w, index)
			return
		}
		if f, err := sub.Open(p); err == nil {
			_ = f.Close()
			fileServer.ServeHTTP(w, r)
			return
		}
		serveIndex(w, index)
	}), nil
}

func serveIndex(w http.ResponseWriter, index []byte) {
	w.Header().Set("Content-Type", "text/html; charset=utf-8")
	w.WriteHeader(http.StatusOK)
	_, _ = io.Copy(w, strings.NewReader(string(index)))
}

// fedOriginFrom extracts the requesting server's name from the X-Matrix
// Authorization header ("origin=<name>,key=<id>,destination=<name>,sig=<sig>").
// An absent header yields "".
func fedOriginFrom(r *http.Request) string {
	h := r.Header.Get("Authorization")
	if i := strings.Index(h, "origin="); i >= 0 {
		rest := h[i+len("origin="):]
		if j := strings.IndexByte(rest, ','); j >= 0 {
			v := rest[:j]
			return strings.Trim(v, `"`)
		}
	}
	return ""
}

// validServerName reports whether a Matrix server name is well-formed (spec
// §Server names): a non-empty host, optionally followed by a port which must
// be numeric. A name like "localhost:http" — a port that is not digits — is
// invalid and such requests are rejected before being processed.
func validServerName(name string) bool {
	if name == "" {
		return false
	}
	host, port, hasPort := strings.Cut(name, ":")
	if host == "" {
		return false
	}
	if !hasPort {
		return true
	}
	if port == "" {
		return false
	}
	for _, c := range port {
		if c < '0' || c > '9' {
			return false
		}
	}
	return true
}

// writeMatrixNotFound responds with the Matrix error format for an unknown
// /_matrix/... path (M_UNRECOGNIZED, 404).
func writeMatrixNotFound(w http.ResponseWriter) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusNotFound)
	_, _ = w.Write([]byte(`{"errcode":"M_UNRECOGNIZED","error":"Unrecognized request"}`))
}

// withMiddleware applies the r0->v3 path rewrite, CORS and server-header
// middleware globally. The Matrix error-body rewriting for /_matrix paths is
// done by the root router (matrixErrorWriter), not here.
func withMiddleware(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Server", "Katrix/"+homeserver.Version)
		// Every response is readable cross-origin. The client API is consumed by
		// browser-based clients (matrix-js-sdk fetches account data, filters and
		// key backups before the first sync, and media is rendered via <img>),
		// and the spec's client-server API is served with CORS enabled. Set it
		// here so no handler can forget it: a response without
		// Access-Control-Allow-Origin makes Chrome reject the fetch with
		// "Failed to fetch" even when the server returned 200 (complement-crypto's
		// TestCanBackupKeys hit exactly this on the js restorer path).
		w.Header().Set("Access-Control-Allow-Origin", "*")
		if r.Method == http.MethodOptions {
			w.Header().Set("Access-Control-Allow-Methods", "GET, POST, PUT, DELETE, OPTIONS")
			w.Header().Set("Access-Control-Allow-Headers", "Origin, X-Requested-With, Content-Type, Accept, Authorization")
			w.WriteHeader(http.StatusOK)
			return
		}
		// Transparently map the legacy r0 API path prefix to v3 so both paths
		// reach the same handlers. Only /_matrix/client/r0/ is rewritten.
		if strings.HasPrefix(r.URL.Path, "/_matrix/client/r0/") {
			r2 := r.Clone(r.Context())
			r2.URL.Path = "/_matrix/client/v3/" + r.URL.Path[len("/_matrix/client/r0/"):]
			r2.URL.RawPath = ""
			next.ServeHTTP(w, r2)
			return
		}
		next.ServeHTTP(w, r)
	})
}

// matrixErrorWriter replaces the router's plain-text 404/405 error bodies for
// unknown /_matrix paths with the Matrix JSON error format. Go's ServeMux
// always writes the status before the body for these errors, so only the
// tiny error response is held; every other status (and every handler-written
// response, including large /sync payloads) passes straight through without
// buffering.
type matrixErrorWriter struct {
	http.ResponseWriter
	status         int // the pending 404/405 status, when pendingRewrite
	pendingRewrite bool
}

func (m *matrixErrorWriter) WriteHeader(status int) {
	switch status {
	case http.StatusNotFound, http.StatusMethodNotAllowed:
		// Only hold when the router produced its plain-text error. A handler's
		// real Matrix error (httpx.WriteError sets application/json before
		// WriteHeader) must pass through untouched.
		if strings.HasPrefix(m.Header().Get("Content-Type"), "application/json") {
			m.ResponseWriter.WriteHeader(status)
			return
		}
		m.status = status
		m.pendingRewrite = true
		return
	default:
		m.ResponseWriter.WriteHeader(status)
	}
}

func (m *matrixErrorWriter) Write(p []byte) (int, error) {
	if m.pendingRewrite {
		m.pendingRewrite = false
		status := m.status
		if status == 0 {
			status = http.StatusNotFound
		}
		m.Header().Set("Content-Type", "application/json")
		m.ResponseWriter.WriteHeader(status)
		body := []byte(`{"errcode":"M_UNRECOGNIZED","error":"Unrecognized request"}`)
		_, _ = m.ResponseWriter.Write(body)
		return len(p), nil
	}
	return m.ResponseWriter.Write(p)
}
