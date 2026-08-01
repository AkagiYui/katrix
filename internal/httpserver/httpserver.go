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
		if r.Method == http.MethodOptions {
			w.Header().Set("Access-Control-Allow-Origin", "*")
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
