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

// Server is the assembled HTTP handler with access to the constructed CS API
// (for background workers started by cmd/katrix, e.g. the delayed-event worker).
type Server struct {
	http.Handler
	cs *csapi.API
}

// CSAPI returns the constructed CS API.
func (s *Server) CSAPI() *csapi.API { return s.cs }

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

	// SPA fallback for everything else.
	spa, err := spaHandler()
	if err != nil {
		return nil, err
	}
	mux.Handle("/", spa)

	return &Server{Handler: withMiddleware(mux), cs: cs}, nil
}

// apiPrefixes are never served by the SPA fallback.
var apiPrefixes = []string{"/_matrix", "/_synapse", "/.well-known", "/health"}

// spaHandler serves embedded static assets, falling back to index.html for
// non-asset, non-API paths so client-side routing works.
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
		for _, p := range apiPrefixes {
			if strings.HasPrefix(r.URL.Path, p) {
				http.NotFound(w, r)
				return
			}
		}
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

// withMiddleware applies the r0->v3 path rewrite, CORS and server-header
// middleware globally.
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
