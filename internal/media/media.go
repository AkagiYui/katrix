// Package media implements the Matrix content repository: upload, download,
// thumbnailing (pure Go). Media bytes are stored on the local filesystem;
// metadata and thumbnails in Postgres.
package media

import (
	"bytes"
	"context"
	"fmt"
	"image"
	"image/jpeg"
	"image/png"
	"io"
	"net/http"
	"strconv"
	"strings"

	"github.com/AkagiYui/katrix/internal/homeserver"
	"github.com/AkagiYui/katrix/internal/httpx"
	"github.com/AkagiYui/katrix/internal/storage"
	"golang.org/x/image/draw"
	_ "golang.org/x/image/webp"
	_ "image/gif" // decode gif
)

// API bundles the content-repository handlers.
type API struct {
	*homeserver.HS
	backend *FileBackend
}

// New constructs the media API surface with a filesystem backend.
func New(hs *homeserver.HS) *API {
	backend, err := NewFileBackend(hs.Store, hs.Config.Media.StorePath)
	if err != nil {
		panic("media: " + err.Error())
	}
	return &API{HS: hs, backend: backend}
}

// Register wires media routes.
func (a *API) Register(mux *http.ServeMux) {
	mux.HandleFunc("GET /_matrix/client/v1/media/config", a.RequireAuth(a.Config_))
	mux.HandleFunc("GET /_matrix/media/v3/config", a.RequireAuth(a.Config_))
	mux.HandleFunc("POST /_matrix/media/v3/upload", a.RequireAuth(a.Upload))
	mux.HandleFunc("POST /_matrix/client/v1/media/upload", a.RequireAuth(a.Upload))
	mux.HandleFunc("GET /_matrix/client/v1/media/download/{serverName}/{mediaID}", a.RequireAuth(a.Download))
	mux.HandleFunc("GET /_matrix/media/v3/download/{serverName}/{mediaID}", a.RequireAuth(a.Download))
	mux.HandleFunc("GET /_matrix/client/v1/media/download/{serverName}/{mediaID}/{fileName}", a.RequireAuth(a.Download))
	mux.HandleFunc("GET /_matrix/media/v3/download/{serverName}/{mediaID}/{fileName}", a.RequireAuth(a.Download))
	mux.HandleFunc("GET /_matrix/client/v1/media/thumbnail/{serverName}/{mediaID}", a.RequireAuth(a.Thumbnail))
	mux.HandleFunc("GET /_matrix/media/v3/thumbnail/{serverName}/{mediaID}", a.RequireAuth(a.Thumbnail))
}

// Config_ handles GET /_matrix/.../config.
func (a *API) Config_(w http.ResponseWriter, r *http.Request) {
	httpx.WriteJSON(w, http.StatusOK, map[string]any{
		"m.upload.size": a.HS.Config.Media.MaxUploadBytes,
	})
}

// Upload handles POST /_matrix/media/v3/upload.
func (a *API) Upload(w http.ResponseWriter, r *http.Request) {
	auth, _ := homeserver.AuthFrom(r.Context())
	if cl := r.ContentLength; cl > a.HS.Config.Media.MaxUploadBytes {
		httpx.WriteError(w, httpx.ErrTooLarge(fmt.Sprintf("upload exceeds limit (%d bytes)", a.HS.Config.Media.MaxUploadBytes)))
		return
	}
	contentType := r.Header.Get("Content-Type")
	if contentType == "" {
		contentType = "application/octet-stream"
	}
	uploadName := r.URL.Query().Get("filename")
	mediaID, err := a.backend.Upload(r.Context(), r.Body, contentType, uploadName, auth.UserID, a.Now())
	if err != nil {
		httpx.WriteError(w, httpx.ErrUnknown(err.Error()))
		return
	}
	mxc := "mxc://" + a.ServerName() + "/" + mediaID
	httpx.WriteJSON(w, http.StatusOK, map[string]string{"content_uri": mxc})
}

// Download handles GET /_matrix/.../download/{serverName}/{mediaID}.
func (a *API) Download(w http.ResponseWriter, r *http.Request) {
	mediaID := r.PathValue("mediaID")
	serverName := r.PathValue("serverName")
	if serverName != a.ServerName() {
		httpx.WriteError(w, httpx.NewError(http.StatusNotFound, "M_NOT_FOUND", "remote media not supported"))
		return
	}
	f, meta, err := a.backend.Download(r.Context(), mediaID)
	if err != nil {
		httpx.WriteError(w, httpx.ErrNotFound("media not found"))
		return
	}
	defer f.Close()
	w.Header().Set("Content-Type", meta.ContentType)
	w.Header().Set("Content-Length", strconv.FormatInt(meta.Size, 10))
	w.Header().Set("Content-Disposition", fmt.Sprintf("inline; filename=%q", meta.UploadName))
	w.Header().Set("Cache-Control", "public,max-age=86400")
	_, _ = io.Copy(w, f)
}

// Thumbnail handles GET /_matrix/.../thumbnail/{serverName}/{mediaID}.
func (a *API) Thumbnail(w http.ResponseWriter, r *http.Request) {
	mediaID := r.PathValue("mediaID")
	serverName := r.PathValue("serverName")
	if serverName != a.ServerName() {
		httpx.WriteError(w, httpx.NewError(http.StatusNotFound, "M_NOT_FOUND", "remote media not supported"))
		return
	}
	q := r.URL.Query()
	width, _ := strconv.Atoi(q.Get("width"))
	height, _ := strconv.Atoi(q.Get("height"))
	method := q.Get("method")
	if method == "" {
		method = "scale"
	}
	if width <= 0 || height <= 0 {
		httpx.WriteError(w, httpx.ErrMissingParam("width and height required"))
		return
	}
	// Try the cache.
	if t, err := a.backend.GetThumbnail(r.Context(), mediaID, width, height, method); err == nil {
		w.Header().Set("Content-Type", t.ContentType)
		w.Header().Set("Content-Length", strconv.FormatInt(t.Size, 10))
		w.Header().Set("Cache-Control", "public,max-age=86400")
		_, _ = w.Write(t.Data)
		return
	}
	out, err := a.generateThumbnail(r.Context(), mediaID, width, height, method)
	if err != nil {
		httpx.WriteError(w, httpx.NewError(http.StatusBadRequest, "M_UNKNOWN", "cannot generate thumbnail: "+err.Error()))
		return
	}
	_ = a.backend.SaveThumbnail(r.Context(), storage.ThumbnailRow{
		MediaID: mediaID, Width: width, Height: height, Method: method,
		ContentType: out.contentType, Size: int64(len(out.data)), Data: out.data,
	})
	w.Header().Set("Content-Type", out.contentType)
	w.Header().Set("Content-Length", strconv.Itoa(len(out.data)))
	w.Header().Set("Cache-Control", "public,max-age=86400")
	_, _ = w.Write(out.data)
}

type thumbResult struct {
	data        []byte
	contentType string
}

// generateThumbnail decodes the source image, resizes it, and re-encodes as
// jpeg (default) or png (for sources with transparency). Supported source
// formats: jpeg, png, gif, webp.
func (a *API) generateThumbnail(ctx context.Context, mediaID string, width, height int, method string) (thumbResult, error) {
	f, meta, err := a.backend.Download(ctx, mediaID)
	if err != nil {
		return thumbResult{}, err
	}
	defer f.Close()
	src, _, err := image.Decode(f)
	if err != nil {
		return thumbResult{}, err
	}
	outType := "image/jpeg"
	switch {
	case strings.HasPrefix(meta.ContentType, "image/png"), strings.HasPrefix(meta.ContentType, "image/gif"):
		outType = "image/png"
	}
	b := src.Bounds()
	srcW, srcH := b.Dx(), b.Dy()
	tw, th := width, height
	if method != "crop" {
		scale := minF(float64(width)/float64(srcW), float64(height)/float64(srcH))
		tw = maxInt(1, int(float64(srcW)*scale))
		th = maxInt(1, int(float64(srcH)*scale))
	}
	dst := image.NewRGBA(image.Rect(0, 0, tw, th))
	if method == "crop" {
		draw.CatmullRom.Scale(dst, dst.Rect, src, b, draw.Over, nil)
	} else {
		draw.BiLinear.Scale(dst, dst.Rect, src, b, draw.Over, nil)
	}
	var buf bytes.Buffer
	switch outType {
	case "image/png":
		if err := png.Encode(&buf, dst); err != nil {
			return thumbResult{}, err
		}
	default:
		if err := jpeg.Encode(&buf, dst, &jpeg.Options{Quality: 85}); err != nil {
			return thumbResult{}, err
		}
	}
	return thumbResult{data: buf.Bytes(), contentType: outType}, nil
}

func minF(a, b float64) float64 {
	if a < b {
		return a
	}
	return b
}

func maxInt(a, b int) int {
	if a > b {
		return a
	}
	return b
}
