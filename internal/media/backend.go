package media

import (
	"context"
	"crypto/rand"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"io"
	"os"
	"path/filepath"

	"github.com/AkagiYui/katrix/internal/storage"
)

// FileBackend persists media blobs to the local filesystem under a configured
// root directory. Metadata goes to Postgres; blob bytes go to disk.
type FileBackend struct {
	store *storage.Store
	root  string
}

// NewFileBackend constructs a filesystem media backend.
func NewFileBackend(store *storage.Store, root string) (*FileBackend, error) {
	if err := os.MkdirAll(root, 0o755); err != nil {
		return nil, fmt.Errorf("media: create store root %q: %w", root, err)
	}
	return &FileBackend{store: store, root: root}, nil
}

// Upload reads the blob, stores it, records metadata, and returns the media id.
func (b *FileBackend) Upload(ctx context.Context, r io.Reader, contentType, uploadName, userID string, now int64) (string, error) {
	mediaID := randomMediaID()
	if err := b.UploadTo(ctx, mediaID, r, contentType, uploadName, userID, now); err != nil {
		return "", err
	}
	return mediaID, nil
}

// UploadTo stores a blob under a specific media ID (used by the async upload
// path, where the ID is reserved first via POST /media/v1/create). It is the
// same write path as Upload.
func (b *FileBackend) UploadTo(ctx context.Context, mediaID string, r io.Reader, contentType, uploadName, userID string, now int64) error {
	path := b.blobPath(mediaID)
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		return err
	}
	f, err := os.Create(path)
	if err != nil {
		return err
	}
	defer f.Close()
	h := sha256.New()
	mw := io.MultiWriter(f, h)
	size, err := io.Copy(mw, io.LimitReader(r, maxFileRead))
	if err != nil {
		_ = os.Remove(path)
		return err
	}
	if err := b.store.CreateMedia(ctx, storage.MediaRow{
		MediaID: mediaID,
		// Local media: origin server is left empty so GetMedia (by mediaID) works.
		ContentType: contentType, UploadName: uploadName, UserID: userID,
		Size: size, SHA256: hex.EncodeToString(h.Sum(nil)), CreatedTS: now,
	}); err != nil {
		_ = os.Remove(path)
		return err
	}
	return nil
}

// UploadRemote stores a fetched remote media blob with a known media id and
// originating server name. Subsequent lookups use GetRemoteMedia(server, id).
// It is idempotent: re-fetching the same remote media overwrites the cache.
func (b *FileBackend) UploadRemote(ctx context.Context, r io.Reader, contentType, uploadName, originServer, mediaID string, now int64) (string, error) {
	path := b.blobPath(mediaID)
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		return "", err
	}
	f, err := os.Create(path)
	if err != nil {
		return "", err
	}
	defer f.Close()
	h := sha256.New()
	mw := io.MultiWriter(f, h)
	size, err := io.Copy(mw, io.LimitReader(r, maxFileRead))
	if err != nil {
		_ = os.Remove(path)
		return "", err
	}
	if err := b.store.CreateMedia(ctx, storage.MediaRow{
		MediaID: mediaID, OriginServer: originServer,
		ContentType: contentType, UploadName: uploadName,
		Size: size, SHA256: hex.EncodeToString(h.Sum(nil)),
		CreatedTS: now, CachedTS: now,
	}); err != nil {
		_ = os.Remove(path)
		return "", err
	}
	return mediaID, nil
}

// Download opens the blob for reading and returns metadata. It tries the
// local-media lookup first, then the remote-media lookup by id (the origin is
// irrelevant for a cache hit).
func (b *FileBackend) Download(ctx context.Context, mediaID string) (*os.File, *storage.MediaRow, error) {
	meta, err := b.store.GetMedia(ctx, mediaID)
	if err != nil {
		return nil, nil, err
	}
	f, err := os.Open(b.blobPath(mediaID))
	if err != nil {
		return nil, nil, err
	}
	return f, meta, nil
}

// SaveThumbnail stores a thumbnail blob in Postgres (small images).
func (b *FileBackend) SaveThumbnail(ctx context.Context, t storage.ThumbnailRow) error {
	return b.store.CreateThumbnail(ctx, t)
}

// GetThumbnail fetches a thumbnail blob from Postgres.
func (b *FileBackend) GetThumbnail(ctx context.Context, mediaID string, w, h int, method string) (*storage.ThumbnailRow, error) {
	return b.store.GetThumbnail(ctx, mediaID, w, h, method)
}

// blobPath shards media ids into a 2-level directory tree to avoid huge dirs.
func (b *FileBackend) blobPath(mediaID string) string {
	if len(mediaID) < 2 {
		mediaID = "00" + mediaID
	}
	return filepath.Join(b.root, mediaID[:2], mediaID[2:4], mediaID)
}

// maxFileRead caps how many bytes an upload will copy; the spec upload limit
// is enforced separately by the handler via Content-Length and a hard cap.
const maxFileRead = 100 << 20 // 100 MB safety net beyond configured limit

// randomMediaID generates an opaque media id (uppercase alnum).
func randomMediaID() string {
	const alphabet = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789"
	b := make([]byte, 24)
	if _, err := rand.Read(b); err != nil {
		panic("media: rand: " + err.Error())
	}
	for i := range b {
		b[i] = alphabet[int(b[i])%len(alphabet)]
	}
	return string(b)
}
