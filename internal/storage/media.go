package storage

import (
	"context"
	"errors"

	"github.com/jackc/pgx/v5"
)

// MediaRow is a media metadata row.
type MediaRow struct {
	MediaID      string
	OriginServer string // "" for local; the remote server name for federated media
	ContentType  string
	UploadName   string
	UserID       string
	Size         int64
	SHA256       string
	Blurhash     string
	CreatedTS    int64
	CachedTS     int64 // when remote media was locally cached (0 for local)
}

// CreateMedia inserts media metadata. For local media OriginServer should be
// the local server name; for remote-cached media it is the originating server.
func (s *Store) CreateMedia(ctx context.Context, m MediaRow) error {
	_, err := s.pool.Exec(ctx,
		`INSERT INTO media(media_id, origin_server, content_type, upload_name, user_id, size, sha256, blurhash, created_ts, cached_ts)
		 VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10)
		 ON CONFLICT (origin_server, media_id) DO UPDATE SET cached_ts=EXCLUDED.cached_ts`,
		m.MediaID, m.OriginServer, m.ContentType, nullString(m.UploadName), m.UserID, m.Size, m.SHA256, nullString(m.Blurhash), m.CreatedTS, m.CachedTS)
	return err
}

// GetMedia fetches media metadata by id. Use this for local media (origin is
// implied). For remote media use GetRemoteMedia.
func (s *Store) GetMedia(ctx context.Context, mediaID string) (*MediaRow, error) {
	var m MediaRow
	var uploadName, blur *string
	err := s.pool.QueryRow(ctx,
		`SELECT media_id, COALESCE(origin_server,''), content_type, upload_name, user_id, size, sha256, blurhash, created_ts, COALESCE(cached_ts,0)
		 FROM media WHERE media_id=$1`, mediaID,
	).Scan(&m.MediaID, &m.OriginServer, &m.ContentType, &uploadName, &m.UserID, &m.Size, &m.SHA256, &blur, &m.CreatedTS, &m.CachedTS)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return nil, ErrNotFound
		}
		return nil, err
	}
	if uploadName != nil {
		m.UploadName = *uploadName
	}
	if blur != nil {
		m.Blurhash = *blur
	}
	return &m, nil
}

// GetRemoteMedia fetches media metadata by (origin server, media id).
func (s *Store) GetRemoteMedia(ctx context.Context, originServer, mediaID string) (*MediaRow, error) {
	var m MediaRow
	var uploadName, blur *string
	err := s.pool.QueryRow(ctx,
		`SELECT media_id, COALESCE(origin_server,''), content_type, upload_name, user_id, size, sha256, blurhash, created_ts, COALESCE(cached_ts,0)
		 FROM media WHERE origin_server=$1 AND media_id=$2`, originServer, mediaID,
	).Scan(&m.MediaID, &m.OriginServer, &m.ContentType, &uploadName, &m.UserID, &m.Size, &m.SHA256, &blur, &m.CreatedTS, &m.CachedTS)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return nil, ErrNotFound
		}
		return nil, err
	}
	if uploadName != nil {
		m.UploadName = *uploadName
	}
	if blur != nil {
		m.Blurhash = *blur
	}
	return &m, nil
}

// ThumbnailRow is a media thumbnail row.
type ThumbnailRow struct {
	MediaID     string
	Width       int
	Height      int
	Method      string
	ContentType string
	Size        int64
	Data        []byte
}

// CreateThumbnail stores a thumbnail.
func (s *Store) CreateThumbnail(ctx context.Context, t ThumbnailRow) error {
	_, err := s.pool.Exec(ctx,
		`INSERT INTO media_thumbnails(media_id, width, height, method, content_type, size, data)
		 VALUES ($1,$2,$3,$4,$5,$6,$7)
		 ON CONFLICT (media_id, width, height, method) DO UPDATE SET data=EXCLUDED.data, size=EXCLUDED.size`,
		t.MediaID, t.Width, t.Height, t.Method, t.ContentType, t.Size, t.Data)
	return err
}

// GetThumbnail fetches a thumbnail.
func (s *Store) GetThumbnail(ctx context.Context, mediaID string, width, height int, method string) (*ThumbnailRow, error) {
	var t ThumbnailRow
	err := s.pool.QueryRow(ctx,
		`SELECT media_id, width, height, method, content_type, size, data
		 FROM media_thumbnails WHERE media_id=$1 AND width=$2 AND height=$3 AND method=$4`,
		mediaID, width, height, method,
	).Scan(&t.MediaID, &t.Width, &t.Height, &t.Method, &t.ContentType, &t.Size, &t.Data)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return nil, ErrNotFound
		}
		return nil, err
	}
	return &t, nil
}
