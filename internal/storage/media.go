package storage

import (
	"bytes"
	"context"
	"errors"
	"regexp"
	"strings"

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
	// QuarantinedTS is non-zero when an admin quarantined the media (admin API
	// POST /_synapse/admin/v1/room/{roomId}/media/quarantine); quarantined media
	// is withheld from every download/thumbnail until unquarantined.
	QuarantinedTS int64
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
		`SELECT media_id, COALESCE(origin_server,''), content_type, upload_name, user_id, size, sha256, blurhash, created_ts, COALESCE(cached_ts,0), COALESCE(quarantined_ts,0)
		 FROM media WHERE media_id=$1`, mediaID,
	).Scan(&m.MediaID, &m.OriginServer, &m.ContentType, &uploadName, &m.UserID, &m.Size, &m.SHA256, &blur, &m.CreatedTS, &m.CachedTS, &m.QuarantinedTS)
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
		`SELECT media_id, COALESCE(origin_server,''), content_type, upload_name, user_id, size, sha256, blurhash, created_ts, COALESCE(cached_ts,0), COALESCE(quarantined_ts,0)
		 FROM media WHERE origin_server=$1 AND media_id=$2`, originServer, mediaID,
	).Scan(&m.MediaID, &m.OriginServer, &m.ContentType, &uploadName, &m.UserID, &m.Size, &m.SHA256, &blur, &m.CreatedTS, &m.CachedTS, &m.QuarantinedTS)
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

// QuarantineMediaInRoom marks every media whose mxc:// URL appears in any of
// the room's events as quarantined (admin API "quarantine media in a room":
// media that has been seen in the room becomes unavailable to all clients and
// remote servers). The scan walks the room's stored events for mxc://
// references, so a media ID is quarantined once regardless of whether it was
// uploaded locally or cached from a remote server. Returns the number of media
// rows newly quarantined.
func (s *Store) QuarantineMediaInRoom(ctx context.Context, roomID string, now int64) (int64, error) {
	rows, err := s.pool.Query(ctx,
		`SELECT json FROM events WHERE room_id=$1 ORDER BY stream_ordering DESC LIMIT 1000`, roomID)
	if err != nil {
		return 0, err
	}
	defer rows.Close()
	type originID struct{ origin, mediaID string }
	found := map[originID]bool{}
	for rows.Next() {
		var raw []byte
		if err := rows.Scan(&raw); err != nil {
			return 0, err
		}
		for _, m := range mxcURLRegex.FindAll(raw, -1) {
			// mxc://<server>/<mediaID>
			rest := m[len("mxc://"):]
			slash := bytes.IndexByte(rest, '/')
			if slash <= 0 || slash == len(rest)-1 {
				continue
			}
			origin := string(rest[:slash])
			mediaID := string(rest[slash+1:])
			// Trim any trailing punctuation the regex may have captured.
			mediaID = strings.TrimRight(mediaID, `"',.})]`)
			if mediaID == "" {
				continue
			}
			found[originID{origin, mediaID}] = true
		}
	}
	if err := rows.Err(); err != nil {
		return 0, err
	}
	var quarantined int64
	for f := range found {
		tag, err := s.pool.Exec(ctx,
			`UPDATE media SET quarantined_ts=$1
			 WHERE origin_server=$2 AND media_id=$3 AND quarantined_ts=0`,
			now, f.origin, f.mediaID)
		if err != nil {
			continue
		}
		quarantined += tag.RowsAffected()
	}
	return quarantined, nil
}

// QuarantineMedia marks a single media row quarantined (admin API
// "quarantine media by id"). Returns ErrNotFound when no row matches.
func (s *Store) QuarantineMedia(ctx context.Context, originServer, mediaID string, now int64) error {
	tag, err := s.pool.Exec(ctx,
		`UPDATE media SET quarantined_ts=$1 WHERE origin_server=$2 AND media_id=$3`,
		now, originServer, mediaID)
	if err != nil {
		return err
	}
	if tag.RowsAffected() == 0 {
		return ErrNotFound
	}
	return nil
}

// UnquarantineMedia lifts the quarantine flag from a media row (admin API
// "unquarantine media").
func (s *Store) UnquarantineMedia(ctx context.Context, originServer, mediaID string) error {
	_, err := s.pool.Exec(ctx,
		`UPDATE media SET quarantined_ts=0 WHERE origin_server=$1 AND media_id=$2`,
		originServer, mediaID)
	return err
}

var mxcURLRegex = regexp.MustCompile(`mxc://[^"\s]+`)

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

// ---- Async media upload (MSC2246) ----

// CreatePendingMedia reserves a media ID for async upload. The blob is uploaded
// later via PUT /_matrix/media/v3/upload/{serverName}/{mediaID}.
func (s *Store) CreatePendingMedia(ctx context.Context, mediaID, userID string, now int64) error {
	_, err := s.pool.Exec(ctx,
		`INSERT INTO media_pending(media_id, user_id, created_ts) VALUES ($1,$2,$3)
		 ON CONFLICT (media_id) DO NOTHING`,
		mediaID, userID, now)
	return err
}

// PendingMedia returns the owner of a created-but-not-uploaded media ID, or
// ErrNotFound.
func (s *Store) PendingMedia(ctx context.Context, mediaID string) (string, error) {
	var userID string
	err := s.pool.QueryRow(ctx, `SELECT user_id FROM media_pending WHERE media_id=$1`, mediaID).Scan(&userID)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return "", ErrNotFound
		}
		return "", err
	}
	return userID, nil
}

// CompletePendingMedia removes the pending marker once the blob is uploaded.
func (s *Store) CompletePendingMedia(ctx context.Context, mediaID string) error {
	_, err := s.pool.Exec(ctx, `DELETE FROM media_pending WHERE media_id=$1`, mediaID)
	return err
}
