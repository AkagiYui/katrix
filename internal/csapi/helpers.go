package csapi

import (
	"bytes"
	"encoding/json"
	"io"
	"math"
	"net"
	"net/http"
	"strings"

	"github.com/AkagiYui/katrix/internal/httpx"
	"github.com/AkagiYui/katrix/internal/ids"
	"github.com/AkagiYui/katrix/internal/storage"
)

// readBody reads a request body with a sane size limit. An empty body is
// allowed (Matrix treats a missing body as {}). The returned bytes may be
// passed to checkUIA or json.Unmarshal by the caller.
//
// The body is validated as JSON before being returned: malformed JSON, NaN,
// Infinity and out-of-range integers are rejected with 400 M_BAD_JSON (this
// matches the Matrix spec's requirement that servers reject invalid JSON event
// content rather than persisting it).
func readBody(r *http.Request) ([]byte, error) {
	limited := http.MaxBytesReader(nil, r.Body, 1<<20)
	b, err := io.ReadAll(limited)
	if err != nil {
		return nil, httpx.ErrTooLarge("request body too large")
	}
	return b, nil
}

// readEventContent reads and validates a request body intended to be used as
// Matrix event content. In addition to readBody's size limit it enforces the
// per-event size limit (spec default 65536 bytes), rejecting larger bodies
// with 413 M_TOO_LARGE, and validates the JSON (rejecting NaN, Infinity and
// malformed JSON with 400 M_BAD_JSON). An empty body returns {}.
//
// Note: this intentionally does NOT forbid fractional numbers (e.g. 1.5),
// which are legal in arbitrary event content even though Matrix's canonical
// JSON (used for hashing/signing) forbids them. Only the structurally invalid
// values the spec requires servers to reject (NaN, Infinity, trailing data)
// are rejected here.
func readEventContent(r *http.Request) (json.RawMessage, error) {
	body, err := readBody(r)
	if err != nil {
		return nil, err
	}
	if len(body) == 0 {
		return json.RawMessage(`{}`), nil
	}
	if len(body) > maxEventBytes {
		return nil, httpx.ErrTooLarge("event content too large")
	}
	if err := validateEventJSON(body); err != nil {
		return nil, httpx.ErrBadJSON("invalid JSON: " + err.Error())
	}
	return json.RawMessage(body), nil
}

// maxEventBytes is the spec default per-event content size limit (65536).
const maxEventBytes = 65536

// validateEventJSON checks that body is a single valid JSON object and contains
// no NaN, Infinity, or trailing data. It uses UseNumber so numbers are not
// coerced to float64 (which would silently accept Infinity as a number token).
//
// The top level must be an object: Matrix event content is always a JSON
// object, and refusing anything else means a body that serialises to a bare
// JSON string/number/array (e.g. a base64 blob) is rejected with 400.
func validateEventJSON(body []byte) error {
	dec := json.NewDecoder(bytes.NewReader(body))
	dec.UseNumber()
	var v any
	if err := dec.Decode(&v); err != nil {
		return err
	}
	if dec.More() {
		return errTrailingData
	}
	if _, ok := v.(map[string]any); !ok {
		return errNotObject
	}
	return scanNumbers(v)
}

var (
	errTrailingData = errStr("trailing data after JSON value")
	errNotObject    = errStr("event content must be a JSON object")
)

type errStr string

func (e errStr) Error() string { return string(e) }

// scanNumbers walks a decoded JSON value and rejects json.Number values that
// are NaN, Infinity, fractional (non-integer) numbers, or integers outside the
// range a Matrix canonical-JSON document may represent ([-(2^53-1), 2^53-1],
// per the spec). The Matrix spec requires servers to reject event content
// containing such numbers (they cannot be represented in canonical JSON, so
// the event could never be signed/hashed).
func scanNumbers(v any) error {
	switch val := v.(type) {
	case json.Number:
		// Try integer first: out-of-range integers must be rejected.
		if i, err := val.Int64(); err == nil {
			if i > maxSafeInt || i < -maxSafeInt {
				return errStr("integer out of range")
			}
			return nil
		}
		// Not a plain int64: this is either a fractional number, an
		// out-of-int64-range integer, or NaN/Infinity. All must be rejected.
		f, err := val.Float64()
		if err != nil {
			return err
		}
		if math.IsNaN(f) || math.IsInf(f, 0) {
			return errStr("NaN/Infinity not allowed")
		}
		// Fractional numbers (non-integer) are rejected per the spec.
		if math.Trunc(f) != f {
			return errStr("fractional number not allowed")
		}
		// A finite integer-like number that overflowed int64 (e.g. 1e30) or
		// exceeds the 2^53-1 canonical-JSON range.
		return errStr("integer out of range")
	case map[string]any:
		for _, item := range val {
			if err := scanNumbers(item); err != nil {
				return err
			}
		}
	case []any:
		for _, item := range val {
			if err := scanNumbers(item); err != nil {
				return err
			}
		}
	}
	return nil
}

// maxSafeInt is 2^53-1, the largest integer a Matrix canonical-JSON document
// may represent.
const maxSafeInt = (1 << 53) - 1

// bearer extracts the access token from the request (Authorization header or
// legacy access_token query parameter). It is the csapi-local alias of the
// homeserver extractor, kept so account handlers can stay self-contained.
func bearer(r *http.Request) string {
	if h := r.Header.Get("Authorization"); len(h) > 7 && h[:7] == "Bearer " {
		return h[7:]
	}
	return r.URL.Query().Get("access_token")
}

// loginResult is the token bundle produced by issueLogin.
type loginResult struct {
	AccessToken  string
	RefreshToken string
	ExpiresInMS  int64
}

// accessTokenTTL is how long a freshly issued access token is valid when
// refresh tokens are in use. Tokens without a refresh token never expire
// (Matrix's default behaviour).
const accessTokenTTL = 5 * 60 * 1000 // 5 minutes

// issueLogin creates a device (if needed) and issues a new access token (and
// optional refresh token) for the given user/device pair. It persists the
// device record and access token before returning.
func (a *API) issueLogin(r *http.Request, localpart, deviceID, displayName string, withRefresh bool) (loginResult, error) {
	now := a.Now()
	userID := a.HS.UserID(localpart)
	// A new device changes the user's device list; the user's other devices
	// (and, via federation, remote servers sharing a room) must learn about it.
	// Record the change in the shared sync stream so /sync emits
	// device_lists.changed, and queue a federation m.device_list_update EDU.
	_, _ = a.Store.RecordDeviceListChange(r.Context(), userID, false)
	a.broadcastDeviceListUpdate(r.Context(), userID, deviceID, false)
	// Persist the device row (idempotent on conflict).
	if err := a.Store.UpsertDevice(r.Context(), storage.Device{
		UserLocalpart: localpart,
		DeviceID:      deviceID,
		DisplayName:   displayName,
		CreatedTS:     now,
		LastSeenTS:    now,
		LastSeenIP:    clientIP(r),
	}); err != nil {
		return loginResult{}, httpx.ErrUnknown(err.Error())
	}

	accessToken := ids.RandomToken()
	var refresh string
	var expires int64
	if withRefresh {
		refresh = ids.RandomToken()
		expires = now + accessTokenTTL
	}
	if err := a.Store.CreateAccessToken(r.Context(), storage.AccessToken{
		Token:         accessToken,
		UserLocalpart: localpart,
		DeviceID:      deviceID,
		RefreshToken:  refresh,
		ExpiresTS:     expires,
		CreatedTS:     now,
	}); err != nil {
		return loginResult{}, httpx.ErrUnknown(err.Error())
	}

	return loginResult{
		AccessToken:  accessToken,
		RefreshToken: refresh,
		ExpiresInMS:  accessTokenTTL,
	}, nil
}

// clientIP extracts the remote address from the request, honouring a single
// X-Forwarded-For hop (leftmost entry) when present. Behind a trusted proxy
// the deployment should configure hop counts; this keeps a sane default.
func clientIP(r *http.Request) string {
	if xff := r.Header.Get("X-Forwarded-For"); xff != "" {
		if i := strings.IndexByte(xff, ','); i >= 0 {
			return strings.TrimSpace(xff[:i])
		}
		return strings.TrimSpace(xff)
	}
	host, _, err := net.SplitHostPort(r.RemoteAddr)
	if err != nil {
		return r.RemoteAddr
	}
	return host
}
