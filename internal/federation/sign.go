package federation

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strings"

	"github.com/AkagiYui/katrix/internal/canonicaljson"
	"github.com/AkagiYui/katrix/internal/crypto"
)

// SignRequest signs an outbound federation request with the local server's
// signing key, adding the X-Matrix Authorization header. The signed payload is
// the canonical JSON of {method, uri, origin, destination} plus the request
// body (content) when present — the spec's request-authentication scheme, and
// the form remote servers (incl. gomatrixserverlib) verify. Signing over an
// empty content key would be a signature mismatch for requests with a body.
func signRequestWith(req *http.Request, origin string, key *crypto.SigningKey) error {
	if key == nil {
		return nil
	}
	destination := req.Host
	signable := map[string]any{
		"method":      strings.ToUpper(req.Method),
		"uri":         req.URL.RequestURI(),
		"origin":      origin,
		"destination": destination,
	}
	if req.Body != nil {
		// The body may already have been read (e.g. by an earlier marshalling
		// step in the caller); snapshot the bytes to sign, then hand them back.
		if body, err := io.ReadAll(req.Body); err == nil && len(body) > 0 {
			signable["content"] = json.RawMessage(body)
			req.Body = io.NopCloser(bytes.NewReader(body))
		}
	}
	canonical, err := canonicaljson.Marshal(signable)
	if err != nil {
		return fmt.Errorf("federation: canonical request: %w", err)
	}
	sig, err := crypto.SignedBytes(key, canonical)
	if err != nil {
		return fmt.Errorf("federation: sign request: %w", err)
	}
	auth := fmt.Sprintf("X-Matrix origin=%s,key=%s,destination=%s,sig=%s",
		origin, key.KeyID(), destination, sig)
	req.Header.Set("Authorization", auth)
	return nil
}
