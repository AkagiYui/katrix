package federation

import (
	"fmt"
	"net/http"
	"strings"

	"github.com/AkagiYui/katrix/internal/canonicaljson"
	"github.com/AkagiYui/katrix/internal/crypto"
)

// SignRequest signs an outbound federation request with the local server's
// signing key, adding the X-Matrix Authorization header. The signed payload is
// the canonical JSON of {method, uri, origin, destination}.
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
