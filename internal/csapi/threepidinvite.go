package csapi

import (
	"bytes"
	"context"
	"crypto/ed25519"
	"crypto/tls"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strings"
	"time"

	"github.com/AkagiYui/katrix/internal/crypto"
	"github.com/AkagiYui/katrix/internal/events"
	"github.com/AkagiYui/katrix/internal/homeserver"
	"github.com/AkagiYui/katrix/internal/httpx"
	"github.com/AkagiYui/katrix/internal/roomver"
	"github.com/AkagiYui/katrix/internal/rooms"
	"github.com/AkagiYui/katrix/internal/storage"
)

// registerThirdPartyInvite wires the federation 3PID onbind endpoint (spec
// §3PID invites): when the identity server validates a previously-stored 3PID
// invite (the invitee bound the 3PID), it POSTs the bind details here so this
// homeserver can turn the pending m.room.third_party_invite into a real
// m.room.member invite.
func (a *API) registerThirdPartyInvite(mux *http.ServeMux) {
	mux.HandleFunc("POST /_matrix/federation/v1/3pid/onbind", a.OnBind3PID)
}

// OnBind3PID handles POST /_matrix/federation/v1/3pid/onbind. The identity
// server delivers one or more pending invites; each is verified (signature
// against the stored m.room.third_party_invite event's public keys, plus key
// revocation check) and exchanged into a member invite. A failing invite is
// skipped (the identity server still gets a 200; the invitee simply cannot
// join until the invite is valid — mirror of Synapse's best-effort loop).
func (a *API) OnBind3PID(w http.ResponseWriter, r *http.Request) {
	var req struct {
		Invites []struct {
			Medium  string          `json:"medium"`
			Address string          `json:"address"`
			Sender  string          `json:"sender"`
			Mxid    string          `json:"mxid"`
			RoomID  string          `json:"room_id"`
			Signed  json.RawMessage `json:"signed"`
		} `json:"invites"`
	}
	if err := httpx.DecodeJSON(w, r, &req); err != nil {
		httpx.WriteError(w, httpx.ErrBadJSON("invalid onbind body"))
		return
	}
	for _, inv := range req.Invites {
		if inv.Mxid == "" || inv.RoomID == "" || len(inv.Signed) == 0 {
			continue
		}
		if err := a.exchangeThirdPartyInvite(r.Context(), inv.Sender, inv.Mxid, inv.RoomID, inv.Signed); err != nil {
			// Best-effort: an unverifiable invite is skipped; the join will
			// surface the failure to the invitee.
			_ = err
		}
	}
	httpx.WriteJSON(w, http.StatusOK, httpx.EmptyJSON)
}

// exchangeThirdPartyInvite verifies a signed third-party invite and persists
// the corresponding m.room.member invite (membership=invite) into the room. The
// sender is the original inviter (whose identity server signed the pending
// invite); the target is the 3PID owner who bound the address. This is how a
// stored 3PID invite becomes a real invite the invitee can accept (spec §3PID
// invites; mirror of Synapse's exchange_third_party_invite).
func (a *API) exchangeThirdPartyInvite(ctx context.Context, sender, mxid, roomID string, signedRaw json.RawMessage) error {
	displayName, err := a.verifyThirdPartySigned(ctx, roomID, signedRaw, mxid)
	if err != nil {
		return err
	}
	// Build the member invite: sender = original inviter, target = invitee.
	// The auth rules let a third-party invite through without the sender being
	// a joined member (the identity server's signature is the authorisation).
	content := map[string]any{
		"membership": "invite",
		"third_party_invite": map[string]any{
			"display_name": displayName,
			"signed":       json.RawMessage(signedRaw),
		},
	}
	contentRaw, _ := json.Marshal(content)
	return a.persistThirdPartyMemberInvite(ctx, sender, mxid, roomID, contentRaw)
}

// verifyThirdPartySigned checks a signed third-party invite block against the
// room's matching m.room.third_party_invite event (the token names the state
// key; the signature must verify against one of the event's published public
// keys and the winning key must not be revoked — spec §3PID invites). It
// returns the invite's display_name.
func (a *API) verifyThirdPartySigned(ctx context.Context, roomID string, signedRaw json.RawMessage, expectedMxid string) (string, error) {
	var signed struct {
		Token      string                       `json:"token"`
		Mxid       string                       `json:"mxid"`
		Signatures map[string]map[string]string `json:"signatures"`
	}
	if err := json.Unmarshal(signedRaw, &signed); err != nil || signed.Token == "" {
		return "", fmt.Errorf("third-party invite: malformed signed block")
	}
	// The signed block names the bound user; when the caller knows who that
	// must be, the two must agree.
	if expectedMxid != "" && signed.Mxid != "" && signed.Mxid != expectedMxid {
		return "", fmt.Errorf("third-party invite: signed mxid does not match")
	}

	// Fetch the stored m.room.third_party_invite event for the token; it holds
	// the public keys the identity server signs with.
	tpiID, err := a.Store.GetStateEvent(ctx, roomID, "m.room.third_party_invite", signed.Token)
	if err != nil {
		return "", fmt.Errorf("third-party invite: no matching m.room.third_party_invite event")
	}
	tpi, err := a.Store.GetEvent(ctx, tpiID)
	if err != nil {
		return "", fmt.Errorf("third-party invite: fetch invite event: %w", err)
	}
	var tpiContent struct {
		DisplayName string `json:"display_name"`
		PublicKey   string `json:"public_key"`
		PublicKeys  []struct {
			PublicKey      string `json:"public_key"`
			KeyValidityURL string `json:"key_validity_url"`
		} `json:"public_keys"`
	}
	if err := json.Unmarshal(tpi.Content, &tpiContent); err != nil {
		return "", fmt.Errorf("third-party invite: bad invite event content")
	}
	// An invite event whose content no longer carries public keys has been
	// revoked: the auth check would reject the eventual join anyway.
	if tpiContent.PublicKey == "" && len(tpiContent.PublicKeys) == 0 {
		return "", fmt.Errorf("third-party invite: invite has no public keys")
	}

	// Verify the signed block against every key the invite published, and check
	// the winning key is not revoked (key_validity_url, when present).
	raw, _ := json.Marshal(signed)
	checkKey := func(b64 string) bool {
		pub, err := base64.RawStdEncoding.DecodeString(b64)
		if err != nil || len(pub) != ed25519.PublicKeySize {
			return false
		}
		for _, sigBlock := range signed.Signatures {
			for keyID, sigB64 := range sigBlock {
				if !strings.HasPrefix(keyID, "ed25519:") {
					continue
				}
				if crypto.VerifyJSONWith(sigB64, "", crypto.KeyID(keyID), ed25519.PublicKey(pub), raw) == nil {
					return true
				}
			}
		}
		return false
	}
	verified := false
	if tpiContent.PublicKey != "" && checkKey(tpiContent.PublicKey) {
		verified = true
	}
	for _, pk := range tpiContent.PublicKeys {
		if verified {
			break
		}
		if checkKey(pk.PublicKey) && (pk.KeyValidityURL == "" || a.thirdPartyKeyValid(ctx, pk.KeyValidityURL, pk.PublicKey)) {
			verified = true
		}
	}
	if !verified {
		return "", fmt.Errorf("third-party invite: signature did not verify against any published key")
	}
	return tpiContent.DisplayName, nil
}

// thirdPartyKeyValid checks a key validity URL (spec §3PID invites: the
// identity server's /pubkey/isvalid endpoint) for the given public key. A
// non-2xx response or {"valid": false} means the key was revoked.
func (a *API) thirdPartyKeyValid(ctx context.Context, validityURL, publicKey string) bool {
	u, err := url.Parse(validityURL)
	if err != nil {
		return false
	}
	q := u.Query()
	q.Set("public_key", publicKey)
	u.RawQuery = q.Encode()
	tr := http.DefaultTransport.(*http.Transport).Clone()
	if a.Config.IdentityServerInsecure {
		tr.TLSClientConfig = &tls.Config{InsecureSkipVerify: true} // #nosec G402 -- test-only flag
	}
	client := &http.Client{Timeout: 10 * time.Second, Transport: tr}
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, u.String(), nil)
	if err != nil {
		return false
	}
	resp, err := client.Do(req)
	if err != nil {
		return false
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return false
	}
	body, err := io.ReadAll(io.LimitReader(resp.Body, 4096))
	if err != nil {
		return false
	}
	var out struct {
		Valid bool `json:"valid"`
	}
	if err := json.Unmarshal(body, &out); err != nil {
		return false
	}
	return out.Valid
}

// exchangeAndJoinWithThirdParty handles a client join carrying
// third_party_signed (spec §3PID invites): the join is authorized by the
// identity server's signature rather than a prior member invite. The signed
// block is verified against the room's m.room.third_party_invite event, then
// the join event is built with the signed block as its third_party_invite so
// the auth rules can authorise it. An already-invited user (e.g. after an
// onbind exchange) joins through the normal invite-acceptance path instead.
func (a *API) exchangeAndJoinWithThirdParty(r *http.Request, auth *homeserver.Auth, roomID string, tps json.RawMessage, extra map[string]any) error {
	if _, err := a.Store.GetRoom(r.Context(), roomID); err != nil {
		return newRoomError(http.StatusNotFound, "M_NOT_FOUND", "room not found")
	}
	if _, err := a.verifyThirdPartySigned(r.Context(), roomID, tps, auth.UserID); err != nil {
		return newRoomError(http.StatusForbidden, "M_FORBIDDEN", err.Error())
	}
	// If the user already holds an invite (the onbind exchange created one),
	// accept it through the normal invite-join path.
	if m, err := a.Store.GetMembership(r.Context(), roomID, auth.UserID); err == nil && m.Membership == rooms.MembershipInvite {
		content := map[string]any{"membership": rooms.MembershipJoin}
		for k, v := range extra {
			content[k] = v
		}
		if _, err := a.sendMemberEventWithContent(r, auth, roomID, auth.UserID, content); err != nil {
			return err
		}
		return nil
	}
	// No prior invite: the join is authorised purely by the third-party
	// signature, carried as content.third_party_invite.signed.
	content := map[string]any{
		"membership": "join",
		"third_party_invite": map[string]any{
			"signed": json.RawMessage(tps),
		},
	}
	for k, v := range extra {
		content[k] = v
	}
	_, err := a.sendMemberEventWithContent(r, auth, roomID, auth.UserID, content)
	return err
}

// persistThirdPartyMemberInvite persists an m.room.member invite authored on
// behalf of a remote (or left) sender, authorized by a verified third-party
// invite signature. It runs the room auth rules (the third-party exception lets
// the invite through without the sender being joined), then persists, updates
// the denormalised membership, wakes the invitee's sync and broadcasts the
// event to the room's servers.
func (a *API) persistThirdPartyMemberInvite(ctx context.Context, sender, target, roomID string, contentRaw json.RawMessage) error {
	a.stateMu.Lock()
	defer a.stateMu.Unlock()
	room, err := a.Store.GetRoom(ctx, roomID)
	if err != nil {
		return newRoomError(http.StatusNotFound, "M_NOT_FOUND", "room not found")
	}
	version := roomver.Version(room.Version)
	rules, ok := roomver.Get(version)
	if !ok {
		return newRoomError(http.StatusBadRequest, "M_UNSUPPORTED_ROOM_VERSION", "unknown room version")
	}
	st, err := a.buildStateSnapshot(ctx, roomID, target, sender, contentRaw)
	if err != nil {
		return err
	}
	if err := rooms.Authorize(rules, "m.room.member", target, sender, contentRaw, st, true); err != nil {
		return newRoomError(http.StatusForbidden, "M_FORBIDDEN", err.Error())
	}
	prev, depth := a.dagTipFor(ctx, roomID)
	authIDs := a.authEventIDs(ctx, roomID, sender, target)
	sk := target
	b := events.Builder{
		Type:           "m.room.member",
		Sender:         sender,
		RoomID:         roomID,
		Content:        contentRaw,
		Depth:          depth,
		OriginServerTS: a.Now(),
		PrevEvents:     prev,
		AuthEvents:     authIDs,
		StateKey:       &sk,
	}
	ev, err := b.BuildForVersion(a.ServerName(), a.Key, version)
	if err != nil {
		return err
	}
	stream, err := persistEventInRoom(ctx, a.Store, ev, version, roomID)
	if err != nil {
		return err
	}
	mc, _ := rooms.ParseMember(contentRaw)
	if mc != nil {
		if err := a.Store.UpsertMembership(ctx, storage.MembershipRow{
			RoomID: roomID, UserID: target, Membership: mc.Membership,
			EventID: ev.EventID(), StreamOrdering: stream, Depth: ev.Depth(),
		}); err != nil {
			return err
		}
	}
	// The invitee is remote in the federated cases; notify local syncs and
	// deliver the invite PDU to the room's servers (including the invitee's).
	a.notifyRoomMembers(ctx, roomID)
	a.broadcastPDU(ctx, roomID, ev)
	return nil
}

// thirdPartySignedFromJoin extracts the third_party_signed block from a join
// request body, if present (spec §3PID invites: a client joins a room it was
// third-party invited to by passing third_party_signed in the /join body). It
// re-seats the remaining body so joinCustomContent can still read the other
// custom fields.
func thirdPartySignedFromJoin(r *http.Request) json.RawMessage {
	if r.Body == nil {
		return nil
	}
	data, err := io.ReadAll(r.Body)
	if err != nil || len(data) == 0 {
		return nil
	}
	var body map[string]json.RawMessage
	if err := json.Unmarshal(data, &body); err != nil {
		return nil
	}
	tps := body["third_party_signed"]
	delete(body, "third_party_signed")
	if rest, err := json.Marshal(body); err == nil {
		r.Body = io.NopCloser(bytes.NewReader(rest))
	}
	return tps
}
