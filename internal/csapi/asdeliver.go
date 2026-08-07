package csapi

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"github.com/AkagiYui/katrix/internal/appservice"
	"github.com/AkagiYui/katrix/internal/events"
)

// Application-service event delivery (spec "Application services" §Pushing
// events): when an event is persisted (client or federation path), every
// application service "interested" in the event receives it via a POST to
// /_matrix/app/v1/transactions/{txnId}. An AS is interested when any of its
// namespaces match: the event's sender (a local user), the membership event's
// target, the room ID (rooms namespace), or any alias of the room (aliases
// namespace). Delivery is best-effort on a goroutine so an unreachable AS
// never blocks the request that stored the event.

// asInterested reports whether the given application service is interested in
// an event in roomID (based on the event's sender/target and the room).
func (a *API) asInterested(ctx context.Context, reg *appservice.Registration, roomID string, sender, stateKey string) bool {
	if reg == nil {
		return false
	}
	// Sender is a local user matching the AS's user namespace.
	if a.IsLocalUser(sender) {
		if appserviceUserInNamespaces(reg, a.LocalpartOf(sender), a.ServerName()) {
			return true
		}
	}
	// Membership event: the target matching the AS's user namespace.
	if stateKey != "" && a.IsLocalUser(stateKey) {
		if appserviceUserInNamespaces(reg, a.LocalpartOf(stateKey), a.ServerName()) {
			return true
		}
	}
	// Room ID matches the AS's rooms namespace.
	for _, ns := range reg.Namespaces.Rooms {
		if regexpMatch(ns.Regex, roomID, "") {
			return true
		}
	}
	// Any alias of the room matches the AS's aliases namespace.
	if aliases, err := a.Store.AliasesForRoom(ctx, roomID); err == nil {
		for _, alias := range aliases {
			if appserviceAliasInNamespaces(reg, aliasLocalpartOf(alias)) {
				return true
			}
		}
	}
	return false
}

func aliasLocalpartOf(alias string) string {
	if i := indexRune(alias, ':'); i >= 0 && len(alias) > 0 && alias[0] == '#' {
		return alias[1:i]
	}
	return alias
}

func indexRune(s string, r rune) int {
	for i, c := range s {
		if c == r {
			return i
		}
	}
	return -1
}

// deliverASEvents pushes a just-persisted event to every interested application
// service. The event is delivered in client format (with room_id) — the spec's
// transaction events are the same shape /sync delivers.
func (a *API) deliverASEvents(ctx context.Context, roomID string, ev *events.Event) {
	if a.HS.AppServices == nil || ev == nil {
		return
	}
	sk, _ := ev.StateKey()
	var interested []*appservice.Registration
	for _, reg := range a.HS.AppServices.All() {
		if a.asInterested(ctx, reg, roomID, ev.Sender(), sk) {
			interested = append(interested, reg)
		}
	}
	if len(interested) == 0 {
		return
	}
	clientEv := asClientEvent(ev, roomID)
	client := appservice.NewClient()
	for _, reg := range interested {
		reg := reg
		go func() {
			// A numeric transaction ID: sytest's AS mock matches
			// /_matrix/app/v1/transactions/\d+ (and the spec's txn IDs are
			// opaque but numeric keeps every client happy).
			txnID := fmt.Sprintf("%d", time.Now().UnixNano())
			_ = client.PushTransaction(context.Background(), reg, txnID, []json.RawMessage{clientEv})
		}()
	}
}

// asClientEvent renders a signed event as the client-format event delivered to
// application services (type, content, sender, room_id, state_key, event_id,
// origin_server_ts — never the raw PDU).
func asClientEvent(ev *events.Event, roomID string) json.RawMessage {
	m := map[string]any{
		"type":             ev.Type(),
		"content":          json.RawMessage(ev.Content()),
		"sender":           ev.Sender(),
		"room_id":          roomID,
		"origin_server_ts": ev.OriginServerTS(),
		"event_id":         ev.EventID(),
	}
	if sk, ok := ev.StateKey(); ok {
		m["state_key"] = sk
	}
	b, _ := json.Marshal(m)
	return b
}
