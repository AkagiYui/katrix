package sync

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"github.com/AkagiYui/katrix/internal/storage"
)

// Response is the /sync response body.
type Response struct {
	NextBatch   string         `json:"next_batch"`
	Rooms       RoomsResp      `json:"rooms"`
	Presence    *PresenceResp  `json:"presence,omitempty"`
	AccountData *EventsSection `json:"account_data,omitempty"`
	ToDevice    *EventsSection `json:"to_device,omitempty"`
}

// RoomsResp is the rooms section of /sync.
type RoomsResp struct {
	Join   map[string]JoinedRoom  `json:"join,omitempty"`
	Invite map[string]InvitedRoom `json:"invite,omitempty"`
	Leave  map[string]LeftRoom    `json:"leave,omitempty"`
}

// JoinedRoom is a room the user has joined.
type JoinedRoom struct {
	Timeline    Timeline          `json:"timeline"`
	State       StateSet          `json:"state"`
	AccountData StateSet          `json:"account_data,omitempty"`
	Ephemeral   []json.RawMessage `json:"ephemeral,omitempty"`
	// UnreadNotifications omitted (P8).
}

// InvitedRoom is a room the user is invited to.
type InvitedRoom struct {
	InviteState StateSet `json:"invite_state"`
}

// LeftRoom is a room the user has left/been banned from.
type LeftRoom struct {
	Timeline Timeline `json:"timeline"`
	State    StateSet `json:"state"`
}

// Timeline is the recent events in a room.
type Timeline struct {
	Events    []json.RawMessage `json:"events"`
	Limited   bool              `json:"limited"`
	PrevBatch string            `json:"prev_batch,omitempty"`
}

// StateSet is a set of state events.
type StateSet struct {
	Events []json.RawMessage `json:"events,omitempty"`
}

// EventsSection is a generic list of events.
type EventsSection struct {
	Events []json.RawMessage `json:"events,omitempty"`
}

// PresenceResp holds presence updates (stubbed for P3).
type PresenceResp struct {
	Events []json.RawMessage `json:"events,omitempty"`
}

// SyncOptions controls a single /sync computation.
type SyncOptions struct {
	UserID      string
	Localpart   string
	DeviceID    string
	Since       Token
	Timeout     time.Duration
	FullState   bool
	SetPresence string
}

// Sync computes the /sync response. For initial sync (Since.Stream==0) it
// returns full room state + recent timeline. For incremental sync it returns
// events with stream_ordering > Since.Stream.
func (e *Engine) Sync(ctx context.Context, opts SyncOptions) (*Response, error) {
	// Determine the current max stream ordering.
	maxStream, err := e.store.MaxStreamOrdering(ctx)
	if err != nil {
		return nil, fmt.Errorf("sync: max stream: %w", err)
	}
	// For initial sync we want the full state of each joined room + a recent
	// timeline window ending at maxStream.
	resp := &Response{}
	rooms := RoomsResp{Join: map[string]JoinedRoom{}, Invite: map[string]InvitedRoom{}, Leave: map[string]LeftRoom{}}

	// Rooms the user has joined.
	joinedRoomIDs, err := e.store.RoomsForUser(ctx, opts.UserID)
	if err != nil {
		return nil, err
	}
	for _, roomID := range joinedRoomIDs {
		jr, err := e.buildJoinedRoom(ctx, roomID, opts, maxStream)
		if err != nil {
			return nil, err
		}
		rooms.Join[roomID] = jr
	}

	// Rooms the user is invited to (membership=invite).
	inviteRows, _ := e.store.Members(ctx, "", "")
	_ = inviteRows // filtered below per-room via a dedicated query path
	invited, err := e.store.InvitedRooms(ctx, opts.UserID)
	if err == nil {
		for _, roomID := range invited {
			rooms.Invite[roomID] = e.buildInvitedRoom(ctx, roomID)
		}
	}

	// Left rooms (membership=leave/ban) - include recent timeline once.
	leftRooms, _ := e.store.LeftRooms(ctx, opts.UserID)
	if leftRooms != nil {
		for _, roomID := range leftRooms {
			lr, err := e.buildLeftRoom(ctx, roomID, opts, maxStream)
			if err != nil {
				continue
			}
			rooms.Leave[roomID] = lr
		}
	}

	resp.Rooms = rooms

	// Account data (global + per-room).
	adRows, err := e.store.AccountDataSince(ctx, opts.Localpart, opts.Since.Stream)
	if err == nil {
		globalAD := StateSet{}
		roomAD := map[string]StateSet{}
		for _, a := range adRows {
			ev := mustMarshalEvent(a.Type, "", "", a.Content, 0, "")
			if a.RoomID == "" {
				globalAD.Events = append(globalAD.Events, ev)
			} else {
				r := roomAD[a.RoomID]
				r.Events = append(r.Events, ev)
				roomAD[a.RoomID] = r
			}
		}
		if len(globalAD.Events) > 0 {
			resp.AccountData = &EventsSection{Events: globalAD.Events}
		}
		for roomID, ad := range roomAD {
			if jr, ok := rooms.Join[roomID]; ok {
				jr.AccountData = ad
				rooms.Join[roomID] = jr
			}
		}
	}

	// Presence: include the calling user's own presence state.
	if p, err := e.store.GetPresence(ctx, opts.UserID); err == nil && p != nil {
		ev, _ := json.Marshal(map[string]any{
			"type": "m.presence",
			"content": map[string]any{
				"presence": p.Presence,
				"user_id":  p.UserID,
			},
		})
		if p.StatusMsg != "" {
			ev, _ = json.Marshal(map[string]any{
				"type": "m.presence",
				"content": map[string]any{
					"presence":   p.Presence,
					"user_id":    p.UserID,
					"status_msg": p.StatusMsg,
				},
			})
		}
		resp.Presence = &PresenceResp{Events: []json.RawMessage{ev}}
	}

	// Ephemeral: typing + receipts.
	for roomID := range rooms.Join {
		jr := rooms.Join[roomID]
		// Typing ephemeral.
		typingUsers := e.typing.TypingUsers(roomID)
		if len(typingUsers) > 0 {
			users := make([]string, len(typingUsers))
			copy(users, typingUsers)
			eph, _ := json.Marshal(map[string]any{
				"type":    "m.typing",
				"content": map[string]any{"user_ids": users},
			})
			jr.Ephemeral = append(jr.Ephemeral, eph)
		}
		// Receipts ephemeral (since-based). Include all users' receipts for
		// rooms the syncer is joined to.
		receipts, _ := e.store.ReceiptsSince(ctx, opts.UserID, opts.Since.Stream)
		if len(receipts) > 0 {
			// Build content: {event_id: {receipt_type: {ts: N, user_id: "@u:hs"}}}
			byRoom := map[string]map[string]map[string]map[string]any{}
			for _, rc := range receipts {
				if byRoom[rc.RoomID] == nil {
					byRoom[rc.RoomID] = map[string]map[string]map[string]any{}
				}
				if byRoom[rc.RoomID][rc.EventID] == nil {
					byRoom[rc.RoomID][rc.EventID] = map[string]map[string]any{}
				}
				byRoom[rc.RoomID][rc.EventID][rc.ReceiptType] = map[string]any{
					"ts":      rc.TS,
					"user_id": rc.UserID,
				}
			}
			if evMap, ok := byRoom[roomID]; ok {
				eph, _ := json.Marshal(map[string]any{
					"type":    "m.receipt",
					"content": evMap,
				})
				jr.Ephemeral = append(jr.Ephemeral, eph)
			}
		}
		rooms.Join[roomID] = jr
	}

	// Next batch token is the current max stream.
	resp.NextBatch = Token{Stream: maxStream}.Encode()

	// To-device messages: deliver any queued for this device. DequeueToDevice
	// deletes on delivery, so each message is received exactly once; pass
	// since=0 to drain the queue each cycle.
	td, err := e.store.DequeueToDevice(ctx, opts.UserID, opts.DeviceID, 0)
	if err == nil && len(td) > 0 {
		events := make([]json.RawMessage, 0, len(td))
		for _, m := range td {
			ev := mustMarshalEvent(m.Type, m.Sender, "", m.Content, m.CreatedTS, "")
			events = append(events, ev)
		}
		resp.ToDevice = &EventsSection{Events: events}
	}
	return resp, nil
}

// buildJoinedRoom constructs the JoinedRoom section.
func (e *Engine) buildJoinedRoom(ctx context.Context, roomID string, opts SyncOptions, maxStream int64) (JoinedRoom, error) {
	jr := JoinedRoom{}
	// Timeline: events with stream_ordering > since, limited to 50 for initial.
	from := opts.Since.Stream
	if from == 0 {
		from = maxStream - 50
		if from < 0 {
			from = 0
		}
	}
	evs, err := e.store.EventsForRoom(ctx, roomID, from, maxStream, 50, "f")
	if err != nil {
		return jr, err
	}
	timeline := Timeline{Events: make([]json.RawMessage, 0, len(evs))}
	for _, ev := range evs {
		timeline.Events = append(timeline.Events, clientEvent(&ev))
	}
	if len(timeline.Events) >= 50 {
		timeline.Limited = true
		timeline.PrevBatch = Token{Stream: from}.Encode()
	}
	jr.Timeline = timeline

	// State: full state on initial sync or full_state; otherwise empty (delta).
	if opts.Since.Stream == 0 || opts.FullState {
		stateRows, err := e.store.GetState(ctx, roomID)
		if err != nil {
			return jr, err
		}
		ids := make([]string, 0, len(stateRows))
		for _, s := range stateRows {
			ids = append(ids, s.EventID)
		}
		stateEvs, _ := e.store.EventsByIDs(ctx, ids)
		jr.State.Events = make([]json.RawMessage, 0, len(stateEvs))
		for _, se := range stateEvs {
			jr.State.Events = append(jr.State.Events, clientEvent(&se))
		}
	}
	return jr, nil
}

// buildInvitedRoom constructs the InvitedRoom section (just the invite state).
func (e *Engine) buildInvitedRoom(ctx context.Context, roomID string) InvitedRoom {
	ir := InvitedRoom{}
	// Fetch the invite m.room.member event.
	id, err := e.store.GetStateEvent(ctx, roomID, "m.room.member", "")
	_ = id
	_ = err
	// Best-effort: include the create + power_levels + join_rules + member.
	stateRows, _ := e.store.GetState(ctx, roomID)
	ids := make([]string, 0, len(stateRows))
	for _, s := range stateRows {
		ids = append(ids, s.EventID)
	}
	evs, _ := e.store.EventsByIDs(ctx, ids)
	for _, se := range evs {
		ir.InviteState.Events = append(ir.InviteState.Events, se.RawJSON)
	}
	return ir
}

// buildLeftRoom constructs the LeftRoom section.
func (e *Engine) buildLeftRoom(ctx context.Context, roomID string, opts SyncOptions, maxStream int64) (LeftRoom, error) {
	lr := LeftRoom{}
	from := opts.Since.Stream
	if from == 0 {
		from = maxStream - 50
		if from < 0 {
			from = 0
		}
	}
	evs, err := e.store.EventsForRoom(ctx, roomID, from, maxStream, 50, "f")
	if err != nil {
		return lr, err
	}
	lr.Timeline.Events = make([]json.RawMessage, 0, len(evs))
	for _, ev := range evs {
		lr.Timeline.Events = append(lr.Timeline.Events, ev.RawJSON)
	}
	return lr, nil
}

// mustMarshalEvent builds a minimal client event JSON for account_data/ephemeral
// sections (which are not real PDUs).
func mustMarshalEvent(eventType, sender, stateKey string, content []byte, ts int64, eventID string) json.RawMessage {
	m := map[string]any{
		"type":    eventType,
		"content": json.RawMessage(content),
	}
	if sender != "" {
		m["sender"] = sender
	}
	if stateKey != "" {
		m["state_key"] = stateKey
	}
	if ts != 0 {
		m["origin_server_ts"] = ts
	}
	if eventID != "" {
		m["event_id"] = eventID
	}
	b, _ := json.Marshal(m)
	return b
}

// clientEvent converts a stored PDU (EventRow.RawJSON) into the client-visible
// event format. The Client-Server API must return the "stripped" event: only
// type, content, sender, state_key (if state), origin_server_ts, event_id —
// never auth_events, hashes, prev_events, or signatures.
func clientEvent(row *storage.EventRow) json.RawMessage {
	m := map[string]any{
		"type":             row.Type,
		"content":          json.RawMessage(row.Content),
		"sender":           row.Sender,
		"origin_server_ts": row.OriginServerTS,
		"event_id":         row.EventID,
	}
	if row.StateKey != "" || isStateTypeSync(row.Type) {
		m["state_key"] = row.StateKey
	}
	b, _ := json.Marshal(m)
	return b
}

func isStateTypeSync(eventType string) bool {
	switch eventType {
	case "m.room.create", "m.room.power_levels", "m.room.join_rules",
		"m.room.history_visibility", "m.room.name", "m.room.topic",
		"m.room.member", "m.room.third_party_invite", "m.room.canonical_alias",
		"m.room.aliases", "m.room.encryption", "m.room.tombstone",
		"m.room.server_acl", "m.room.pinned_events":
		return true
	}
	return false
}
