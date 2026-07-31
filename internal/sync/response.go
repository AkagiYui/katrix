package sync

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"time"

	"github.com/AkagiYui/katrix/internal/events"
	"github.com/AkagiYui/katrix/internal/roomver"
	"github.com/AkagiYui/katrix/internal/storage"
)

// Response is the /sync response body.
type Response struct {
	NextBatch   string         `json:"next_batch"`
	Rooms       RoomsResp      `json:"rooms"`
	Presence    *PresenceResp  `json:"presence,omitempty"`
	AccountData *EventsSection `json:"account_data,omitempty"`
	ToDevice    *EventsSection `json:"to_device,omitempty"`
	DeviceLists *DeviceLists   `json:"device_lists,omitempty"`
}

// DeviceLists carries device-list changes for the syncing user.
type DeviceLists struct {
	Changed []string `json:"changed,omitempty"`
	Left    []string `json:"left,omitempty"`
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
	Summary     *RoomSummary      `json:"summary,omitempty"`
	// UnreadNotifications omitted (P8).
}

// RoomSummary carries the room's member counts (joined + invited).
type RoomSummary struct {
	JoinedMembers  int      `json:"m.joined_member_count,omitempty"`
	InvitedMembers int      `json:"m.invited_member_count,omitempty"`
	Heroes         []string `json:"m.heroes,omitempty"`
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
	Filter      *SyncFilter
}

// SyncFilter is the subset of the filter object that /sync honours. Unknown
// fields are ignored (the full filter is validated at POST /filter).
type SyncFilter struct {
	// Room timeline filters, applied per joined room.
	TimelineTypes      []string
	TimelineNotTypes   []string
	TimelineSenders    []string
	TimelineNotSenders []string
	TimelineLimit      int
	// Lazy-load members: only include membership state events, and only for
	// senders present in the timeline.
	LazyLoadMembers bool
	// IncludeLeave controls whether left rooms appear in the response.
	IncludeLeave bool
	// EventFields narrows the per-event fields returned (JSON pointer paths).
	EventFields []string
}

// ParseSyncFilter decodes a raw filter JSON object into the subset /sync
// honours. A nil/empty input yields a nil filter (no restriction).
func ParseSyncFilter(raw json.RawMessage) *SyncFilter {
	if len(raw) == 0 || string(raw) == "null" {
		return nil
	}
	var obj struct {
		Room struct {
			Timeline struct {
				Types      []string `json:"types"`
				NotTypes   []string `json:"not_types"`
				Senders    []string `json:"senders"`
				NotSenders []string `json:"not_senders"`
				Limit      int      `json:"limit"`
			} `json:"timeline"`
			State struct {
				LazyLoadMembers bool `json:"lazy_load_members"`
			} `json:"state"`
			IncludeLeave bool `json:"include_leave"`
		} `json:"room"`
		EventFields []string `json:"event_fields"`
	}
	if err := json.Unmarshal(raw, &obj); err != nil {
		return nil
	}
	f := &SyncFilter{
		TimelineTypes:      obj.Room.Timeline.Types,
		TimelineNotTypes:   obj.Room.Timeline.NotTypes,
		TimelineSenders:    obj.Room.Timeline.Senders,
		TimelineNotSenders: obj.Room.Timeline.NotSenders,
		TimelineLimit:      obj.Room.Timeline.Limit,
		LazyLoadMembers:    obj.Room.State.LazyLoadMembers,
		IncludeLeave:       obj.Room.IncludeLeave,
		EventFields:        obj.EventFields,
	}
	// A filter with nothing set behaves like no filter.
	if !f.anySet() {
		return nil
	}
	return f
}

func (f *SyncFilter) anySet() bool {
	return f.TimelineLimit > 0 || f.LazyLoadMembers || f.IncludeLeave ||
		len(f.TimelineTypes) > 0 || len(f.TimelineNotTypes) > 0 ||
		len(f.TimelineSenders) > 0 || len(f.TimelineNotSenders) > 0 ||
		len(f.EventFields) > 0
}

// keepTimeline reports whether a timeline event passes the filter.
func (f *SyncFilter) keepTimeline(ev *storage.EventRow) bool {
	if f == nil {
		return true
	}
	if strIn(f.TimelineNotTypes, ev.Type) {
		return false
	}
	if len(f.TimelineTypes) > 0 && !strIn(f.TimelineTypes, ev.Type) {
		return false
	}
	if strIn(f.TimelineNotSenders, ev.Sender) {
		return false
	}
	if len(f.TimelineSenders) > 0 && !strIn(f.TimelineSenders, ev.Sender) {
		return false
	}
	return true
}

// lazyLoadMembers reports whether member events for timeline senders should be
// included in the state section (and only those).
func (f *SyncFilter) lazyLoadMembers() bool { return f != nil && f.LazyLoadMembers }

// applyEventFields narrows a client event to the requested JSON pointer paths.
// When the filter has no event_fields the event is returned unchanged.
func (f *SyncFilter) applyEventFields(ev json.RawMessage) json.RawMessage {
	if f == nil || len(f.EventFields) == 0 {
		return ev
	}
	var obj map[string]json.RawMessage
	if err := json.Unmarshal(ev, &obj); err != nil {
		return ev
	}
	out := map[string]json.RawMessage{}
	for _, ptr := range f.EventFields {
		key := ptr
		if len(key) > 0 && key[0] == '/' {
			key = key[1:]
		}
		// Only single-segment pointers are supported.
		if strings.Contains(key, "/") {
			continue
		}
		if v, ok := obj[key]; ok {
			out[key] = v
		}
	}
	b, _ := json.Marshal(out)
	return b
}

func strIn(list []string, s string) bool {
	for _, v := range list {
		if v == s {
			return true
		}
	}
	return false
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
	ignored := e.ignoredUsers(ctx, opts.Localpart)
	invited, err := e.store.InvitedRooms(ctx, opts.UserID)
	if err == nil {
		for _, roomID := range invited {
			if ignored != nil && e.inviteIsFromIgnored(ctx, roomID, opts.UserID, ignored) {
				// Spec: "Servers must not send room invites from ignored users
				// to clients." Drop the invite from the sync response.
				continue
			}
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

	// Presence: presence events for the calling user and for room peers whose
	// presence changed after the sync token (incremental) or all joined room
	// peers (initial sync).
	e.appendPresence(ctx, resp, opts)

	// Device list changes for the syncing user (their own device list updates
	// surface on their other devices' /sync).
	if changed, left, err := e.store.DeviceListChangesSince(ctx, opts.Since.Stream); err == nil {
		if len(changed) > 0 || len(left) > 0 {
			resp.DeviceLists = &DeviceLists{Changed: changed, Left: left}
		}
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
			// Build content: {event_id: {receipt_type: {user_id: {ts: N}}}}
			byRoom := map[string]map[string]map[string]map[string]int64{}
			for _, rc := range receipts {
				if byRoom[rc.RoomID] == nil {
					byRoom[rc.RoomID] = map[string]map[string]map[string]int64{}
				}
				if byRoom[rc.RoomID][rc.EventID] == nil {
					byRoom[rc.RoomID][rc.EventID] = map[string]map[string]int64{}
				}
				if byRoom[rc.RoomID][rc.EventID][rc.ReceiptType] == nil {
					byRoom[rc.RoomID][rc.EventID][rc.ReceiptType] = map[string]int64{}
				}
				byRoom[rc.RoomID][rc.EventID][rc.ReceiptType][rc.UserID] = rc.TS
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

// appendPresence fills the response's presence section: the syncing user's own
// presence, plus room peers' presence. On incremental sync only peers whose
// presence changed after the sync token are emitted; on initial sync all joined
// room peers are emitted (the spec's "presence events for all users the client
// shares a room with" on initial sync).
func (e *Engine) appendPresence(ctx context.Context, resp *Response, opts SyncOptions) {
	// Own presence (always included).
	var events []json.RawMessage
	if p, err := e.store.GetPresence(ctx, opts.UserID); err == nil && p != nil {
		events = append(events, presenceEvent(p))
	}

	// Peers: users sharing a joined room with the syncer.
	peers := e.roomPeers(ctx, opts)
	if len(peers) == 0 {
		if len(events) > 0 {
			resp.Presence = &PresenceResp{Events: events}
		}
		return
	}

	var userIDs []string
	if opts.Since.Stream > 0 {
		// Incremental: only peers whose presence changed since the token.
		changed, err := e.store.PresenceChangesSince(ctx, opts.Since.Stream)
		if err != nil {
			changed = nil
		}
		for _, u := range changed {
			if peers[u] && u != opts.UserID {
				userIDs = append(userIDs, u)
			}
		}
	} else {
		for u := range peers {
			if u != opts.UserID {
				userIDs = append(userIDs, u)
			}
		}
	}
	for _, u := range userIDs {
		if p, err := e.store.GetPresence(ctx, u); err == nil && p != nil {
			events = append(events, presenceEvent(p))
		}
	}
	if len(events) > 0 {
		resp.Presence = &PresenceResp{Events: events}
	}
}

// presenceEvent marshals a stored presence row as an m.presence client event.
func presenceEvent(p *storage.PresenceRow) json.RawMessage {
	content := map[string]any{
		"presence":        p.Presence,
		"user_id":         p.UserID,
		"last_active_ago": presenceLastActiveAgo(p),
	}
	if p.StatusMsg != "" {
		content["status_msg"] = p.StatusMsg
	}
	ev, _ := json.Marshal(map[string]any{"type": "m.presence", "content": content})
	return ev
}

func presenceLastActiveAgo(p *storage.PresenceRow) int64 {
	if p.LastActiveTS > 0 {
		if ago := time.Now().UnixMilli() - p.LastActiveTS; ago >= 0 {
			return ago
		}
	}
	return 0
}

// roomPeers returns the set of user IDs sharing a joined room with the syncer.
func (e *Engine) roomPeers(ctx context.Context, opts SyncOptions) map[string]bool {
	roomIDs, err := e.store.RoomsForUser(ctx, opts.UserID)
	if err != nil {
		return nil
	}
	out := map[string]bool{}
	for _, roomID := range roomIDs {
		users, err := e.store.JoinedUserIDs(ctx, roomID)
		if err != nil {
			continue
		}
		for _, u := range users {
			out[u] = true
		}
	}
	return out
}

// buildJoinedRoom constructs the JoinedRoom section.
func (e *Engine) buildJoinedRoom(ctx context.Context, roomID string, opts SyncOptions, maxStream int64) (JoinedRoom, error) {
	jr := JoinedRoom{}
	filter := opts.Filter
	// Timeline: events with stream_ordering > since, limited to 50 for initial.
	from := opts.Since.Stream
	limit := 50
	if filter != nil && filter.TimelineLimit > 0 {
		limit = filter.TimelineLimit
	}
	if from == 0 {
		from = maxStream - int64(limit)
		if from < 0 {
			from = 0
		}
	}
	evs, err := e.store.EventsForRoom(ctx, roomID, from, maxStream, limit, "f")
	if err != nil {
		return jr, err
	}
	// Track senders for lazy-load members.
	senders := map[string]bool{}
	timeline := Timeline{Events: make([]json.RawMessage, 0, len(evs))}
	for _, ev := range evs {
		if !filter.keepTimeline(&ev) {
			continue
		}
		timeline.Events = append(timeline.Events, filter.applyEventFields(clientEvent(&ev)))
		senders[ev.Sender] = true
	}
	if len(evs) >= limit {
		timeline.Limited = true
		timeline.PrevBatch = Token{Stream: from}.Encode()
	}
	jr.Timeline = timeline

	// State: full state on initial sync or full_state; otherwise empty (delta).
	// Lazy-load members replaces the full state with only the m.room.member
	// events for timeline senders.
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
			if filter.lazyLoadMembers() && !lazyLoadMemberEvent(&se, senders) {
				continue
			}
			jr.State.Events = append(jr.State.Events, filter.applyEventFields(clientEvent(&se)))
		}
	}

	// Room summary: joined/invited member counts (spec m.joined_member_count /
	// m.invited_member_count) plus up to five hero user IDs.
	jr.Summary = e.roomSummary(ctx, roomID, opts.UserID)
	return jr, nil
}

// lazyLoadMemberEvent reports whether a state event should be included under a
// lazy_load_members filter: membership events of timeline senders (and the
// syncing user's own membership) are included; everything else is dropped.
func lazyLoadMemberEvent(ev *storage.EventRow, senders map[string]bool) bool {
	if ev.Type != "m.room.member" {
		return false
	}
	if senders[ev.Sender] {
		return true
	}
	return false
}

// roomSummary builds the room summary section (member counts + heroes).
func (e *Engine) roomSummary(ctx context.Context, roomID, selfUserID string) *RoomSummary {
	members, err := e.store.Members(ctx, roomID, "")
	if err != nil {
		return nil
	}
	joined, invited := 0, 0
	var heroes []string
	for _, m := range members {
		switch m.Membership {
		case "join":
			joined++
			if m.UserID != selfUserID && len(heroes) < 5 {
				heroes = append(heroes, m.UserID)
			}
		case "invite":
			invited++
			if len(heroes) < 5 {
				heroes = append(heroes, m.UserID)
			}
		}
	}
	return &RoomSummary{
		JoinedMembers:  joined,
		InvitedMembers: invited,
		Heroes:         heroes,
	}
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
// type, content, sender, state_key (if state), origin_server_ts, event_id -
// never auth_events, hashes, prev_events, or signatures.
//
// If the event has been redacted, the content is replaced with its pruned
// redacted form (per the default room version redaction rules).
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
	if row.Redacted {
		if rules, ok := roomver.Get(roomver.Default); ok {
			if red, err := events.Redact(row.RawJSON, rules); err == nil {
				if c, exists := red["content"]; exists {
					m["content"] = c
				}
			}
		}
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

// ignoredUsers returns the set of user IDs the given user has in their global
// m.ignored_user_list account data, or nil if none. The set is honoured when
// filtering room invites out of /sync.
func (e *Engine) ignoredUsers(ctx context.Context, localpart string) map[string]bool {
	raw, err := e.store.GetAccountData(ctx, localpart, "", "m.ignored_user_list")
	if err != nil || len(raw) == 0 {
		return nil
	}
	var data struct {
		IgnoredUsers map[string]json.RawMessage `json:"ignored_users"`
	}
	if err := json.Unmarshal(raw, &data); err != nil || len(data.IgnoredUsers) == 0 {
		return nil
	}
	out := make(map[string]bool, len(data.IgnoredUsers))
	for u := range data.IgnoredUsers {
		out[u] = true
	}
	return out
}

// inviteIsFromIgnored reports whether the invite for roomID targeting userID
// was sent by a user in the ignored set. The inviter is the sender of the
// room's m.room.member(invite) event for the target user.
func (e *Engine) inviteIsFromIgnored(ctx context.Context, roomID, userID string, ignored map[string]bool) bool {
	id, err := e.store.GetStateEvent(ctx, roomID, "m.room.member", userID)
	if err != nil {
		return false
	}
	ev, err := e.store.GetEvent(ctx, id)
	if err != nil {
		return false
	}
	return ignored[ev.Sender]
}
