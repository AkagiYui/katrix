package sync

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"time"

	"github.com/AkagiYui/katrix/internal/events"
	"github.com/AkagiYui/katrix/internal/eventstate"
	"github.com/AkagiYui/katrix/internal/pushrules"
	"github.com/AkagiYui/katrix/internal/roomver"
	"github.com/AkagiYui/katrix/internal/storage"
)

// Response is the /sync response body.
type Response struct {
	NextBatch                    string         `json:"next_batch"`
	Rooms                        RoomsResp      `json:"rooms"`
	Presence                     *PresenceResp  `json:"presence,omitempty"`
	AccountData                  *EventsSection `json:"account_data,omitempty"`
	ToDevice                     *EventsSection `json:"to_device,omitempty"`
	DeviceLists                  *DeviceLists   `json:"device_lists,omitempty"`
	DeviceOneTimeKeysCount       map[string]int `json:"device_one_time_keys_count,omitempty"`
	DeviceUnusedFallbackKeyTypes *[]string      `json:"device_unused_fallback_key_types,omitempty"`
	// ProfileUpdates (MSC4429) carries profile-field changes for room peers,
	// keyed by user ID -> {"profile_updates": {field: value}}. Only populated
	// when the filter's profile_fields.ids is non-empty.
	ProfileUpdates map[string]ProfileUpdatesForUser `json:"org.matrix.msc4429.users,omitempty"`
}

// ProfileUpdatesForUser is one user's profile-update payload in the sync
// response (MSC4429): a null profile_updates tells the client to clear its
// cached profile for that user (e.g. the user left the last shared room).
type ProfileUpdatesForUser struct {
	ProfileUpdates map[string]json.RawMessage `json:"profile_updates"`
}

// hasDeltas reports whether an incremental response carries any content beyond
// the empty baseline: room timeline/state/leave/invite changes, account data,
// presence (of peers — the user's own presence is always echoed), to-device
// messages or device-list updates. The long-poll loop uses this to decide
// whether to park: a response whose NextBatch token has not moved may still
// carry real data (notably to-device messages, which are not tied to the
// shared event stream), and such a response must be delivered immediately
// rather than held for the next notify.
//
// A joined room is always present in rooms.join even with nothing new (its
// timeline and state are empty arrays), and state_after is always populated
// for use_state_after clients, so only rooms with non-empty timeline, state,
// account_data or ephemeral content count as a delta.
func (r *Response) HasDeltas(userID string) bool {
	if len(r.Rooms.Invite) > 0 || len(r.Rooms.Leave) > 0 || len(r.Rooms.Peek) > 0 {
		return true
	}
	for _, jr := range r.Rooms.Join {
		if len(jr.Timeline.Events) > 0 {
			return true
		}
		if len(jr.State.Events) > 0 {
			return true
		}
		if jr.Ephemeral != nil && len(jr.Ephemeral.Events) > 0 {
			return true
		}
		if len(jr.AccountData.Events) > 0 {
			return true
		}
	}
	if r.Presence != nil {
		for _, ev := range r.Presence.Events {
			var obj map[string]json.RawMessage
			if json.Unmarshal(ev, &obj) != nil {
				continue
			}
			var sender string
			_ = json.Unmarshal(obj["sender"], &sender)
			// The user's own presence is always echoed and is not a delta.
			if sender != userID {
				return true
			}
		}
	}
	if r.AccountData != nil && len(r.AccountData.Events) > 0 {
		return true
	}
	if r.ToDevice != nil && len(r.ToDevice.Events) > 0 {
		return true
	}
	if r.DeviceLists != nil && (len(r.DeviceLists.Changed) > 0 || len(r.DeviceLists.Left) > 0) {
		return true
	}
	return false
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
	// Knock (MSC2409): rooms the user has knocked on. The knock is delivered
	// like an invite but under `knock_state` (the room's stripped state).
	Knock map[string]KnockedRoom `json:"knock,omitempty"`
	Leave map[string]LeftRoom    `json:"leave,omitempty"`
	// Peek (MSC2753): rooms the calling device has peeks into without joining
	// (world_readable only). Peeked rooms appear only in the sync of the device
	// that peeked them, never in other devices' or users' syncs.
	Peek map[string]JoinedRoom `json:"peek,omitempty"`
}

// JoinedRoom is a room the user has joined.
type JoinedRoom struct {
	Timeline    Timeline          `json:"timeline"`
	State       StateSet          `json:"state"`
	StateAfter  *StateSet         `json:"state_after,omitempty"`
	AccountData StateSet          `json:"account_data,omitempty"`
	Ephemeral   *EphemeralSection `json:"ephemeral,omitempty"`
	Summary     *RoomSummary      `json:"summary,omitempty"`
	// Unread notifications (spec unread_notifications; MSC3774 thread counts).
	UnreadNotifications *UnreadNotifications           `json:"unread_notifications,omitempty"`
	UnreadThreads       map[string]UnreadNotifications `json:"unread_thread_notifications,omitempty"`
}

// UnreadNotifications carries a user's unread notification counts for a room
// timeline (main timeline, or a single thread under unread_thread_notifications).
type UnreadNotifications struct {
	NotificationCount *int `json:"notification_count,omitempty"`
	HighlightCount    *int `json:"highlight_count,omitempty"`
	// UnreadCount (MSC2625): the number of events matched by a push rule whose
	// actions include org.matrix.msc2625.mark_unread, since the user's read
	// position. Rendered as org.matrix.msc2625.unread_count.
	UnreadCount *int `json:"org.matrix.msc2625.unread_count,omitempty"`
}

// EphemeralSection holds the ephemeral events for a joined room. The spec
// nests them under an `events` array (rooms.join.<room_id>.ephemeral.events),
// so the section is an object rather than a bare array.
type EphemeralSection struct {
	Events []json.RawMessage `json:"events"`
}

// RoomSummary carries the room's member counts (joined + invited) and, for
// rooms without a name or canonical alias, up to five hero user IDs. The
// counts are always present — including 0 — per the spec (sytest asserts an
// empty invited count appears as 0, not an absent key).
type RoomSummary struct {
	JoinedMembers  int      `json:"m.joined_member_count"`
	InvitedMembers int      `json:"m.invited_member_count"`
	Heroes         []string `json:"m.heroes,omitempty"`
}

// InvitedRoom is a room the user is invited to.
type InvitedRoom struct {
	InviteState StateSet `json:"invite_state"`
}

// KnockedRoom is a room the user has knocked on (MSC2409). The state is
// delivered under `knock_state` (mirroring the invite section's `invite_state`).
type KnockedRoom struct {
	KnockState StateSet `json:"knock_state"`
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
	// Events is rendered unconditionally (even empty): clients rely on the
	// `state.events` / `account_data.events` arrays existing in every joined
	// room, and Complement's JSONArrayEach matches require the key present.
	Events []json.RawMessage `json:"events"`
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
	UserID        string
	Localpart     string
	DeviceID      string
	Since         Token
	Timeout       time.Duration
	FullState     bool
	UseStateAfter bool
	SetPresence   string
	Filter        *SyncFilter
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
	// TimelineLimitSet distinguishes an explicit `limit: 0` (empty timeline)
	// from an unset limit (default).
	TimelineLimitSet bool
	// TimelineTypesSet distinguishes a present-but-empty `types: []` (no type
	// matches → the timeline is blocked entirely) from an absent list (no
	// restriction). The spec's RoomEventFilter treats `types: []` as "filters
	// all types" (sytest "A change to displayname should not result in a full
	// state sync" filters the timeline with `types: []` and expects nothing
	// down the timeline).
	TimelineTypesSet bool
	// Room state filters (spec room.state): applied to the state section. Each
	// XxxSet flag distinguishes a present-but-empty list (`types: []` means no
	// types match → nothing passes) from an absent list (no restriction).
	StateTypesSet      bool
	StateNotTypesSet   bool
	StateSendersSet    bool
	StateNotSendersSet bool
	StateTypes         []string
	StateNotTypes      []string
	StateSenders       []string
	StateNotSenders    []string
	// Lazy-load members: only include membership state events, and only for
	// senders present in the timeline.
	LazyLoadMembers bool
	// IncludeRedundantMembers (spec RoomEventFilter): when lazy-loading, re-send
	// the membership events of timeline senders even when they were already
	// delivered to this client (default: the server suppresses memberships it
	// has already sent — sytest "We do send redundant membership state across
	// incremental syncs if asked").
	IncludeRedundantMembers bool
	// UnreadThreadNotifications: when true, per-thread unread notification
	// counts are returned under unread_thread_notifications and the main
	// unread_notifications counts exclude thread events (MSC3773/MSC3774).
	UnreadThreadNotifications bool
	// IncludeLeave controls whether left rooms appear in the response.
	IncludeLeave bool
	// EventFields narrows the per-event fields returned (JSON pointer paths).
	EventFields []string
	// EventFormatFederation renders timeline/state events in the federation
	// format (the signed PDU shape with prev_events/auth_events/depth/hashes/
	// signatures and a room_id) instead of the default stripped client format
	// (filter.event_format: "federation", spec "Filtering").
	EventFormatFederation bool
	// Account-data filters (spec filter.account_data): applied to both the
	// global account_data section and every room's account_data section. A
	// present-but-empty `types: []` list matches nothing (the standard
	// RoomEventFilter semantics — sytest "Latest account data appears in v2
	// /sync" filters account_data.types=["my.test.type"] and expects exactly
	// that type and nothing else, so the default push-rules entry must be
	// filtered out).
	AccountDataTypesSet    bool
	AccountDataNotTypesSet bool
	AccountDataTypes       []string
	AccountDataNotTypes    []string
	// ProfileFields (MSC4429 filter.profile_fields.ids): when non-empty, the
	// response carries profile-update events for room peers under
	// org.matrix.msc4429.users, restricted to these field IDs. Absent or empty
	// means profile updates are not delivered.
	ProfileFields []string
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
				Limit      *int     `json:"limit"`
				// UnreadThreadNotifications (MSC3773): per-thread unread counts.
				UnreadThreadNotifications bool `json:"unread_thread_notifications"`
			} `json:"timeline"`
			State struct {
				Types           []string `json:"types"`
				NotTypes        []string `json:"not_types"`
				Senders         []string `json:"senders"`
				NotSenders      []string `json:"not_senders"`
				LazyLoadMembers bool     `json:"lazy_load_members"`
				// IncludeRedundantMembers lives under room.state in the spec's
				// RoomEventFilter schema (it is a sibling of lazy_load_members).
				IncludeRedundantMembers bool `json:"include_redundant_members"`
			} `json:"state"`
			IncludeLeave bool `json:"include_leave"`
		} `json:"room"`
		EventFields []string `json:"event_fields"`
		// event_format: "client" (default) or "federation" (spec Filtering).
		EventFormat string `json:"event_format"`
		AccountData struct {
			Types    []string `json:"types"`
			NotTypes []string `json:"not_types"`
		} `json:"account_data"`
		ProfileFields struct {
			IDs []string `json:"ids"`
		} `json:"profile_fields"`
	}
	if err := json.Unmarshal(raw, &obj); err != nil {
		return nil
	}
	f := &SyncFilter{
		TimelineTypes:             obj.Room.Timeline.Types,
		TimelineNotTypes:          obj.Room.Timeline.NotTypes,
		TimelineSenders:           obj.Room.Timeline.Senders,
		TimelineNotSenders:        obj.Room.Timeline.NotSenders,
		TimelineTypesSet:          obj.Room.Timeline.Types != nil,
		StateTypes:                obj.Room.State.Types,
		StateTypesSet:             obj.Room.State.Types != nil,
		StateNotTypes:             obj.Room.State.NotTypes,
		StateNotTypesSet:          obj.Room.State.NotTypes != nil,
		StateSenders:              obj.Room.State.Senders,
		StateSendersSet:           obj.Room.State.Senders != nil,
		StateNotSenders:           obj.Room.State.NotSenders,
		StateNotSendersSet:        obj.Room.State.NotSenders != nil,
		LazyLoadMembers:           obj.Room.State.LazyLoadMembers,
		IncludeRedundantMembers:   obj.Room.State.IncludeRedundantMembers,
		UnreadThreadNotifications: obj.Room.Timeline.UnreadThreadNotifications,
		IncludeLeave:              obj.Room.IncludeLeave,
		EventFields:               obj.EventFields,
		EventFormatFederation:     obj.EventFormat == "federation",
		AccountDataTypes:          obj.AccountData.Types,
		AccountDataTypesSet:       obj.AccountData.Types != nil,
		AccountDataNotTypes:       obj.AccountData.NotTypes,
		AccountDataNotTypesSet:    obj.AccountData.NotTypes != nil,
		ProfileFields:             obj.ProfileFields.IDs,
	}
	if obj.Room.Timeline.Limit != nil {
		f.TimelineLimit = *obj.Room.Timeline.Limit
		f.TimelineLimitSet = true
	}
	// A filter with nothing set behaves like no filter.
	if !f.anySet() {
		return nil
	}
	return f
}

func (f *SyncFilter) anySet() bool {
	return f.TimelineLimitSet || f.TimelineLimit > 0 || f.LazyLoadMembers || f.IncludeRedundantMembers || f.UnreadThreadNotifications || f.IncludeLeave ||
		len(f.TimelineTypes) > 0 || len(f.TimelineNotTypes) > 0 || f.TimelineTypesSet ||
		len(f.TimelineSenders) > 0 || len(f.TimelineNotSenders) > 0 ||
		f.StateTypesSet || f.StateNotTypesSet || f.StateSendersSet || f.StateNotSendersSet ||
		len(f.EventFields) > 0 ||
		f.EventFormatFederation ||
		f.AccountDataTypesSet || f.AccountDataNotTypesSet ||
		len(f.AccountDataTypes) > 0 || len(f.AccountDataNotTypes) > 0 ||
		len(f.ProfileFields) > 0
}

// keepAccountData reports whether an account_data row passes the filter's
// account_data section (spec filter.account_data: a RoomEventFilter applied to
// both the global and per-room account_data sections). A present-but-empty
// `types: []` matches nothing (standard RoomEventFilter semantics).
func (f *SyncFilter) keepAccountData(eventType string) bool {
	if f == nil {
		return true
	}
	if f.AccountDataNotTypesSet && strIn(f.AccountDataNotTypes, eventType) {
		return false
	}
	if f.AccountDataTypesSet && !strIn(f.AccountDataTypes, eventType) {
		return false
	}
	return true
}

// keepState reports whether a room state event passes the filter's state
// section rules (spec room.state.types/not_types/senders/not_senders). A
// filter with no state restrictions keeps everything. A present-but-empty list
// (`types: []`) is a restriction: no type matches it, so nothing passes
// (sytest "Can pass a JSON filter as a query parameter" filters
// state.types=["m.room.member"] and asserts exactly one event; Synapse treats
// an empty list the same way).
func (f *SyncFilter) keepState(ev *storage.EventRow) bool {
	if f == nil {
		return true
	}
	// A not_types / not_senders list excludes events whose value is in it. The
	// list is a restriction whenever present (even empty: nothing is excluded,
	// but the field's presence is still valid).
	if f.StateNotTypesSet && strIn(f.StateNotTypes, ev.Type) {
		return false
	}
	if f.StateNotSendersSet && strIn(f.StateNotSenders, ev.Sender) {
		return false
	}
	if f.StateTypesSet && !strIn(f.StateTypes, ev.Type) {
		return false
	}
	if f.StateSendersSet && !strIn(f.StateSenders, ev.Sender) {
		return false
	}
	return true
}

// keepTimeline reports whether a timeline event passes the filter. A
// present-but-empty `types: []` list blocks everything (spec RoomEventFilter:
// an empty list matches no types); an absent list is no restriction. not_types
// and not_senders always restrict when present (an empty not_ list excludes
// nothing but is still a valid restriction).
func (f *SyncFilter) keepTimeline(ev *storage.EventRow) bool {
	if f == nil {
		return true
	}
	if strIn(f.TimelineNotTypes, ev.Type) {
		return false
	}
	if f.TimelineTypesSet && !strIn(f.TimelineTypes, ev.Type) {
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

// renderEvent renders a stored event row for a /sync response, honouring the
// filter's event_format: "client" renders the stripped client-visible event,
// "federation" renders the signed PDU shape (spec Filtering: "federation"
// returns the event in its federation format). A redacted row renders with its
// pruned content either way.
func (f *SyncFilter) renderEvent(row *storage.EventRow) json.RawMessage {
	if f != nil && f.EventFormatFederation {
		return federationEvent(row, row.Redacted)
	}
	return clientEvent(row)
}

// renderEventErased is renderEvent for a sender who has been erased (spec
// §Erasure): the content is always pruned regardless of the row's redaction
// state.
func (f *SyncFilter) renderEventErased(row *storage.EventRow) json.RawMessage {
	if f != nil && f.EventFormatFederation {
		return federationEvent(row, true)
	}
	return erasedClientEvent(row)
}

// federationEvent renders a stored PDU in the federation event format: the
// signed event with prev_events, auth_events, depth, hashes and signatures
// intact, plus a guaranteed room_id and event_id (v12+ events omit event_id in
// their raw form; the row carries it). A redacted/erased event uses its pruned
// content (mirror of clientEventCore).
func federationEvent(row *storage.EventRow, redact bool) json.RawMessage {
	raw := row.RawJSON
	if selfDestructed(row, time.Now().UnixMilli()) {
		redact = true
	}
	if redact {
		if rules, ok := roomver.Get(roomver.Default); ok {
			if red, err := events.Redact(raw, rules); err == nil {
				if b, err := json.Marshal(red); err == nil {
					raw = b
				}
			}
		}
	}
	var m map[string]json.RawMessage
	if err := json.Unmarshal(raw, &m); err != nil {
		// Fall back to the client format if the raw PDU cannot be parsed.
		return clientEventCore(row, redact)
	}
	// The row is authoritative for the IDs (v12 raw events carry no event_id,
	// and the create event may omit room_id): stamp them so every federation
	// event has the fields the spec guarantees.
	id, _ := json.Marshal(row.EventID)
	m["event_id"] = id
	if rid, _ := json.Marshal(row.RoomID); row.RoomID != "" {
		m["room_id"] = rid
	}
	if row.StateKey != "" {
		if sk, _ := json.Marshal(row.StateKey); row.StateKey != "" {
			m["state_key"] = sk
		}
	}
	b, _ := json.Marshal(m)
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
	rooms := RoomsResp{
		Join:   map[string]JoinedRoom{},
		Invite: map[string]InvitedRoom{},
		Knock:  map[string]KnockedRoom{},
		Leave:  map[string]LeftRoom{},
		Peek:   map[string]JoinedRoom{},
	}

	// Rooms the user has joined.
	joinedRoomIDs, err := e.store.RoomsForUser(ctx, opts.UserID)
	if err != nil {
		return nil, err
	}
	// Users whose events the syncer ignores (spec m.ignored_user_list): their
	// timeline and state events are excluded from the sync, mirroring how the
	// filter's room rules exclude events.
	ignored := e.ignoredUsers(ctx, opts.Localpart)
	for _, roomID := range joinedRoomIDs {
		// A partial-state room (MSC3902) is omitted from eager /sync responses
		// until its background resync completes: the full room state is not
		// available yet. Lazy-loading syncs (which only need the joining user's
		// own membership from the critical state) include it immediately. An
		// eager sync waits for an in-flight resync to complete (the resync is a
		// few state/event fetches away) so a client that syncs right after a
		// partial join sees the room once the state is there; a room whose
		// resync is still blocked after the wait is omitted. The wait is
		// generous because the resync is a network round-trip that is routinely
		// slow under load — a fixed 500ms routinely drops the room for clients
		// that sync within the first second of a partial join, which is exactly
		// when they are most likely to sync (the join just completed).
		if room, err := e.store.GetRoom(ctx, roomID); err == nil && room.PartialState {
			if opts.Filter == nil || !opts.Filter.LazyLoadMembers {
				if e.waitForResync(ctx, roomID, 5*time.Second) {
					// resync completed while we waited; include the room
				} else {
					continue
				}
			}
		}
		jr, err := e.buildJoinedRoom(ctx, roomID, opts, maxStream, ignored)
		if err != nil {
			return nil, err
		}
		rooms.Join[roomID] = jr
	}

	// Rooms the user is invited to (membership=invite).
	invited, err := e.store.InvitedRooms(ctx, opts.UserID)
	if err == nil {
		for _, roomID := range invited {
			// MSC4155 + m.ignored_user_list: an invite whose sender (or sender's
			// server) the user has configured as ignored is accepted by the
			// server but must not be delivered in /sync.
			if e.inviteIsHidden(ctx, roomID, opts.UserID, opts.Localpart, ignored) {
				continue
			}
			rooms.Invite[roomID] = e.buildInvitedRoom(ctx, roomID)
		}
	}

	// Rooms the user has knocked on (membership=knock, MSC2409). Knock rooms are
	// delivered like invites but under `knock` with a `knock_state` section.
	knocked, err := e.store.KnockedRooms(ctx, opts.UserID)
	if err == nil {
		for _, roomID := range knocked {
			rooms.Knock[roomID] = e.buildKnockedRoom(ctx, roomID)
		}
	}

	// Left rooms (membership=leave/ban) - include recent timeline once.
	// Only rooms left after the sync token are reported incrementally (a leave
	// already delivered must not reappear); forgotten rooms are hidden on
	// initial sync but still reported incrementally so other devices learn the
	// room was left.
	leftRooms, _ := e.store.LeftRooms(ctx, opts.UserID, opts.Since.Stream, opts.Since.Stream > 0)
	if leftRooms != nil {
		for _, roomID := range leftRooms {
			lr, err := e.buildLeftRoom(ctx, roomID, opts, maxStream, ignored)
			if err != nil {
				continue
			}
			rooms.Leave[roomID] = lr
		}
	}

	// Peeked rooms (MSC2753): world_readable rooms this device peeks into
	// without joining. They are scoped to the device, so each device's /sync
	// carries only its own peeks. A peeked room the user has since joined is
	// delivered via the join section instead (the peek is subsumed).
	peeked, _ := e.store.PeekedRooms(ctx, opts.UserID, opts.DeviceID)
	for _, roomID := range peeked {
		if _, joined := rooms.Join[roomID]; joined {
			continue
		}
		pr, err := e.buildPeekedRoom(ctx, roomID, opts, maxStream)
		if err != nil || pr == nil {
			continue
		}
		rooms.Peek[roomID] = *pr
	}

	resp.Rooms = rooms

	// Account data (global + per-room). The filter's account_data section
	// applies to both (spec filter.account_data): a client asking for only
	// certain types sees those and nothing else — the default push-rules
	// account-data entry is filtered out just like a timeline filter drops
	// non-matching events.
	adRows, err := e.store.AccountDataSince(ctx, opts.Localpart, opts.Since.Stream)
	if err == nil {
		globalAD := StateSet{Events: []json.RawMessage{}}
		roomAD := map[string]StateSet{}
		for _, a := range adRows {
			// A deleted entry is a tombstone (empty content). It must appear in
			// incremental sync (as the delete signal, MSC3391) but never in an
			// initial sync — a freshly-syncing client must not see deleted types.
			if opts.Since.Stream == 0 && storage.IsAccountDataDeleted(a.Content) {
				continue
			}
			if !opts.Filter.keepAccountData(a.Type) {
				continue
			}
			ev := mustMarshalEvent(a.Type, "", "", a.Content, 0, "")
			if a.RoomID == "" {
				globalAD.Events = append(globalAD.Events, ev)
			} else {
				r := roomAD[a.RoomID]
				if r.Events == nil {
					r.Events = []json.RawMessage{}
				}
				r.Events = append(r.Events, ev)
				roomAD[a.RoomID] = r
			}
		}
		// A joined room always carries an account_data section (empty when no
		// account data is set): clients rely on the array existing.
		for roomID := range rooms.Join {
			if _, ok := roomAD[roomID]; !ok {
				roomAD[roomID] = StateSet{Events: []json.RawMessage{}}
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

	// Profile updates (MSC4429): delivered only when the filter's
	// profile_fields.ids is non-empty. On an incremental sync the response
	// carries the fields that changed since the token (the latest value per
	// user+field); on an initial sync it carries the current values of the
	// requested fields for every room peer (mirror of Synapse's
	// _generate_initial_sync_entry_for_profile_updates).
	if opts.Filter != nil && len(opts.Filter.ProfileFields) > 0 {
		resp.ProfileUpdates = e.profileUpdates(ctx, opts, opts.Filter.ProfileFields)
	}

	// Device list changes for the syncing user. Per the spec, the server only
	// reports users whose device lists changed AND who share a room with the
	// syncing user (plus the user's own device list, which surfaces on their
	// other devices). The `left` list is the exception: it reports users who
	// just stopped sharing a room with the syncing user, so it passes through
	// unfiltered (filtering it by current room membership would drop exactly the
	// users it exists to report).
	//
	// Additionally, users who *newly* share a room with the syncer (either the
	// syncer joined one of their rooms, or they joined one of the syncer's
	// rooms, after the token) are reported in `changed` even when their device
	// list itself did not change: their devices became newly-visible (spec
	// "users who now share an encrypted room").
	//
	// Device-list changes are recorded from the authoritative m.device_list_update
	// EDUs (which the joining server broadcasts for its users, and the room's
	// resident servers broadcast for theirs when a new user joins), so a remote
	// user's join is reported once via their EDU record. A membership-based
	// "newly shared" fallback is consulted only when the window has no direct
	// change records (e.g. an EDU lost across a server restart): running both
	// would surface the peer twice, because the membership row and the EDU
	// record advance the shared stream at different times.
	changed, left, _ := e.store.DeviceListChangesSince(ctx, opts.Since.Stream)
	peers := e.deviceListPeers(ctx, opts)
	peers[opts.UserID] = true // always see your own device-list changes
	seen := map[string]bool{}
	var ch []string
	for _, u := range changed {
		// A user may appear more than once in the change stream (a join
		// records a change both via the send_join path and via the peer's
		// m.device_list_update EDU): report each user once.
		if peers[u] && !seen[u] {
			seen[u] = true
			ch = append(ch, u)
		}
	}
	// Users whose keys the syncer signed (POST /keys/signatures/upload) appear
	// in the syncer's OWN `changed` list: uploading signatures of another user
	// means the signer must re-fetch that user's keys (mirror of Synapse's
	// get_users_whose_signatures_changed in generate_sync_entry_for_device_list;
	// sytest "Changing user-signing key notifies local users"). The signed
	// targets share a room by construction (they were reached via the signer's
	// peer set), so the peer filter passes them.
	if opts.Since.Stream > 0 {
		if signed, err := e.store.SignatureTargetsSince(ctx, opts.UserID, opts.Since.Stream); err == nil {
			for _, u := range signed {
				if peers[u] && !seen[u] {
					seen[u] = true
					ch = append(ch, u)
				}
			}
		}
	}
	// Newly-shared room members, added to `changed`. The membership-based
	// "newly shared an encrypted room" fallback fires when either (a) the
	// syncer themselves newly joined a room after the token — every joined
	// member of that room, and the syncer's own ID, becomes newly-visible — or
	// (b) the window had no direct change records at all (an EDU was lost, e.g.
	// across a server restart) and a peer newly joined. It does NOT fire on
	// windows that already carry direct changes from an unrelated user: a
	// peer whose join was already reported via their EDU must not be re-listed
	// from the membership row (the two advance the shared stream at different
	// times, so the membership row can still postdate the token).
	syncerJoined := false
	var newPeers []string
	if opts.Since.Stream > 0 {
		roomIDs, _ := e.store.RoomsForUser(ctx, opts.UserID)
		if np, err := e.store.NewRoomPeersSince(ctx, roomIDs, opts.Since.Stream, opts.UserID); err == nil {
			newPeers = np
			for _, u := range np {
				if u == opts.UserID {
					syncerJoined = true
					break
				}
			}
		}
	}
	if len(ch) == 0 && len(left) == 0 && len(newPeers) > 0 && !syncerJoined {
		// (b) no direct changes: a peer joined and no EDU record arrived.
		for _, u := range newPeers {
			if peers[u] && !seen[u] {
				seen[u] = true
				ch = append(ch, u)
			}
		}
	}
	if syncerJoined {
		// (a) the syncer newly joined a room: the room's members and the
		// syncer's own ID are newly-visible.
		for _, u := range newPeers {
			if peers[u] && !seen[u] {
				seen[u] = true
				ch = append(ch, u)
			}
		}
	}
	// Users who stopped sharing a room with the syncer (a joined member left
	// or was banned after the token) must be reported in `left`: their device
	// lists are no longer being tracked. The membership deltas are the
	// authoritative source (a remote user's leave is never recorded as a
	// device-list change), so these are appended to the recorded `left` list
	// with dedup (a local leave records the change AND the membership delta).
	if opts.Since.Stream > 0 {
		roomIDs2, _ := e.store.RoomsForUser(ctx, opts.UserID)
		if nl, err := e.store.NewLeftPeersSince(ctx, roomIDs2, opts.Since.Stream); err == nil {
			for _, u := range nl {
				dup := false
				for _, lu := range left {
					if lu == u {
						dup = true
						break
					}
				}
				if !dup {
					left = append(left, u)
				}
			}
		}
	}
	if len(ch) > 0 || len(left) > 0 {
		resp.DeviceLists = &DeviceLists{Changed: ch, Left: left}
	}

	// Ephemeral: typing + receipts. Collected as a section object whose
	// `events` array carries the ephemeral events (spec shape
	// rooms.join.<room_id>.ephemeral.events).
	for roomID := range rooms.Join {
		jr := rooms.Join[roomID]
		// Empty (not nil) so the always-present section serialises as "events":
		// [] rather than "events": null.
		ephEvents := make([]json.RawMessage, 0)
		// Typing ephemeral. A stop-typing leaves an empty user_ids list, which
		// is itself a notification (spec + clients expect the empty EDU).
		typingUsers := e.typing.TypingUsers(roomID)
		if len(typingUsers) > 0 {
			users := make([]string, len(typingUsers))
			copy(users, typingUsers)
			eph, _ := json.Marshal(map[string]any{
				"type":    "m.typing",
				"content": map[string]any{"user_ids": users},
			})
			ephEvents = append(ephEvents, eph)
		} else if e.typing.RecentStop(roomID) {
			eph, _ := json.Marshal(map[string]any{
				"type":    "m.typing",
				"content": map[string]any{"user_ids": []string{}},
			})
			ephEvents = append(ephEvents, eph)
		}
		// Receipts ephemeral (since-based). Include all users' receipts for
		// rooms the syncer is joined to.
		receipts, _ := e.store.ReceiptsSince(ctx, opts.UserID, opts.Since.Stream)
		if len(receipts) > 0 {
			// Build content: {event_id: {receipt_type: {user_id: {ts, thread_id?}}}}
			// (spec + MSC3773: a threaded read receipt carries the thread root id
			// under thread_id; the unthreaded form omits it).
			//
			// MSC4102: when a user has both an unthreaded and a threaded receipt
			// for the same event, the unthreaded receipt must be the one emitted —
			// it is the stronger signal (it reads every timeline, including the
			// thread). The receipts arrive stream-ordered (a later unthreaded
			// receipt supersedes an earlier threaded one), so an existing
			// unthreaded entry is never overwritten by a threaded one.
			byRoom := map[string]map[string]map[string]map[string]any{}
			for _, rc := range receipts {
				if byRoom[rc.RoomID] == nil {
					byRoom[rc.RoomID] = map[string]map[string]map[string]any{}
				}
				if byRoom[rc.RoomID][rc.EventID] == nil {
					byRoom[rc.RoomID][rc.EventID] = map[string]map[string]any{}
				}
				if byRoom[rc.RoomID][rc.EventID][rc.ReceiptType] == nil {
					byRoom[rc.RoomID][rc.EventID][rc.ReceiptType] = map[string]any{}
				}
				existing := byRoom[rc.RoomID][rc.EventID][rc.ReceiptType][rc.UserID]
				if existing != nil && rc.ThreadID != "" {
					// An unthreaded receipt already occupies the key; a threaded
					// receipt for the same event must not displace it (MSC4102).
					continue
				}
				userObj := map[string]any{"ts": rc.TS}
				if rc.ThreadID != "" {
					userObj["thread_id"] = rc.ThreadID
				}
				byRoom[rc.RoomID][rc.EventID][rc.ReceiptType][rc.UserID] = userObj
			}
			if evMap, ok := byRoom[roomID]; ok {
				eph, _ := json.Marshal(map[string]any{
					"type":    "m.receipt",
					"content": evMap,
				})
				ephEvents = append(ephEvents, eph)
			}
		}
		// The ephemeral section is always present (possibly an empty events
		// array): the spec requires rooms.join.<room_id>.ephemeral.events, and
		// clients/tests rely on it (an omitted key is not the same as an empty
		// section).
		jr.Ephemeral = &EphemeralSection{Events: ephEvents}
		rooms.Join[roomID] = jr
	}

	// Incremental sync omits rooms that have not changed since the token: a
	// joined room with an empty timeline, no state, no account-data deltas and
	// no ephemeral content is not re-sent (spec: "rooms.join is a map of rooms
	// where the user is joined to, *and has been modified since the previous
	// sync*"). A full_state sync always re-sends the room's state, so it is
	// never dropped here (its state events count as a delta).
	if opts.Since.Stream > 0 && !opts.FullState {
		for roomID, jr := range rooms.Join {
			if len(jr.Timeline.Events) == 0 && len(jr.State.Events) == 0 &&
				(jr.Ephemeral == nil || len(jr.Ephemeral.Events) == 0) &&
				len(jr.AccountData.Events) == 0 {
				delete(rooms.Join, roomID)
			}
		}
	}

	// Next batch token is the current max stream (plus the device's to-device
	// cursor, which is carried across polls so a poll that delivers no to-device
	// messages does not reset the cursor).
	resp.NextBatch = Token{Stream: maxStream, ToDevice: opts.Since.ToDevice}.Encode()

	// Per-device E2EE key counts: how many unused one-time keys (and which
	// fallback key algorithms) the requesting device holds. Clients use the
	// one-time-key count to decide when to upload more keys, and the unused
	// fallback algorithms to decide when to (re)upload a fallback key.
	//
	// device_unused_fallback_key_types is emitted even when empty: its presence
	// advertises that the server supports fallback keys, which tells clients to
	// generate and upload one (spec: "If the list is empty, the client should
	// upload a fallback key").
	if opts.DeviceID != "" {
		if counts, err := e.store.OneTimeKeyCounts(ctx, opts.UserID, opts.DeviceID); err == nil && len(counts) > 0 {
			resp.DeviceOneTimeKeysCount = counts
		}
		algos, err := e.store.UnusedFallbackKeyAlgorithms(ctx, opts.UserID, opts.DeviceID)
		if err == nil {
			if len(algos) == 0 {
				algos = []string{}
			}
			resp.DeviceUnusedFallbackKeyTypes = &algos
		}
	}

	// To-device messages: deliver any queued for this device whose ID exceeds
	// the device's incoming cursor (the cursor travels in the since token).
	// Messages are retained until the device acknowledges them (its next sync
	// prunes up to this batch's cursor), so a client killed before processing a
	// /sync response gets the messages redelivered on restart (spec: to-device
	// messages must not be dropped between delivery and processing).
	tdCursor := opts.Since.ToDevice
	td, err := e.store.DequeueToDevice(ctx, opts.UserID, opts.DeviceID, tdCursor)
	if err == nil && len(td) > 0 {
		events := make([]json.RawMessage, 0, len(td))
		for _, m := range td {
			// Per the spec, a to-device event is {sender, type, content} and
			// nothing else — no origin_server_ts (sytest "Can recv a device
			// message using /sync" deep-compares the exact object).
			ev := mustMarshalEvent(m.Type, m.Sender, "", m.Content, 0, "")
			events = append(events, ev)
			if m.ID > tdCursor {
				tdCursor = m.ID
			}
		}
		resp.ToDevice = &EventsSection{Events: events}
		// The next_batch token carries the new to-device cursor so the client
		// does not redeliver this batch.
		resp.NextBatch = Token{Stream: maxStream, ToDevice: tdCursor}.Encode()
	}
	// Acknowledge the messages the client has already received: its incoming
	// cursor names the last delivered message, so everything at or below it was
	// processed (the client advanced its token). Prune those now; anything
	// above the cursor stays queued for redelivery.
	_ = e.store.PruneToDevice(ctx, opts.UserID, opts.DeviceID, opts.Since.ToDevice)
	return resp, nil
}

// appendPresence fills the response's presence section: the syncing user's own
// presence, plus room peers' presence. On incremental sync only peers whose
// presence changed after the sync token are emitted; on initial sync all joined
// room peers are emitted (the spec's "presence events for all users the client
// shares a room with" on initial sync). The user's OWN presence is treated the
// same way: it appears on initial sync, and on incremental sync only when it
// actually changed after the token (spec: the presence section carries events
// for users whose presence changed — echoing an unchanged own-presence on every
// poll would make /sync never quiesce; sytest's presence tests assert an empty
// presence section on the follow-up sync).
func (e *Engine) appendPresence(ctx context.Context, resp *Response, opts SyncOptions) {
	peers := e.roomPeers(ctx, opts)
	if len(peers) == 0 && opts.Since.Stream == 0 {
		// Initial sync with no room peers: still echo own presence. A user with
		// no presence row yet (never /sync'd, or joined via a path that does not
		// write one) is reported as online — the spec's /sync default.
		resp.Presence = &PresenceResp{Events: []json.RawMessage{presenceEventFor(ctx, e.store, opts.UserID)}}
		return
	}

	var events []json.RawMessage
	if opts.Since.Stream > 0 {
		// Incremental: peers whose presence changed since the token, plus peers
		// who newly share a room (they joined one of the syncer's rooms after
		// the token) — the shared-room relationship is new so their presence is
		// newly visible even if their presence row predates the token.
		changed, err := e.store.PresenceChangesSince(ctx, opts.Since.Stream)
		if err != nil {
			changed = nil
		}
		roomIDs, _ := e.store.RoomsForUser(ctx, opts.UserID)
		newPeers, err := e.store.NewRoomPeersSince(ctx, roomIDs, opts.Since.Stream, opts.UserID)
		if err != nil {
			newPeers = nil
		}
		seen := map[string]bool{}
		own := false
		for _, u := range append(changed, newPeers...) {
			if seen[u] {
				continue
			}
			seen[u] = true
			if u == opts.UserID {
				own = true
				continue
			}
			if !peers[u] {
				continue
			}
			userIDs := []string{u}
			events = append(events, presenceEvents(ctx, e.store, userIDs)...)
		}
		// Own presence only when it changed after the token (see doc comment).
		if own {
			events = append(events, presenceEventFor(ctx, e.store, opts.UserID))
		}
	} else {
		// Initial sync: own presence plus every joined room peer.
		events = append(events, presenceEventFor(ctx, e.store, opts.UserID))
		var userIDs []string
		for u := range peers {
			if u != opts.UserID {
				userIDs = append(userIDs, u)
			}
		}
		events = append(events, presenceEvents(ctx, e.store, userIDs)...)
	}
	if len(events) > 0 {
		resp.Presence = &PresenceResp{Events: events}
	}
}

// presenceEvents renders the stored presence rows for the given user IDs. A
// profileUpdates computes the MSC4429 profile-updates section. On an initial
// sync it carries the current values of the requested fields for every room
// peer (and the syncer); on an incremental sync it carries the fields that
// changed since the token (the latest value per user+field, filtered to the
// requested field IDs). Local users' profile fields come from the profile
// store; a user who no longer shares a room with the syncer (their leave was
// recorded in the window) is emitted as a null profile_updates so the client
// clears its cached profile (MSC4429; sytest "A user leaving the last shared
// room returns a profile update of null").
func (e *Engine) profileUpdates(ctx context.Context, opts SyncOptions, fields []string) map[string]ProfileUpdatesForUser {
	want := map[string]bool{}
	for _, f := range fields {
		want[f] = true
	}
	out := map[string]ProfileUpdatesForUser{}

	// Incremental: profile-field changes delivered to this receiver since the
	// token, keeping the latest value per user+field.
	if opts.Since.Stream > 0 {
		updates, err := e.store.ProfileUpdatesSince(ctx, opts.Localpart, opts.Since.Stream)
		if err != nil {
			return nil
		}
		latest := map[string]map[string]json.RawMessage{} // userID -> field -> value
		order := map[string]int64{}
		for _, u := range updates {
			if !want[u.Field] {
				continue
			}
			if _, ok := latest[u.UserID]; !ok {
				latest[u.UserID] = map[string]json.RawMessage{}
			}
			if u.StreamID >= order[u.UserID] {
				order[u.UserID] = u.StreamID
				latest[u.UserID][u.Field] = u.Value
			}
		}
		for uid, fields := range latest {
			out[uid] = ProfileUpdatesForUser{ProfileUpdates: fields}
		}
		// Users who stopped sharing a room with the syncer in the window: emit a
		// null profile_updates so the client clears their cached profile.
		roomIDs, _ := e.store.RoomsForUser(ctx, opts.UserID)
		if nl, err := e.store.NewLeftPeersSince(ctx, roomIDs, opts.Since.Stream); err == nil {
			for _, u := range nl {
				out[u] = ProfileUpdatesForUser{ProfileUpdates: nil}
			}
		}
		return out
	}

	// Initial sync: the current values of the requested fields for every room
	// peer and the syncer.
	peers := e.roomPeers(ctx, opts)
	for u := range peers {
		uid := u
		if !isLocalUserSync(uid, opts.UserID) {
			// Only local users' profiles are served (mirror of Synapse).
			continue
		}
		localpart := localpartOfSync(uid)
		pf, err := e.store.ProfileFields(ctx, localpart)
		if err != nil {
			continue
		}
		updates := map[string]json.RawMessage{}
		for f, v := range pf {
			if want[f] {
				updates[f] = v
			}
		}
		if len(updates) > 0 {
			out[uid] = ProfileUpdatesForUser{ProfileUpdates: updates}
		}
	}
	return out
}

// isLocalUserSync reports whether a user ID belongs to the same server as the
// given local user (a lightweight check used by the profile-updates initial
// sync, which serves local users only — mirror of Synapse's is_mine_id).
func isLocalUserSync(userID, self string) bool {
	di := -1
	for i := len(userID) - 1; i >= 0; i-- {
		if userID[i] == ':' {
			di = i
			break
		}
	}
	if di < 0 {
		return false
	}
	ds := -1
	for i := len(self) - 1; i >= 0; i-- {
		if self[i] == ':' {
			ds = i
			break
		}
	}
	return ds >= 0 && userID[di+1:] == self[ds+1:]
}

func localpartOfSync(userID string) string {
	for i := 0; i < len(userID); i++ {
		if userID[i] == ':' {
			return userID[1:i]
		}
	}
	return userID
}

// presenceEvents renders the stored presence rows for the given user IDs. A
// peer with no stored presence row (they have never synced with set_presence
// online, e.g. sytest's matrix_do_and_wait_for_sync always syncs offline) still
// has presence: the default state is "offline" with no status message (Synapse's
// UserPresenceState default). Emitting it matters because a newly-visible peer's
// presence must appear exactly once in the sync that makes them visible — a
// missing row must not silently drop the event.
func presenceEvents(ctx context.Context, store *storage.Store, userIDs []string) []json.RawMessage {
	var out []json.RawMessage
	for _, u := range userIDs {
		out = append(out, presenceEventFor(ctx, store, u))
	}
	return out
}

// presenceEventFor renders the m.presence event for one user, falling back to
// "online" when the user has no presence row yet. A user who exists but has
// never PUT /presence or /sync'd (or joined via a path that writes no row) is
// online per the spec's /sync default ("If this parameter is omitted then the
// client is automatically marked as online"), and a peer must still be able to
// learn it (sytest "New federated private chats get full presence information
// (SYN-115)").
func presenceEventFor(ctx context.Context, store *storage.Store, userID string) json.RawMessage {
	if p, err := store.GetPresence(ctx, userID); err == nil && p != nil {
		return presenceEvent(p)
	}
	return presenceEvent(&storage.PresenceRow{
		UserID:   userID,
		Presence: "online",
	})
}

// presenceEvent marshals a stored presence row as an m.presence client event.
// The event carries a top-level sender (the user whose presence changed), per
// the presence EDU shape the spec requires in /sync.
func presenceEvent(p *storage.PresenceRow) json.RawMessage {
	content := map[string]any{
		"presence":        p.Presence,
		"user_id":         p.UserID,
		"last_active_ago": presenceLastActiveAgo(p),
	}
	if p.StatusMsg != "" {
		content["status_msg"] = p.StatusMsg
	}
	ev, _ := json.Marshal(map[string]any{
		"type":    "m.presence",
		"sender":  p.UserID,
		"content": content,
	})
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

// deviceListPeers returns the set of user IDs whose device-list changes the
// syncer cares about: the joined members of the syncer's rooms plus users
// currently invited to them (an invite makes the invitee's devices newly
// relevant — sytest "uploading self-signing key notifies over federation"
// expects an invited remote user in device_lists.changed). This deliberately
// excludes membership beyond joined/invited, unlike roomPeers which drives
// presence (presence is only delivered for shared *joined* rooms).
func (e *Engine) deviceListPeers(ctx context.Context, opts SyncOptions) map[string]bool {
	roomIDs, err := e.store.RoomsForUser(ctx, opts.UserID)
	if err != nil {
		return nil
	}
	out := map[string]bool{}
	for _, roomID := range roomIDs {
		users, err := e.store.JoinedOrInvitedUserIDs(ctx, roomID)
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
// buildJoinedRoom constructs the JoinedRoom section. ignored is the syncing
// user's m.ignored_user_list: events sent by an ignored user are excluded from
// the timeline (spec "Ignoring Users"; state events are not filtered — the
// ignore list applies to timeline content only, mirror of Synapse's
// filter_events_for_client being applied to timeline events but not state).
func (e *Engine) buildJoinedRoom(ctx context.Context, roomID string, opts SyncOptions, maxStream int64, ignored map[string]bool) (JoinedRoom, error) {
	jr := JoinedRoom{}
	filter := opts.Filter
	// Timeline limit: the filter's per-room timeline limit, else 50 (the spec
	// default /current window). The window is the room's recent history — the
	// *last* `limit` events — not the first `limit` events after the token
	// (mirror of Synapse's _load_filtered_recents, which paginates backwards
	// from the newest event). An explicit `limit: 0` means an empty timeline
	// (spec: "limit: The maximum number of events to return. Defaults to 0,
	// which means all events are returned" — per-room this yields no events;
	// sytest "Can pass a JSON filter as a query parameter" asserts an empty
	// timeline for limit: 0).
	limit := 50
	if filter != nil && filter.TimelineLimitSet {
		limit = filter.TimelineLimit
	}
	if limit <= 0 {
		// An explicit timeline limit of 0 yields an empty timeline but the
		// state section still follows the filter's room.state rules (sytest
		// "Can pass a JSON filter as a query parameter" filters
		// state.types=["m.room.member"] with timeline.limit=0 and asserts the
		// single member event arrives in state.events).
		jr.Timeline = Timeline{Events: []json.RawMessage{}, Limited: false, PrevBatch: Token{Stream: maxStream}.Encode()}
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
			prevContent := e.prevContentAnnotator(ctx, roomID, maxStream)
			jr.State.Events = make([]json.RawMessage, 0, len(stateEvs))
			for _, se := range stateEvs {
				if !filter.keepState(&se) {
					continue
				}
				if filter.lazyLoadMembers() && !lazyLoadMemberEvent(&se, map[string]bool{}) {
					continue
				}
				jr.State.Events = append(jr.State.Events, filter.applyEventFields(prevContent(filter.renderEvent(&se), &se)))
			}
		} else {
			jr.State.Events = []json.RawMessage{}
		}
		jr.Summary = e.roomSummary(ctx, roomID, opts.UserID)
		jr.UnreadNotifications, jr.UnreadThreads = e.unreadCounts(ctx, roomID, opts)
		return jr, nil
	}

	// A room the user *transitioned into* after the sync token is a "newly
	// joined" room: its timeline is the room's recent history (not just the
	// events since the token) and is always marked limited, forcing the client
	// to back-paginate the pre-join history (spec + Synapse). A profile update
	// (an m.room.member with membership=join when the user was already joined,
	// e.g. a displayname change) does NOT trigger this — NewlyJoinedAfter
	// requires the join to be a real membership transition.
	newlyJoined := false
	unpartialStated := false
	if opts.Since.Stream > 0 {
		if joined, err := e.store.NewlyJoinedAfter(ctx, roomID, opts.UserID, opts.Since.Stream); err == nil {
			newlyJoined = joined
		}
	}
	// A room that was partial-state and became fully-stated is treated as newly
	// joined (mirror of Synapse's forced_newly_joined_room_ids): eager syncs
	// deliberately omitted the room while it was partial, so the first sync
	// whose baseline predates the resync delivers its full state and a full-room
	// (limited) timeline instead of an empty delta the client cannot overlay
	// onto anything. This applies to initial syncs too (Since == 0, i.e. a fresh
	// baseline): the resync'd state events were persisted into the room's
	// history, so without the flag the state section would dedup them out
	// (Complement's TestPartialStateJoin/EagerInitialSyncDuringPartialStateJoin
	// expects the resynced members in state.events). unpartialStated
	// distinguishes this case from an ordinary join (e.g. accepting an invite),
	// where the client already holds the invite_state and the state section
	// stays empty.
	if up, err := e.store.RoomUnpartialStateStream(ctx, roomID); err == nil && up > opts.Since.Stream {
		newlyJoined = true
		unpartialStated = true
	}

	// The upper bound of the window is the room's own latest event (its tail is
	// unaffected by other rooms advancing the shared stream), falling back to
	// the global max.
	to := maxStream
	if latest, err := e.store.LatestEvent(ctx, roomID); err == nil && latest != nil {
		to = latest.StreamOrdering
	}

	from := opts.Since.Stream
	if opts.Since.Stream == 0 || newlyJoined {
		// Full-room window: the last `limit` events.
		from = to - int64(limit)
		if from < 0 {
			from = 0
		}
	}
	// A DAG gap inside the window (an event was persisted while its
	// prev_events were still missing locally) marks the timeline limited and
	// drops everything before the gap: the client must back-paginate to fill
	// the hole (mirror of Synapse's get_timeline_gaps). The gap's own event is
	// included — the gap sits *before* it.
	gapLimited := false
	if gap, ok := e.store.TimelineGapSince(ctx, roomID, opts.Since.Stream); ok && gap > from && gap <= to {
		gapLimited = true
		from = gap - 1
	}

	// Incremental windows are fetched newest-first (DESC LIMIT limit+1) so the
	// `limit` most recent events are returned; the extra row reveals whether
	// the window was truncated by the count limit. Initial/full-room windows
	// (initial sync, newly-joined rooms) fetch a wider raw set (Synapse's
	// `load_limit = max(timeline_limit * 2, 10)`) so that pre-existing state
	// events — create, member, power_levels, ... — do not squeeze the message
	// window below `limit`: the raw set is filtered *after* fetching, and only
	// filtered events count toward the limit (a room whose messages alone
	// exceed the limit is limited, a room whose raw set merely contains many
	// state events is not).
	rawLimit := limit + 1
	if opts.Since.Stream == 0 || newlyJoined {
		rawLimit = max(limit*2, 10)
	}
	var evs []storage.EventRow
	var err error
	if (opts.Since.Stream == 0 || newlyJoined) && !gapLimited {
		// Full-room window: paginate backward in DAG (topological) order —
		// depth, then stream_ordering — accumulating batches until the FILTERED
		// events fill `limit` or the room's start is reached (mirror of
		// Synapse's _load_filtered_recents loop, which uses
		// paginate_room_events_by_topological_ordering for initial / newly-joined
		// windows). Depth ordering matters: a late-arriving fork event (low
		// depth, high stream position) must not displace genuinely-newer events
		// from the window (Complement's TestSyncOmitsStateChangeOnFilteredEvents
		// forks the DAG and expects the newest message in the limit-1 timeline,
		// with the fork's state event in the state section). The fill loop widens
		// the window down the depth axis; the number of batches is bounded
		// (Synapse's max_repeat = 5) so a room with mostly filter-blocked history
		// does not paginate to the room's start.
		const maxFillBatches = 5
		// The window's depth ceiling is the room's MAX depth, not the depth of
		// the highest-stream event: a late-arriving fork or partial-state critical
		// event (low depth, high stream position) must not shrink the window and
		// drop genuinely-newer (higher-depth) events — e.g. the joining user's own
		// join event (Complement's TestDeviceListUpdates
		// /when_joining_a_room_with_a_remote_user syncs for the join).
		maxDepth := int64(0)
		if md, mderr := e.store.MaxDepth(ctx, roomID); mderr == nil && md > 0 {
			maxDepth = md
		} else if latest, lerr := e.store.LatestEvent(ctx, roomID); lerr == nil && latest != nil {
			maxDepth = latest.Depth
		}
		evs, err = e.store.EventsForRoomByDepth(ctx, roomID, maxDepth, rawLimit)
		minDepth := int64(0)
		if len(evs) > 0 {
			minDepth = evs[len(evs)-1].Depth // newest-first: last is the oldest
		}
		for b := 0; err == nil && filteredCount(evs, filter) < limit && b < maxFillBatches; b++ {
			if len(evs) == 0 {
				break
			}
			// Fetch the batch strictly below the current oldest fetched depth.
			batch, berr := e.store.EventsForRoomByDepth(ctx, roomID, minDepth-1, rawLimit)
			if berr != nil {
				err = berr
				break
			}
			if len(batch) == 0 {
				break
			}
			// batch is newest-first, like evs; append it AFTER evs so the merged
			// list stays newest-first (the batch is strictly older than evs, so it
			// belongs at the tail). A misordered merge would make the window's
			// oldest event (used for the HasEventsBelowDepth truncation check and
			// prev_batch) point into the middle of the room's history.
			evs = append(evs, batch...)
			minDepth = batch[len(batch)-1].Depth
		}
		// Newest-first (depth DESC) → oldest-first (depth ASC).
		for i, j := 0, len(evs)-1; i < j; i, j = i+1, j-1 {
			evs[i], evs[j] = evs[j], evs[i]
		}
	} else {
		evs, err = e.store.EventsForRoom(ctx, roomID, from, to, rawLimit, "b")
		for i, j := 0, len(evs)-1; i < j; i, j = i+1, j-1 {
			evs[i], evs[j] = evs[j], evs[i]
		}
	}
	if err != nil {
		return jr, err
	}
	// Count truncation is decided AFTER client filtering: fetch the raw set,
	// drop the filter-excluded events, then truncate to `limit` if more remain.
	// For initial/full-room windows the raw fetch is wider (above), so a room
	// whose messages alone exceed the limit is limited while a room whose raw
	// set merely carries many state events is not. The extra probe row (+1)
	// for incremental windows reveals truncation directly; a full-room window
	// is limited when the filtered events still exceed `limit`, or when more
	// events exist before the window's start (the fill loop stopped short of
	// the room's start — mirror of Synapse's pagination-limited flag, which
	// tells the client to back-paginate).
	countLimited := false
	if (opts.Since.Stream == 0 || newlyJoined) && !gapLimited {
		countLimited = filteredCount(evs, filter) > limit
		if !countLimited {
			if len(evs) > 0 {
				if trunc, terr := e.store.HasEventsBelowDepth(ctx, roomID, evs[0].Depth); terr == nil && trunc {
					countLimited = true
				}
			}
		}
	} else if filteredCount(evs, filter) > limit {
		// Incremental windows: truncation is decided AFTER client filtering. The
		// extra probe row (+1) only signals truncation when the FILTERED events
		// still exceed the limit — a window whose raw set merely carries many
		// filter-blocked events is not limited (sytest "A filtered timeline
		// reaches its limit" syncs a room with 13 messages under a
		// timeline.types=["m.room.message"] limit=1 filter and expects the single
		// matching message with limited=false).
		countLimited = true
	}
	// Soft-failed (rejected) events are never delivered to clients: drop them
	// from the timeline (their IDs remain valid as prev_events so pagination
	// and the DAG stay intact, mirror of Synapse's soft-fail).
	if len(evs) > 0 {
		ids := make([]string, 0, len(evs))
		for _, ev := range evs {
			ids = append(ids, ev.EventID)
		}
		if rejected, err := e.store.RejectedEventIDs(ctx, ids); err == nil && len(rejected) > 0 {
			kept := evs[:0]
			for _, ev := range evs {
				if !rejected[ev.EventID] {
					kept = append(kept, ev)
				}
			}
			evs = kept
		}
	}

	// Track senders for lazy-load members. Events are rendered in stream order
	// (newest last); when the filtered count exceeds `limit`, the newest `limit`
	// are kept (countLimited was set above from the pre-filter count).
	// MSC4115: annotate each timeline event with the syncing user's membership
	// at the time of the event (unsigned.membership).
	membershipAt := e.membershipAnnotator(ctx, roomID, opts.UserID, maxStream)
	// Spec: membership changes carry the previous membership in
	// unsigned.prev_content (and unsigned.prev_sender) so clients can render
	// transitions (e.g. invite -> join, join -> leave).
	prevContent := e.prevContentAnnotator(ctx, roomID, maxStream)
	// Per-event history_visibility filtering: in invited/joined rooms the user
	// only sees events from their invite/join boundary onward (events sent
	// before the room became invited/joined stay visible under the earlier,
	// more permissive visibility). The evaluator's histories are bounded by the
	// room's latest stream so a newly-joined user's full-room window filters
	// pre-boundary history correctly.
	//
	// Events in the room's CURRENT state bypass the visibility check (Synapse's
	// always_include_ids=current_state_ids passed to
	// filter_and_transform_events_for_client): current state is what the client
	// needs to render the room, and in a private (joined) room the events that
	// establish it — e.g. an invitee's join member event — must still surface in
	// a limited/newly-joined timeline even though the syncing user was not
	// joined at the time they were sent (sytest "Current state appears in
	// timeline in private history").
	limited := newlyJoined || gapLimited || countLimited
	currentState := map[string]bool{}
	if limited {
		if stateRows, serr := e.store.GetState(ctx, roomID); serr == nil {
			for _, s := range stateRows {
				currentState[s.EventID] = true
			}
		}
	}
	vis, _ := eventstate.NewVisibilityEvaluator(ctx, e.store, roomID, opts.UserID, maxStream)
	// GDPR erasure (spec §Erasure): an erased sender's events are served redacted
	// to users who were not joined at the event's time (mirror of Synapse's
	// _check_client_allowed_to_see_event: `sender_erased and not joined → prune`).
	// The window's senders are checked once; the viewer's membership per event is
	// taken from the visibility evaluator.
	erased := map[string]bool{}
	if len(evs) > 0 {
		senders := make([]string, 0, len(evs))
		for _, ev := range evs {
			senders = append(senders, ev.Sender)
		}
		if es, err := e.store.ErasedUsers(ctx, senders); err == nil {
			erased = es
		}
	}
	type renderedEvent struct {
		raw    json.RawMessage
		send   string
		stream int64
	}
	var rendered []renderedEvent
	for _, ev := range evs {
		if !filter.keepTimeline(&ev) {
			continue
		}
		// Events sent by an ignored user are never delivered (spec "Ignoring
		// Users"; mirror of Synapse, which applies filter_events_for_client to
		// timeline events but not to state).
		if ignored[ev.Sender] {
			continue
		}
		if vis != nil && !currentState[ev.EventID] && !vis.CanSeeRow(&ev) {
			continue
		}
		var raw json.RawMessage
		if erased[ev.Sender] && (vis == nil || vis.MembershipAt(ev.Depth) != "join") {
			raw = filter.applyEventFields(e.annotateTxn(ctx, prevContent(membershipAt(filter.renderEventErased(&ev), &ev), &ev), ev.EventID))
		} else {
			raw = filter.applyEventFields(e.annotateTxn(ctx, prevContent(membershipAt(filter.renderEvent(&ev), &ev), &ev), ev.EventID))
		}
		rendered = append(rendered, renderedEvent{raw: raw, send: ev.Sender, stream: ev.StreamOrdering})
	}
	if countLimited && len(rendered) > limit {
		// Keep the newest `limit` filtered events.
		rendered = rendered[len(rendered)-limit:]
	}
	// prev_batch anchor: the oldest event the client actually receives in the
	// stream window. Computed AFTER count truncation: a truncated window's
	// dropped oldest events must remain reachable, so prev_batch must point
	// before the oldest *delivered* event — paginating back from a token below
	// a dropped event skips it, and it was not delivered in the window either
	// (spec invariant: every event is either in /sync or reachable via
	// /messages from prev_batch; Synapse derives prev_batch from the final
	// timeline's recents[0].stream - 1 for the same reason).
	earliest := int64(0)
	for _, re := range rendered {
		if earliest == 0 || re.stream < earliest {
			earliest = re.stream
		}
	}
	timeline := Timeline{Events: make([]json.RawMessage, 0, len(rendered))}
	senders := map[string]bool{}
	for _, re := range rendered {
		timeline.Events = append(timeline.Events, re.raw)
		senders[re.send] = true
	}
	// prev_batch is the token a client passes to paginate further back: one
	// position before the oldest visible event (spec/Synapse: backward /messages
	// `from` is inclusive of the token, so prev_batch must point *before* the
	// window's oldest event or the oldest event would be re-delivered). It is
	// set even when the timeline is not limited (so clients that page backwards
	// from a full window get an empty page rather than a missing token), and
	// doubles as the `at` anchor for /members?at= and /messages back-pagination.
	// A full_state / initial sync with an empty window still carries prev_batch
	// (sytest "Full state sync includes joined rooms" requires the key).
	timeline.Limited = limited
	if limited {
		if len(evs) > 0 {
			timeline.PrevBatch = Token{Stream: earliest - 1}.Encode()
		} else {
			timeline.PrevBatch = Token{Stream: to - 1}.Encode()
		}
	} else {
		timeline.PrevBatch = Token{Stream: to - 1}.Encode()
	}
	jr.Timeline = timeline

	// State: full state on initial sync or full_state; otherwise empty (delta).
	// A room that was partial-state and became fully-stated during the sync
	// window also carries its full current state in the state section: eager
	// syncs deliberately omitted the room while it was partial, so this poll is
	// the client's first view (spec: "The state updates for the room up to the
	// start of the timeline" — for such a room, that is the full state). An
	// ordinary newly-joined room (e.g. accepting an invite) does NOT: the
	// client already holds the invite's stripped state, and Synapse returns an
	// empty state delta there. A limited (gappy or count-truncated) incremental
	// sync carries the state too: the client cannot overlay the deltas onto a
	// baseline it never received (the gap cut its view of the room). Lazy-load
	// members replaces the full state with only the m.room.member events for
	// timeline senders.
	//
	// The state section obeys the filter's room.state rules
	// (state.types/not_types/senders/not_senders), and events already delivered
	// in the timeline are not duplicated here (spec + Synapse: "state from the
	// timeline should not appear in the state dictionary" — sytest "State is
	// included in the timeline in the initial sync").
	//
	// A room that just de-partial-stated is the exception: the resync's fetched
	// state events were persisted as part of the room's history (unlike Synapse,
	// which stores them as outliers outside the timeline stream), so deduping
	// them out of the state section would empty it entirely (Complement's
	// TestPartialStateJoin expects the resync'd members in state.events). For
	// such rooms the state section carries the full current state regardless of
	// the timeline.
	// The state section carries, in order of precedence:
	//   - the FULL current state (minus events already in the timeline) on
	//     initial syncs, full_state syncs, rooms that just de-partial-stated,
	//     and rooms the user newly joined during the window (mirror of
	//     Synapse's full_state for newly_joined_rooms: the client's baseline
	//     predates the join, so it needs the room's state to render it —
	//     sytest "When user joins a room the state is included in the next
	//     sync");
	//   - the STATE DELTA (current state minus the state at the previous
	//     token) for limited (gappy or count-truncated) incremental syncs: the
	//     gap/truncation cut the client's view of the room, so it needs the
	//     state changes since its token to rebuild the room (mirror of
	//     Synapse's _calculate_state for limited batches — sytest "Changes to
	//     state are included in an gapped incremental sync" expects exactly
	//     the changed state event, not the full state);
	//   - nothing (empty array) for plain incremental syncs without
	//     lazy-loading.
	//
	// Lazy-loading replaces the above with only the m.room.member events for
	// timeline senders (plus the syncing user's own), deduplicated against the
	// memberships already delivered to this device (spec lazy-loading; sytest
	// "We don't send redundant membership state across incremental syncs by
	// default"). For limited syncs the delta part still carries all state
	// changes (Synapse disables the LL restriction for gappy syncs) and the
	// sender memberships are layered on top.
	//
	// The state section obeys the filter's room.state rules
	// (state.types/not_types/senders/not_senders), and events already delivered
	// in the timeline are not duplicated here (spec + Synapse: "state from the
	// timeline should not appear in the state dictionary" — sytest "State is
	// included in the timeline in the initial sync").
	//
	// A room that just de-partial-stated is the exception to the timeline dedup:
	// the resync's fetched state events were persisted as part of the room's
	// history (unlike Synapse, which stores them as outliers outside the
	// timeline stream), so deduping them out of the state section would empty
	// it entirely (Complement's TestPartialStateJoin expects the resync'd
	// members in state.events).
	// The set of event IDs delivered in the timeline, used to avoid duplicating
	// state events in the state section (spec: "state from the timeline should
	// not appear in the state dictionary"). A room that just de-partial-stated
	// is the exception: the resync's fetched state events were persisted as part
	// of the room's history (unlike Synapse, which stores them as outliers
	// outside the timeline stream), so deduping them out of the state section
	// would empty it entirely (Complement's TestPartialStateJoin expects the
	// resync'd members in state.events).
	inTimeline := map[string]bool{}
	if !unpartialStated {
		for _, re := range rendered {
			var obj map[string]json.RawMessage
			if json.Unmarshal(re.raw, &obj) == nil {
				var id string
				if json.Unmarshal(obj["event_id"], &id) == nil {
					inTimeline[id] = true
				}
			}
		}
	}
	fullState := opts.Since.Stream == 0 || opts.FullState || unpartialStated || newlyJoined
	if fullState {
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
			if inTimeline[se.EventID] {
				continue
			}
			if !filter.keepState(&se) {
				continue
			}
			if filter.lazyLoadMembers() && !lazyLoadMemberEvent(&se, senders) {
				continue
			}
			jr.State.Events = append(jr.State.Events, filter.applyEventFields(prevContent(filter.renderEvent(&se), &se)))
		}
	} else if opts.Since.Stream > 0 && (gapLimited || countLimited) {
		// State delta for a limited incremental sync: the room_state tuples
		// whose governing event was persisted after the token (i.e. the net
		// state changes during the window).
		stateDeltas, err := e.store.CurrentStateDeltasSince(ctx, roomID, opts.Since.Stream)
		if err != nil {
			return jr, err
		}
		jr.State.Events = make([]json.RawMessage, 0, len(stateDeltas))
		for i := range stateDeltas {
			se := &stateDeltas[i]
			if inTimeline[se.EventID] {
				continue
			}
			if !filter.keepState(se) {
				continue
			}
			// Limited syncs disable the LL member restriction for the delta
			// part (mirror of Synapse: state_filter = StateFilter.all() when
			// batch.limited).
			jr.State.Events = append(jr.State.Events, filter.applyEventFields(prevContent(filter.renderEvent(se), se)))
		}
	} else {
		// A plain incremental sync with no full state renders `state.events` as
		// an empty array (spec + clients expect the key to exist). Lazy-loading
		// fills it below with the timeline senders' memberships.
		jr.State.Events = []json.RawMessage{}
	}
	// Lazy-loading: the state section must carry the membership events of every
	// timeline sender (spec lazy-loading). In a partial-state room the room's
	// current state may still be missing them (the background resync has not
	// completed), so backfill from the denormalised membership table — the
	// senders' member events were persisted when their auth chains were fetched
	// on ingest (mirror of Synapse's _find_missing_partial_state_memberships).
	// The syncing user's own membership is always included when they (re)join a
	// room under lazy-loading (spec requirement). Unless the client asked for
	// redundant members (filter.include_redundant_members), memberships already
	// delivered to this device are suppressed (mirror of Synapse's
	// lazy_loaded_members_cache).
	if filter.lazyLoadMembers() {
		llCache := e.llCacheFor(opts.UserID, opts.DeviceID)
		seen := make(map[string]bool, len(jr.State.Events))
		for _, raw := range jr.State.Events {
			var sev struct {
				Type     string `json:"type"`
				StateKey string `json:"state_key"`
			}
			if json.Unmarshal(raw, &sev) == nil && sev.Type == "m.room.member" {
				seen[sev.StateKey] = true
			}
		}
		// The syncing user's own membership is included only when the state
		// section is a full state (initial sync, full_state, newly-joined room):
		// it is needed to render the room on the first sync, and was already
		// delivered then — an incremental sync must not re-send it (mirror of
		// Synapse, which unions the user into members_to_fetch only in
		// _compute_state_delta_for_full_sync; sytest "We do send redundant
		// membership state across incremental syncs if asked" expects only the
		// timeline senders' memberships, not the syncing user's own).
		if opts.Since.Stream == 0 || opts.FullState || newlyJoined {
			senders[opts.UserID] = true
		}
		for u := range senders {
			if seen[u] {
				continue
			}
			m, err := e.store.GetMembership(ctx, roomID, u)
			if err != nil || m == nil || m.EventID == "" {
				continue
			}
			ev, err := e.store.GetEvent(ctx, m.EventID)
			if err != nil || ev == nil {
				continue
			}
			if !filter.IncludeRedundantMembers {
				if cached, ok := llCache[u]; ok && cached == ev.EventID {
					// This device already received this exact membership event.
					continue
				}
			}
			jr.State.Events = append(jr.State.Events, filter.applyEventFields(prevContent(filter.renderEvent(ev), ev)))
		}
		// Remember every membership event delivered in this response (in the
		// state section and in the timeline) so the next incremental sync can
		// suppress the redundant ones.
		if opts.Since.Stream == 0 {
			llCache = e.resetLLCache(opts.UserID, opts.DeviceID)
		}
		for _, raw := range jr.State.Events {
			var sev struct {
				Type     string `json:"type"`
				StateKey string `json:"state_key"`
				EventID  string `json:"event_id"`
			}
			if json.Unmarshal(raw, &sev) == nil && sev.Type == "m.room.member" {
				llCache[sev.StateKey] = sev.EventID
			}
		}
		for _, re := range rendered {
			var sev struct {
				Type     string `json:"type"`
				StateKey string `json:"state_key"`
				EventID  string `json:"event_id"`
			}
			if json.Unmarshal(re.raw, &sev) == nil && sev.Type == "m.room.member" {
				llCache[sev.StateKey] = sev.EventID
			}
		}
	}
	// MSC4222 use_state_after: the client asks for the room state as of the end
	// of the timeline instead of the state at the start (state). On an initial
	// sync both coincide with the room's current state; populate state_after
	// with the same (filtered) state set. Clients (incl. matrix-rust-sdk) rely
	// on this when syncing with use_state_after=true.
	if opts.UseStateAfter {
		stateRows, err := e.store.GetState(ctx, roomID)
		if err != nil {
			return jr, err
		}
		ids := make([]string, 0, len(stateRows))
		for _, s := range stateRows {
			ids = append(ids, s.EventID)
		}
		stateEvs, _ := e.store.EventsByIDs(ctx, ids)
		sa := StateSet{Events: make([]json.RawMessage, 0, len(stateEvs))}
		for _, se := range stateEvs {
			if !filter.keepState(&se) {
				continue
			}
			if filter.lazyLoadMembers() && !lazyLoadMemberEvent(&se, senders) {
				continue
			}
			sa.Events = append(sa.Events, filter.applyEventFields(filter.renderEvent(&se)))
		}
		jr.StateAfter = &sa
	}

	// Room summary: joined/invited member counts (spec m.joined_member_count /
	// m.invited_member_count) plus up to five hero user IDs.
	jr.Summary = e.roomSummary(ctx, roomID, opts.UserID)
	jr.UnreadNotifications, jr.UnreadThreads = e.unreadCounts(ctx, roomID, opts)
	return jr, nil
}

// unreadCounts computes the user's unread notification counts for a joined
// room (spec unread_notifications; MSC3773/MSC3774 thread counts). The main
// timeline count covers events since the user's latest unthreaded read receipt
// (falling back to their join position); each thread's count is scoped by its
// own threaded read receipt, floored by the unthreaded receipt. With the
// unread_thread_notifications filter the main count excludes thread events and
// per-thread counts are returned under unread_thread_notifications; otherwise
// the thread counts are folded into the main count (mirror of Synapse's
// _get_unread_counts_by_pos_txn + sync handler combination).
//
// An event counts as a notification when the user's push rules evaluate it to
// notify (never for their own events), and as a highlight when the matched
// rule sets the highlight tweak.
func (e *Engine) unreadCounts(ctx context.Context, roomID string, opts SyncOptions) (*UnreadNotifications, map[string]UnreadNotifications) {
	if opts.UserID == "" {
		return nil, nil
	}
	receipts, err := e.store.ReadReceiptsForUserInRoom(ctx, roomID, opts.UserID)
	if err != nil {
		return nil, nil
	}
	// Read positions per timeline (Synapse semantics): the unthreaded receipt
	// (thread_id "") is a floor for every timeline; the main-threaded receipt
	// (thread_id "main", MSC3773's MAIN_TIMELINE sentinel) advances the main
	// timeline; a threaded receipt (thread_id = thread root) advances that
	// thread. A timeline's effective position is the later of its own receipt
	// and the unthreaded floor.
	unthreadedPos := int64(0)
	mainPos := int64(0)
	threadPos := map[string]int64{}
	for _, rc := range receipts {
		pos := rc.StreamID
		if so, err := e.store.EventStreamOrdering(ctx, rc.EventID); err == nil && so > 0 {
			pos = so
		}
		switch rc.ThreadID {
		case "":
			if pos > unthreadedPos {
				unthreadedPos = pos
			}
		case "main":
			if pos > mainPos {
				mainPos = pos
			}
		default:
			if pos > threadPos[rc.ThreadID] {
				threadPos[rc.ThreadID] = pos
			}
		}
	}
	if unthreadedPos == 0 {
		// No unthreaded receipt: fall back to the user's join position.
		if m, err := e.store.GetMembership(ctx, roomID, opts.UserID); err == nil && m.StreamOrdering > 0 {
			unthreadedPos = m.StreamOrdering
		}
	}
	if unthreadedPos <= 0 {
		return nil, nil
	}
	if mainPos < unthreadedPos {
		mainPos = unthreadedPos
	}

	rulesRaw, _ := e.store.GetPushRules(ctx, opts.Localpart)
	var rules map[string]any
	_ = json.Unmarshal(rulesRaw, &rules)
	if rules == nil {
		return nil, nil
	}

	joined := int64(0)
	if users, err := e.store.JoinedUserIDs(ctx, roomID); err == nil {
		joined = int64(len(users))
	}

	// Main timeline: events after the main timeline's effective read position.
	mainCount, mainHighlight, mainUnread := e.countFor(ctx, roomID, "", mainPos, opts, rules, joined)
	// Threads: events after each thread's own (threaded) read position, which
	// is floored by the unthreaded position (a new unthreaded receipt reads all
	// threads up to its position, per Synapse).
	threadCounts := map[string]UnreadNotifications{}
	roots, err := e.store.ThreadRootsInRoom(ctx, roomID)
	if err == nil {
		for _, root := range roots {
			pos := threadPos[root]
			if pos < unthreadedPos {
				pos = unthreadedPos
			}
			n, h, u := e.countFor(ctx, roomID, root, pos, opts, rules, joined)
			if n == 0 && h == 0 && u == 0 {
				continue
			}
			threadCounts[root] = UnreadNotifications{NotificationCount: &n, HighlightCount: &h, UnreadCount: &u}
		}
	}

	if opts.Filter != nil && opts.Filter.UnreadThreadNotifications {
		main := &UnreadNotifications{NotificationCount: &mainCount, HighlightCount: &mainHighlight, UnreadCount: &mainUnread}
		return main, threadCounts
	}
	// No thread filter: fold the thread counts into the main count.
	combinedCount, combinedHighlight, combinedUnread := mainCount, mainHighlight, mainUnread
	for _, tc := range threadCounts {
		if tc.NotificationCount != nil {
			combinedCount += *tc.NotificationCount
		}
		if tc.HighlightCount != nil {
			combinedHighlight += *tc.HighlightCount
		}
		if tc.UnreadCount != nil {
			combinedUnread += *tc.UnreadCount
		}
	}
	return &UnreadNotifications{NotificationCount: &combinedCount, HighlightCount: &combinedHighlight, UnreadCount: &combinedUnread}, nil
}

// SlidingUnreadCounts computes a user's combined unread notification and
// highlight counts for a joined room, for the sliding-sync room response
// (which carries flat notification_count/highlight_count fields rather than
// /v3/sync's nested unread_notifications object). It returns false when the
// counts cannot be computed (no read position, or no push ruleset), in which
// case the caller omits the fields.
func (e *Engine) SlidingUnreadCounts(ctx context.Context, roomID, userID, localpart string) (notif, highlight int, ok bool) {
	main, _ := e.unreadCounts(ctx, roomID, SyncOptions{UserID: userID, Localpart: localpart})
	if main == nil {
		return 0, 0, false
	}
	n, h := 0, 0
	if main.NotificationCount != nil {
		n = *main.NotificationCount
	}
	if main.HighlightCount != nil {
		h = *main.HighlightCount
	}
	return n, h, true
}

// filteredCount reports how many events in rows pass the sync filter's
// timeline keep rule. Used to size full-room windows: only filtered events
// count toward the timeline limit (see buildJoinedRoom).
func filteredCount(rows []storage.EventRow, filter *SyncFilter) int {
	n := 0
	for i := range rows {
		if filter.keepTimeline(&rows[i]) {
			n++
		}
	}
	return n
}

// countFor evaluates the unread push actions for one timeline (main or a
// single thread) and returns the notification and highlight counts.
func (e *Engine) countFor(ctx context.Context, roomID, root string, since int64, opts SyncOptions, rules map[string]any, joined int64) (int, int, int) {
	evs, err := e.store.EventsForNotificationCount(ctx, roomID, root, since, 1000)
	if err != nil {
		return 0, 0, 0
	}
	var notif, highlight, unread int
	for _, ev := range evs {
		res := pushrules.Evaluate(rules, opts.UserID, opts.Localpart, pushrules.EventSnapshot{
			Type:        ev.Type,
			Sender:      ev.Sender,
			RoomID:      roomID,
			Content:     ev.Content,
			MemberCount: int(joined),
		})
		if res.Notifies {
			notif++
		}
		if res.Highlights {
			highlight++
		}
		if res.MarkUnread {
			unread++
		}
	}
	return notif, highlight, unread
}

// buildPeekedRoom constructs the peek section entry (MSC2753) for a device that
// peeks into a world_readable room without joining. The timeline starts at the
// room's beginning (a peek is a history watch, so the first sync carries the
// room's creation onward) and advances incrementally. On an incremental sync
// with no new events the room is omitted entirely (nil), matching the spec's
// "unchanged rooms aren't re-sent" behaviour.
func (e *Engine) buildPeekedRoom(ctx context.Context, roomID string, opts SyncOptions, maxStream int64) (*JoinedRoom, error) {
	from := opts.Since.Stream
	limit := 50
	evs, err := e.store.EventsForRoom(ctx, roomID, from, maxStream, limit, "f")
	if err != nil {
		return nil, err
	}
	// Incremental sync with nothing new: the peeked room disappears from the
	// response (the client already has the timeline up to its token).
	if from > 0 && len(evs) == 0 {
		return nil, nil
	}
	pr := &JoinedRoom{}
	timeline := Timeline{Events: make([]json.RawMessage, 0, len(evs))}
	for _, ev := range evs {
		timeline.Events = append(timeline.Events, clientEvent(&ev))
	}
	if len(evs) >= limit {
		timeline.Limited = true
	}
	// prev_batch is always present: it points at the sync point (or the oldest
	// visible event for a truncated window), so clients can back-paginate.
	timeline.PrevBatch = Token{Stream: maxStream}.Encode()
	pr.Timeline = timeline
	// The state section, when present, must hold no events for a peeked room.
	pr.State.Events = []json.RawMessage{}
	return pr, nil
}

// lazyLoadMemberEvent reports whether a state event should be included under a
// lazy_load_members filter. Non-member state events pass (the lazy-load filter
// is a member filter, not a state filter — mirror of Synapse's
// StateFilter.from_lazy_load_member_list, whose include_others=True keeps every
// non-member state type); membership events are kept only for users present in
// the timeline (their state_key), which is what lazy-loading is for.
func lazyLoadMemberEvent(ev *storage.EventRow, senders map[string]bool) bool {
	if ev.Type != "m.room.member" {
		return true
	}
	return senders[ev.StateKey]
}

// membershipAnnotator returns a function that attaches unsigned.membership to a
// rendered client event based on the syncing user's membership at that event's
// stream position (MSC4115). It loads the user's member-event history in the
// room once per sync and walks it per event.
func (e *Engine) membershipAnnotator(ctx context.Context, roomID, userID string, upto int64) func(ev json.RawMessage, row *storage.EventRow) json.RawMessage {
	history, err := e.store.MemberHistory(ctx, roomID, upto)
	if err != nil {
		return func(ev json.RawMessage, _ *storage.EventRow) json.RawMessage { return ev }
	}
	return func(ev json.RawMessage, row *storage.EventRow) json.RawMessage {
		if row == nil {
			return ev
		}
		membership := "leave" // MSC4115: events predating the user's membership are "leave"
		best := int64(0)
		for _, h := range history {
			if h.UserID == userID && h.StreamOrdering <= row.StreamOrdering && h.StreamOrdering > best {
				best = h.StreamOrdering
				membership = h.Membership
			}
		}
		var obj map[string]json.RawMessage
		if err := json.Unmarshal(ev, &obj); err != nil {
			return ev
		}
		// Merge into any existing unsigned object (e.g. unsigned.redacted_by from
		// clientEvent) rather than replacing it, mirroring annotateTxn.
		unsigned := map[string]any{"membership": membership}
		if existing, ok := obj["unsigned"]; ok {
			var m map[string]json.RawMessage
			if err := json.Unmarshal(existing, &m); err == nil {
				for k, v := range m {
					unsigned[k] = v
				}
			}
		}
		unsignedJSON, _ := json.Marshal(unsigned)
		obj["unsigned"] = unsignedJSON
		b, _ := json.Marshal(obj)
		return b
	}
}

// prevContentAnnotator returns a function that attaches unsigned.prev_content
// and unsigned.prev_sender to a rendered m.room.member event: the content (and
// sender) of the previous member event for the same state_key. Per the spec,
// "Previous membership can be retrieved from the prev_content object on an
// event"; it lets clients render transitions (invite -> join, join -> leave).
// Only member events with an earlier member event for the same user are
// annotated.
func (e *Engine) prevContentAnnotator(ctx context.Context, roomID string, upto int64) func(ev json.RawMessage, row *storage.EventRow) json.RawMessage {
	history, err := e.store.MemberEvents(ctx, roomID, upto)
	if err != nil {
		return func(ev json.RawMessage, _ *storage.EventRow) json.RawMessage { return ev }
	}
	return func(ev json.RawMessage, row *storage.EventRow) json.RawMessage {
		if row == nil || row.Type != "m.room.member" {
			return ev
		}
		// Find the most recent earlier member event for the same user.
		var prev *storage.MemberEventRow
		for i := range history {
			h := &history[i]
			if h.UserID == row.StateKey && h.StreamOrdering < row.StreamOrdering {
				if prev == nil || h.StreamOrdering > prev.StreamOrdering {
					prev = h
				}
			}
		}
		if prev == nil {
			return ev
		}
		var obj map[string]json.RawMessage
		if err := json.Unmarshal(ev, &obj); err != nil {
			return ev
		}
		unsigned := map[string]any{}
		if existing, ok := obj["unsigned"]; ok {
			var m map[string]json.RawMessage
			if err := json.Unmarshal(existing, &m); err == nil {
				for k, v := range m {
					unsigned[k] = v
				}
			}
		}
		var prevContent map[string]any
		if err := json.Unmarshal(prev.Content, &prevContent); err != nil {
			return ev
		}
		unsigned["prev_content"] = prevContent
		unsigned["prev_sender"] = prev.Sender
		unsignedJSON, _ := json.Marshal(unsigned)
		obj["unsigned"] = unsignedJSON
		b, _ := json.Marshal(obj)
		return b
	}
}

// annotateTxn merges unsigned.transaction_id into a rendered timeline event
// when the event was produced by a client transaction (spec: events sent with a
// transaction ID carry it in unsigned.transaction_id).
func (e *Engine) annotateTxn(ctx context.Context, rendered json.RawMessage, eventID string) json.RawMessage {
	txnID, err := e.store.GetEventTxnID(ctx, eventID)
	if err != nil || txnID == "" {
		return rendered
	}
	var obj map[string]json.RawMessage
	if err := json.Unmarshal(rendered, &obj); err != nil {
		return rendered
	}
	// Merge into any existing unsigned object (e.g. unsigned.membership from
	// MSC4115) rather than replacing it.
	unsigned := map[string]any{"transaction_id": txnID}
	if existing, ok := obj["unsigned"]; ok {
		var m map[string]json.RawMessage
		if err := json.Unmarshal(existing, &m); err == nil {
			for k, v := range m {
				unsigned[k] = v
			}
		}
	}
	unsignedJSON, _ := json.Marshal(unsigned)
	obj["unsigned"] = unsignedJSON
	b, _ := json.Marshal(obj)
	return b
}

// roomSummary builds the room summary section (member counts + heroes). The
// joined/invited counts are always present; heroes are included only when the
// room has no name and no canonical alias (spec Room Summaries: "The heroes
// are the users which are most likely to be correct when rendering a room
// without a name", and are omitted when the room has a name — sytest's
// "Named room comes with just joined member count summary" asserts the absence
// of m.heroes).
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
	s := &RoomSummary{JoinedMembers: joined, InvitedMembers: invited}
	if e.roomHasName(ctx, roomID) {
		return s
	}
	s.Heroes = heroes
	return s
}

// roomHasName reports whether the room carries a non-empty m.room.name or
// m.room.canonical_alias (either suppresses the heroes in the room summary —
// mirror of Synapse's sync handler, which skips hero computation when a name
// or canonical alias is set).
func (e *Engine) roomHasName(ctx context.Context, roomID string) bool {
	if id, err := e.store.GetStateEvent(ctx, roomID, "m.room.name", ""); err == nil {
		if ev, err := e.store.GetEvent(ctx, id); err == nil {
			var c struct {
				Name string `json:"name"`
			}
			if json.Unmarshal(ev.Content, &c) == nil && c.Name != "" {
				return true
			}
		}
	}
	if id, err := e.store.GetStateEvent(ctx, roomID, "m.room.canonical_alias", ""); err == nil {
		if ev, err := e.store.GetEvent(ctx, id); err == nil {
			var c struct {
				Alias string `json:"alias"`
			}
			if json.Unmarshal(ev.Content, &c) == nil && c.Alias != "" {
				return true
			}
		}
	}
	return false
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

// buildKnockedRoom constructs the KnockedRoom section for a room the user has
// knocked on (MSC2409): the room's current state events under `knock_state`
// (mirroring the invite section's `invite_state`).
func (e *Engine) buildKnockedRoom(ctx context.Context, roomID string) KnockedRoom {
	kr := KnockedRoom{}
	stateRows, err := e.store.GetState(ctx, roomID)
	if err == nil {
		ids := make([]string, 0, len(stateRows))
		for _, s := range stateRows {
			ids = append(ids, s.EventID)
		}
		evs, _ := e.store.EventsByIDs(ctx, ids)
		for _, se := range evs {
			kr.KnockState.Events = append(kr.KnockState.Events, se.RawJSON)
		}
	}
	if kr.KnockState.Events == nil {
		kr.KnockState.Events = []json.RawMessage{}
	}
	return kr
}

// buildLeftRoom constructs the LeftRoom section. The timeline is capped at the
// user's leave position (archived rooms "only contain history from before the
// user left") and filtered by the room timeline filter; when the filter yields
// an empty timeline, `state` carries the leave-time state snapshot so clients
// still learn the leave (and the pre-leave state) — the spec's archived-room
// behaviour (cf. element-hq/synapse#16932).
func (e *Engine) buildLeftRoom(ctx context.Context, roomID string, opts SyncOptions, maxStream int64, ignored map[string]bool) (LeftRoom, error) {
	lr := LeftRoom{}
	filter := opts.Filter
	// Where the user left: only events before (and including) the leave are in
	// the archived timeline.
	upto := maxStream
	if m, err := e.store.GetMembership(ctx, roomID, opts.UserID); err == nil && m.Membership == "leave" && m.StreamOrdering > 0 {
		upto = m.StreamOrdering
	}
	from := opts.Since.Stream
	limit := 50
	if filter != nil && filter.TimelineLimitSet {
		limit = filter.TimelineLimit
	}
	if from == 0 {
		from = upto - int64(limit)
		if from < 0 {
			from = 0
		}
	}
	evs, err := e.store.EventsForRoom(ctx, roomID, from, upto, limit, "f")
	if err != nil {
		return lr, err
	}
	lr.Timeline.Events = make([]json.RawMessage, 0, len(evs))
	for _, ev := range evs {
		if filter != nil && !filter.keepTimeline(&ev) {
			continue
		}
		if ignored[ev.Sender] {
			continue
		}
		lr.Timeline.Events = append(lr.Timeline.Events, filter.applyEventFields(filter.renderEvent(&ev)))
	}
	// `state` must always be an array: it holds the state as of the leave when
	// the timeline is empty (the leave event + pre-leave state), and is empty
	// otherwise (spec regression test).
	lr.State.Events = []json.RawMessage{}
	// When the timeline is empty, fill `state` with the state as of the leave
	// event (leave event + pre-leave state) per the spec regression test. The
	// filter's room.state rules apply (sytest "When user joins and leaves a
	// room in the same batch, the full state is still included in the next
	// sync" filters state.types=["a.madeup.test.state"] and expects exactly
	// that one event in the leave section).
	if len(lr.Timeline.Events) == 0 {
		if m, err := e.store.GetMembership(ctx, roomID, opts.UserID); err == nil && m.EventID != "" {
			if rows, err := e.store.GetEventState(ctx, m.EventID); err == nil {
				ids := make([]string, 0, len(rows))
				for _, s := range rows {
					ids = append(ids, s.EventID)
				}
				stateEvs, _ := e.store.EventsByIDs(ctx, ids)
				lr.State.Events = make([]json.RawMessage, 0, len(stateEvs))
				for i := range stateEvs {
					if filter != nil && !filter.keepState(&stateEvs[i]) {
						continue
					}
					lr.State.Events = append(lr.State.Events, filter.applyEventFields(filter.renderEvent(&stateEvs[i])))
				}
			}
		}
	}
	return lr, nil
}

// waitForResync polls the room's partial_state flag for up to timeout,
// reporting whether the background resync completed within the window. It is
// used by eager /sync to include a room as soon as its partial-state resync
// finishes (instead of dropping the room entirely when the resync lands a
// moment after the sync request starts).
func (e *Engine) waitForResync(ctx context.Context, roomID string, timeout time.Duration) bool {
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		room, err := e.store.GetRoom(ctx, roomID)
		if err != nil {
			return false
		}
		if !room.PartialState {
			return true
		}
		select {
		case <-ctx.Done():
			return false
		case <-time.After(20 * time.Millisecond):
		}
	}
	return false
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
// redacted form (per the default room version redaction rules), and the
// redaction's event ID is exposed as unsigned.redacted_by (spec: a redacted
// event tells clients which redaction redacted it).
func clientEvent(row *storage.EventRow) json.RawMessage {
	return clientEventCore(row, row.Redacted)
}

// erasedClientEvent renders a client event whose sender has been erased: the
// content is always pruned, regardless of row.Redacted (spec §Erasure — a user
// who was not joined when the erased user's event was sent sees only its
// redacted form). No unsigned.redacted_by is attached: there is no redaction
// event behind the pruning.
func erasedClientEvent(row *storage.EventRow) json.RawMessage {
	return clientEventCore(row, true)
}

// selfDestructed reports whether the event's content declares an MSC2228
// org.matrix.self_destruct_after timestamp that has now passed. An expired
// event is served with its content pruned (the redacted form), hiding the
// message body — mirror of Synapse's expire_event → redact_event. Only
// non-state events honour the key.
func selfDestructed(row *storage.EventRow, now int64) bool {
	if row.StateKey != "" || isStateTypeSync(row.Type) {
		return false
	}
	var c struct {
		SelfDestructAfter int64 `json:"org.matrix.self_destruct_after"`
	}
	if err := json.Unmarshal(row.Content, &c); err != nil || c.SelfDestructAfter == 0 {
		return false
	}
	return now >= c.SelfDestructAfter
}

func clientEventCore(row *storage.EventRow, redact bool) json.RawMessage {
	if selfDestructed(row, time.Now().UnixMilli()) {
		redact = true
	}
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
	if redact {
		if rules, ok := roomver.Get(roomver.Default); ok {
			if red, err := events.Redact(row.RawJSON, rules); err == nil {
				if c, exists := red["content"]; exists {
					m["content"] = c
				}
			}
		}
		if row.Redacted && row.RedactedBy != "" {
			unsigned, _ := json.Marshal(map[string]any{"redacted_by": row.RedactedBy})
			// json.RawMessage (not []byte) so the marshalled unsigned object is
			// embedded as JSON rather than base64-encoded by the outer Marshal.
			m["unsigned"] = json.RawMessage(unsigned)
		}
	}
	if row.Redacts != "" {
		m["redacts"] = row.Redacts
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

// inviteIsHidden reports whether the invite for roomID must be hidden from the
// invitee's /sync: the sender is in their m.ignored_user_list, or the sender /
// sender's server is ignored under the MSC4155 invite-permission config.
func (e *Engine) inviteIsHidden(ctx context.Context, roomID, inviteeUserID, inviteeLocalpart string, ignored map[string]bool) bool {
	// The inviter is the sender of the room's m.room.member(invite) event for
	// the invitee.
	id, err := e.store.GetStateEvent(ctx, roomID, "m.room.member", inviteeUserID)
	if err != nil {
		return false
	}
	ev, err := e.store.GetEvent(ctx, id)
	if err != nil || ev == nil {
		return false
	}
	// m.ignored_user_list: the inviter is directly ignored.
	if ignored != nil && ignored[ev.Sender] {
		return true
	}
	// MSC4155: ignored_users / ignored_servers.
	hidden, err := e.store.InviteIsHiddenFromSync(ctx, inviteeLocalpart, ev.Sender, serverOfUser(ev.Sender))
	if err != nil {
		return false
	}
	return hidden
}

// serverOfUser extracts the server name from a Matrix user ID.
func serverOfUser(userID string) string {
	for i := len(userID) - 1; i >= 0; i-- {
		if userID[i] == ':' {
			return userID[i+1:]
		}
	}
	return ""
}
