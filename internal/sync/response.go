package sync

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"time"

	"github.com/AkagiYui/katrix/internal/events"
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
}

// EphemeralSection holds the ephemeral events for a joined room. The spec
// nests them under an `events` array (rooms.join.<room_id>.ephemeral.events),
// so the section is an object rather than a bare array.
type EphemeralSection struct {
	Events []json.RawMessage `json:"events"`
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
	// Lazy-load members: only include membership state events, and only for
	// senders present in the timeline.
	LazyLoadMembers bool
	// UnreadThreadNotifications: when true, per-thread unread notification
	// counts are returned under unread_thread_notifications and the main
	// unread_notifications counts exclude thread events (MSC3773/MSC3774).
	UnreadThreadNotifications bool
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
				Limit      *int     `json:"limit"`
				// UnreadThreadNotifications (MSC3773): per-thread unread counts.
				UnreadThreadNotifications bool `json:"unread_thread_notifications"`
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
		TimelineTypes:             obj.Room.Timeline.Types,
		TimelineNotTypes:          obj.Room.Timeline.NotTypes,
		TimelineSenders:           obj.Room.Timeline.Senders,
		TimelineNotSenders:        obj.Room.Timeline.NotSenders,
		LazyLoadMembers:           obj.Room.State.LazyLoadMembers,
		UnreadThreadNotifications: obj.Room.Timeline.UnreadThreadNotifications,
		IncludeLeave:              obj.Room.IncludeLeave,
		EventFields:               obj.EventFields,
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
	return f.TimelineLimitSet || f.TimelineLimit > 0 || f.LazyLoadMembers || f.UnreadThreadNotifications || f.IncludeLeave ||
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
		jr, err := e.buildJoinedRoom(ctx, roomID, opts, maxStream)
		if err != nil {
			return nil, err
		}
		rooms.Join[roomID] = jr
	}

	// Rooms the user is invited to (membership=invite).
	ignored := e.ignoredUsers(ctx, opts.Localpart)
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
			lr, err := e.buildLeftRoom(ctx, roomID, opts, maxStream)
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

	// Account data (global + per-room).
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
	peers := e.roomPeers(ctx, opts)
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

	// Next batch token is the current max stream.
	resp.NextBatch = Token{Stream: maxStream}.Encode()

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
		for _, u := range append(changed, newPeers...) {
			if u == opts.UserID || seen[u] || !peers[u] {
				continue
			}
			seen[u] = true
			userIDs = append(userIDs, u)
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

// buildJoinedRoom constructs the JoinedRoom section.
func (e *Engine) buildJoinedRoom(ctx context.Context, roomID string, opts SyncOptions, maxStream int64) (JoinedRoom, error) {
	jr := JoinedRoom{}
	filter := opts.Filter
	// Timeline limit: the filter's per-room timeline limit, else 50 (the spec
	// default /current window). The window is the room's recent history — the
	// *last* `limit` events — not the first `limit` events after the token
	// (mirror of Synapse's _load_filtered_recents, which paginates backwards
	// from the newest event).
	limit := 50
	if filter != nil && filter.TimelineLimit > 0 {
		limit = filter.TimelineLimit
	}

	// A room the user *transitioned into* after the sync token is a "newly
	// joined" room: its timeline is the room's recent history (not just the
	// events since the token) and is always marked limited, forcing the client
	// to back-paginate the pre-join history (spec + Synapse). A profile update
	// (an m.room.member with membership=join when the user was already joined,
	// e.g. a displayname change) does NOT trigger this — NewlyJoinedAfter
	// requires the join to be a real membership transition.
	newlyJoined := false
	if opts.Since.Stream > 0 {
		if joined, err := e.store.NewlyJoinedAfter(ctx, roomID, opts.UserID, opts.Since.Stream); err == nil {
			newlyJoined = joined
		}
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
		// Full-room window: paginate backward from the room's latest event,
		// accumulating batches until the FILTERED events fill `limit` or the
		// room's start is reached (mirror of Synapse's _load_filtered_recents
		// loop). The shared sync stream advances on other rooms' writes, so a
		// single stream-position anchor (to-limit) can land inside the room's
		// recent history and drop messages the client should receive; widening
		// on the filtered count (not the raw count) also keeps rooms whose raw
		// history is mostly state events from squeezing the message window. The
		// number of batches is bounded (Synapse's max_repeat = 5) so a room with
		// mostly filter-blocked history does not paginate to the room's start.
		const maxFillBatches = 5
		evs, err = e.store.EventsForRoom(ctx, roomID, from, to, rawLimit, "f")
		for b := 0; err == nil && filteredCount(evs, filter) < limit && from > 0 && b < maxFillBatches; b++ {
			prevFrom := from
			from -= int64(rawLimit)
			if from < 0 {
				from = 0
			}
			var batch []storage.EventRow
			batch, err = e.store.EventsForRoom(ctx, roomID, from, prevFrom, rawLimit, "f")
			if len(batch) == 0 {
				break
			}
			evs = append(batch, evs...)
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
			if trunc, terr := e.store.HasEventsBefore(ctx, roomID, from); terr == nil && trunc {
				countLimited = true
			}
		}
	} else if len(evs) > limit {
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
		raw := filter.applyEventFields(e.annotateTxn(ctx, prevContent(membershipAt(clientEvent(&ev), &ev), &ev), ev.EventID))
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
	// A limited timeline that contains any state event must also carry the
	// room's CURRENT state: Synapse's "always include current state in the
	// timeline" behaviour (filter_and_transform_events_for_client with
	// always_include_ids=current_state_ids) — when a limited window drops
	// events, clients still need the current state events to render the room
	// correctly. This covers both count-truncated windows and newly-joined /
	// full-room windows: a room whose join event predates a window that drops
	// to the newest `limit` events would otherwise never surface the user's own
	// membership (Complement's syncMembershipIn checks the timeline).
	// The current-state events not already in the timeline are appended (they
	// are the newest authoritative values).
	if (newlyJoined || countLimited) && len(evs) > 0 {
		hasState := false
		for _, ev := range evs {
			if ev.StateKey != "" {
				hasState = true
				break
			}
		}
		if hasState {
			if stateRows, err := e.store.GetState(ctx, roomID); err == nil {
				ids := make([]string, 0, len(stateRows))
				for _, s := range stateRows {
					ids = append(ids, s.EventID)
				}
				stateEvs, _ := e.store.EventsByIDs(ctx, ids)
				inTimeline := map[string]bool{}
				for _, re := range rendered {
					var obj map[string]json.RawMessage
					if json.Unmarshal(re.raw, &obj) == nil {
						var id string
						if json.Unmarshal(obj["event_id"], &id) == nil {
							inTimeline[id] = true
						}
					}
				}
				for _, se := range stateEvs {
					if inTimeline[se.EventID] {
						continue
					}
					if !filter.keepTimeline(&se) {
						continue
					}
					rendered = append(rendered, renderedEvent{raw: filter.applyEventFields(prevContent(clientEvent(&se), &se)), send: se.Sender, stream: se.StreamOrdering})
				}
			}
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
	if len(evs) > 0 {
		limited := newlyJoined || gapLimited || countLimited
		timeline.Limited = limited
		if limited {
			timeline.PrevBatch = Token{Stream: earliest - 1}.Encode()
		} else {
			timeline.PrevBatch = Token{Stream: to - 1}.Encode()
		}
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
			jr.State.Events = append(jr.State.Events, filter.applyEventFields(prevContent(clientEvent(&se), &se)))
		}
	} else if jr.State.Events == nil {
		// An incremental sync with no full_state still renders `state.events` as
		// an empty array (spec + clients expect the key to exist).
		jr.State.Events = []json.RawMessage{}
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
			if filter.lazyLoadMembers() && !lazyLoadMemberEvent(&se, senders) {
				continue
			}
			sa.Events = append(sa.Events, filter.applyEventFields(clientEvent(&se)))
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
	mainCount, mainHighlight := e.countFor(ctx, roomID, "", mainPos, opts, rules, joined)
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
			n, h := e.countFor(ctx, roomID, root, pos, opts, rules, joined)
			if n == 0 && h == 0 {
				continue
			}
			threadCounts[root] = UnreadNotifications{NotificationCount: &n, HighlightCount: &h}
		}
	}

	if opts.Filter != nil && opts.Filter.UnreadThreadNotifications {
		main := &UnreadNotifications{NotificationCount: &mainCount, HighlightCount: &mainHighlight}
		return main, threadCounts
	}
	// No thread filter: fold the thread counts into the main count.
	combinedCount, combinedHighlight := mainCount, mainHighlight
	for _, tc := range threadCounts {
		if tc.NotificationCount != nil {
			combinedCount += *tc.NotificationCount
		}
		if tc.HighlightCount != nil {
			combinedHighlight += *tc.HighlightCount
		}
	}
	return &UnreadNotifications{NotificationCount: &combinedCount, HighlightCount: &combinedHighlight}, nil
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
func (e *Engine) countFor(ctx context.Context, roomID, root string, since int64, opts SyncOptions, rules map[string]any, joined int64) (int, int) {
	evs, err := e.store.EventsForNotificationCount(ctx, roomID, root, since, 1000)
	if err != nil {
		return 0, 0
	}
	var notif, highlight int
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
	}
	return notif, highlight
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
		unsigned, _ := json.Marshal(map[string]any{"membership": membership})
		obj["unsigned"] = unsigned
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
func (e *Engine) buildLeftRoom(ctx context.Context, roomID string, opts SyncOptions, maxStream int64) (LeftRoom, error) {
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
		lr.Timeline.Events = append(lr.Timeline.Events, filter.applyEventFields(clientEvent(&ev)))
	}
	// `state` must always be an array: it holds the state as of the leave when
	// the timeline is empty (the leave event + pre-leave state), and is empty
	// otherwise (spec regression test).
	lr.State.Events = []json.RawMessage{}
	// When the timeline is empty, fill `state` with the state as of the leave
	// event (leave event + pre-leave state) per the spec regression test.
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
					lr.State.Events = append(lr.State.Events, clientEvent(&stateEvs[i]))
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
