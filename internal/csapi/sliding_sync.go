package csapi

import (
	"context"
	"encoding/json"
	"net/http"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/AkagiYui/katrix/internal/homeserver"
	"github.com/AkagiYui/katrix/internal/httpx"
	"github.com/AkagiYui/katrix/internal/storage"
)

// The MSC4186 / MSC3575 sliding-sync endpoint
// (POST /_matrix/client/unstable/org.matrix.simplified_msc3575/sync).
//
// The matrix-rust-sdk's native sliding sync (SlidingSyncVersion::Native) uses
// this endpoint exclusively: the room-list connection (conn_id "room-list")
// drives the RoomListService with a list plus the account-data/receipts/typing
// extensions, and the encryption connection (conn_id "encryption") carries the
// to-device + e2ee extensions. A server that returns no lists/rooms section
// leaves the client's room list permanently NotLoaded, which is exactly what
// the katrix stub did before this implementation.
//
// The wire format follows ruma's sync_events::v5 and the reference
// implementation (matrix-org/sliding-sync). Only the subset the rust SDK
// exercises is implemented; unsupported extension configs are ignored.

// ---- request shapes ----

type slidingSyncRequest struct {
	ConnID            string                        `json:"conn_id"`
	Lists             map[string]slidingSyncListReq `json:"lists"`
	RoomSubscriptions map[string]slidingSyncSubReq  `json:"room_subscriptions"`
	Extensions        slidingSyncExtReq             `json:"extensions"`
}

type slidingSyncListReq struct {
	Ranges        [][2]int64          `json:"ranges"`
	TimelineLimit *int                `json:"timeline_limit"`
	RequiredState [][2]string         `json:"required_state"`
	Filters       *slidingSyncFilters `json:"filters"`
}

type slidingSyncSubReq struct {
	RequiredState [][2]string `json:"required_state"`
	TimelineLimit *int        `json:"timeline_limit"`
}

type slidingSyncFilters struct {
	IsDM         *bool             `json:"is_dm"`
	IsEncrypted  *bool             `json:"is_encrypted"`
	IsInvite     *bool             `json:"is_invite"`
	RoomTypes    []json.RawMessage `json:"room_types"`
	NotRoomTypes []json.RawMessage `json:"not_room_types"`
}

type slidingSyncExtReq struct {
	ToDevice *struct {
		Enabled bool    `json:"enabled"`
		Limit   *int    `json:"limit"`
		Since   *string `json:"since"`
	} `json:"to_device"`
	E2EE *struct {
		Enabled bool `json:"enabled"`
	} `json:"e2ee"`
	AccountData *struct {
		Enabled bool `json:"enabled"`
	} `json:"account_data"`
	Receipts *struct {
		Enabled bool `json:"enabled"`
	} `json:"receipts"`
	Typing *struct {
		Enabled bool `json:"enabled"`
	} `json:"typing"`
	ThreadSubs *struct {
		Enabled bool `json:"enabled"`
		Limit   *int `json:"limit"`
	} `json:"io.element.msc4308.thread_subscriptions"`
}

// ---- response shapes ----

type slidingSyncResponse struct {
	Pos        string                         `json:"pos"`
	Lists      map[string]slidingSyncListResp `json:"lists,omitempty"`
	Rooms      map[string]slidingSyncRoomResp `json:"rooms,omitempty"`
	Extensions *slidingSyncExtResp            `json:"extensions,omitempty"`
}

type slidingSyncListResp struct {
	Count int `json:"count"`
}

type slidingSyncRoomResp struct {
	Name          *string           `json:"name,omitempty"`
	Avatar        *string           `json:"avatar,omitempty"`
	Initial       bool              `json:"initial,omitempty"`
	RequiredState []json.RawMessage `json:"required_state,omitempty"`
	// Timeline is always emitted, even when empty: clients (and test
	// harnesses) rely on the field being present to locate a room's event
	// list in the response.
	Timeline      []json.RawMessage `json:"timeline"`
	PrevBatch     string            `json:"prev_batch,omitempty"`
	Limited       bool              `json:"limited,omitempty"`
	NumLive       *int64            `json:"num_live,omitempty"`
	BumpStamp     *int64            `json:"bump_stamp,omitempty"`
	JoinedCount   *int              `json:"joined_count,omitempty"`
	InvitedCount  *int              `json:"invited_count,omitempty"`
	Membership    string            `json:"membership,omitempty"`
	StrippedState []json.RawMessage `json:"invite_state,omitempty"`
	// Unread notification counts (flat form, per the sync v5 / sliding-sync
	// room schema — the spec's UnreadNotificationsCount object is flattened
	// into the room). The matrix-rust-sdk's NotificationClient reads these to
	// decide which events to surface as notifications.
	HighlightCount    int `json:"highlight_count,omitempty"`
	NotificationCount int `json:"notification_count,omitempty"`
}

type slidingSyncExtResp struct {
	ToDevice    *slidingSyncToDeviceResp    `json:"to_device,omitempty"`
	E2EE        *slidingSyncE2EEResp        `json:"e2ee,omitempty"`
	AccountData *slidingSyncAccountDataResp `json:"account_data,omitempty"`
	Receipts    *slidingSyncReceiptsResp    `json:"receipts,omitempty"`
	Typing      *slidingSyncTypingResp      `json:"typing,omitempty"`
	ThreadSubs  *slidingSyncThreadSubsResp  `json:"io.element.msc4308.thread_subscriptions,omitempty"`
}

type slidingSyncToDeviceResp struct {
	NextBatch string            `json:"next_batch"`
	Events    []json.RawMessage `json:"events,omitempty"`
}

type slidingSyncE2EEResp struct {
	DeviceLists                  slidingSyncDeviceLists `json:"device_lists,omitempty"`
	DeviceOneTimeKeysCount       map[string]int         `json:"device_one_time_keys_count,omitempty"`
	DeviceUnusedFallbackKeyTypes *[]string              `json:"device_unused_fallback_key_types,omitempty"`
}

type slidingSyncDeviceLists struct {
	Changed []string `json:"changed,omitempty"`
	Left    []string `json:"left,omitempty"`
}

type slidingSyncAccountDataResp struct {
	Global []json.RawMessage            `json:"global,omitempty"`
	Rooms  map[string][]json.RawMessage `json:"rooms,omitempty"`
}

type slidingSyncReceiptsResp struct {
	Rooms map[string]json.RawMessage `json:"rooms,omitempty"`
}

type slidingSyncTypingResp struct {
	Rooms map[string]json.RawMessage `json:"rooms,omitempty"`
}

type slidingSyncThreadSubsResp struct {
	Subscribed   map[string]map[string]map[string]any `json:"subscribed,omitempty"`
	Unsubscribed map[string]map[string]any            `json:"unsubscribed,omitempty"`
}

// registerSlidingSync wires the MSC4186 sliding-sync endpoint.
func (a *API) registerSlidingSync(mux *http.ServeMux) {
	mux.HandleFunc("POST /_matrix/client/unstable/org.matrix.simplified_msc3575/sync", a.RequireAuth(a.SlidingSync))
}

// SlidingSync handles POST /_matrix/client/unstable/org.matrix.simplified_msc3575/sync.
func (a *API) SlidingSync(w http.ResponseWriter, r *http.Request) {
	auth, _ := homeserver.AuthFrom(r.Context())
	q := r.URL.Query()
	posStr := q.Get("pos")
	since := parsePos(posStr)

	timeout := time.Duration(0)
	if v := q.Get("timeout"); v != "" {
		if n, err := strconv.Atoi(v); err == nil && n >= 0 && n <= 60000 {
			timeout = time.Duration(n) * time.Millisecond
		}
	}

	var req slidingSyncRequest
	if err := httpx.DecodeJSON(w, r, &req); err != nil {
		httpx.WriteError(w, err)
		return
	}

	// set_presence mirrors /v3/sync (default online; "offline" leaves the
	// stored presence untouched — see the /v3/sync handler for why).
	sp := q.Get("set_presence")
	if sp == "" {
		sp = "online"
	}
	if sp != "offline" {
		if changed, err := a.Store.SetPresence(r.Context(), auth.UserID, sp, "", a.Now()); err == nil && changed {
			a.broadcastLocalPresence(r.Context(), auth.UserID)
			// Wake local room peers so their parked long-polls deliver the
			// change promptly (mirror of the /v3/sync handler).
			a.notifyDeviceListPeers(r.Context(), auth.UserID)
		}
	}

	compute := func() *slidingSyncResponse {
		return a.buildSlidingSync(r.Context(), auth, since, &req)
	}

	resp := compute()
	// Long-poll: when the client provided a pos and a timeout, park on the
	// notifier until something changes or the timeout elapses.
	if since > 0 && timeout > 0 && !slidingSyncHasData(resp) {
		wait, cancel := a.Notifier.Wait(auth.UserID)
		defer cancel()
		// Re-check after registering the waiter (lost-wakeup race).
		resp = compute()
		if !slidingSyncHasData(resp) {
			select {
			case <-wait:
			case <-time.After(timeout):
			case <-r.Context().Done():
				return
			}
			resp = compute()
		}
	}
	httpx.WriteJSON(w, http.StatusOK, resp)
}

// sameRequiredState reports whether two required_state lists are equal (same
// (type, state_key) pairs, order-insensitive).
func sameRequiredState(a, b [][2]string) bool {
	if len(a) != len(b) {
		return false
	}
	seen := map[[2]string]bool{}
	for _, p := range a {
		seen[p] = true
	}
	for _, p := range b {
		if !seen[p] {
			return false
		}
	}
	return true
}

// parsePos decodes a sliding-sync position into a stream value (0 when absent
// or malformed). The position is the shared sync stream's "s<N>" form; a sync
// next_batch token may carry a trailing to-device cursor ("s<N>t<M>"), which
// is ignored here.
func parsePos(pos string) int64 {
	pos = strings.TrimPrefix(strings.TrimSpace(pos), "s")
	if i := strings.IndexByte(pos, 't'); i >= 0 {
		pos = pos[:i]
	}
	if pos == "" {
		return 0
	}
	n, err := strconv.ParseInt(pos, 10, 64)
	if err != nil || n < 0 {
		return 0
	}
	return n
}

// slidingSyncHasData reports whether a computed response carries anything
// beyond the empty baseline. The long-poll loop uses it to decide whether to
// park.
func slidingSyncHasData(resp *slidingSyncResponse) bool {
	if len(resp.Rooms) > 0 || len(resp.Lists) > 0 {
		return true
	}
	if resp.Extensions == nil {
		return false
	}
	if resp.Extensions.ToDevice != nil && len(resp.Extensions.ToDevice.Events) > 0 {
		return true
	}
	if resp.Extensions.E2EE != nil &&
		(len(resp.Extensions.E2EE.DeviceLists.Changed) > 0 || len(resp.Extensions.E2EE.DeviceLists.Left) > 0) {
		return true
	}
	if resp.Extensions.AccountData != nil &&
		(len(resp.Extensions.AccountData.Global) > 0 || len(resp.Extensions.AccountData.Rooms) > 0) {
		return true
	}
	if resp.Extensions.ThreadSubs != nil && len(resp.Extensions.ThreadSubs.Subscribed) > 0 {
		return true
	}
	return false
}

// buildSlidingSync computes one sliding-sync response.
func (a *API) buildSlidingSync(ctx context.Context, auth *homeserver.Auth, since int64, req *slidingSyncRequest) *slidingSyncResponse {
	maxStream, _ := a.Store.MaxStreamOrdering(ctx)
	resp := &slidingSyncResponse{
		Pos:   "s" + strconv.FormatInt(maxStream, 10),
		Rooms: map[string]slidingSyncRoomResp{},
	}

	// ---- Room subscriptions ----
	// Processed first: a subscription's config (timeline_limit, required_state)
	// always wins over the coarser list config for the same room, because the
	// client subscribes to follow a specific room closely. A room that was
	// already delivered on this connection (through a list) but is now
	// subscribed with a different (typically higher) config must be re-delivered
	// with initial=true and the subscription's timeline, so the client replaces
	// its local copy instead of merging incrementally.
	connID := req.ConnID
	for roomID, sub := range req.RoomSubscriptions {
		entry := a.slidingRoomEntryFor(ctx, roomID, auth.UserID)
		if entry == nil {
			continue
		}
		forceInitial := false
		if cfg, ok := a.ssConns.wasDelivered(auth.UserID, connID, roomID); ok {
			// Re-deliver when the subscription asks for a different config than
			// the room was last delivered with.
			subLimit := 1
			if sub.TimelineLimit != nil {
				subLimit = *sub.TimelineLimit
			}
			if subLimit > cfg.timelineLimit || !sameRequiredState(sub.RequiredState, cfg.requiredState) {
				forceInitial = true
			}
		}
		a.ssConns.setSubscribed(auth.UserID, connID, roomID, true)
		if rr := a.slidingRoomResult(ctx, *entry, auth.UserID, auth.Localpart, since, maxStream, sub.TimelineLimit, sub.RequiredState, forceInitial); rr != nil {
			resp.Rooms[roomID] = *rr
			cfg := ssDeliveredConfig{timelineLimit: 1}
			if sub.TimelineLimit != nil {
				cfg.timelineLimit = *sub.TimelineLimit
			}
			cfg.requiredState = sub.RequiredState
			a.ssConns.markDelivered(auth.UserID, connID, roomID, cfg)
		}
	}

	// ---- Lists ----
	// Lists fill in any room not already returned by a subscription.
	if len(req.Lists) > 0 {
		resp.Lists = map[string]slidingSyncListResp{}
		// One ordered, filtered view over the user's rooms (joined by recency,
		// then invited) shared by all lists. Each list applies its own filters
		// and ranges.
		entries := a.slidingRoomEntries(ctx, auth.UserID)
		prevWindow := a.ssConns.listWindow(auth.UserID, connID)
		window := make(map[string]bool, len(entries))
		for name, list := range req.Lists {
			filtered := filterRoomEntries(entries, list.Filters)
			// The total number of rooms matching the list, ignoring the range.
			resp.Lists[name] = slidingSyncListResp{Count: len(filtered)}
			for _, rng := range list.Ranges {
				start, end := rng[0], rng[1]
				if end < start {
					end = start
				}
				if start >= int64(len(filtered)) {
					continue
				}
				if end >= int64(len(filtered)) {
					end = int64(len(filtered)) - 1
				}
				for i := start; i <= end; i++ {
					entry := filtered[i]
					window[entry.roomID] = true
					if _, ok := resp.Rooms[entry.roomID]; ok {
						continue
					}
					// A room entering the list window on an incremental sync
					// (first appearance, or re-entry after having scrolled out of
					// range) is new to the client and must be delivered as
					// initial=true with a timeline anchored at its latest event,
					// per MSC4186. The membership-stream ordering alone cannot
					// detect this: a room that entered just before `since` (e.g.
					// the syncing user's own join) would otherwise yield an empty
					// incremental window.
					forceInitial := since > 0 && !prevWindow[entry.roomID]
					if cfg, ok := a.ssConns.wasDelivered(auth.UserID, connID, entry.roomID); ok {
						// Re-deliver when the list's config asks for more than the
						// room was last delivered with (e.g. a higher timeline
						// limit), mirroring the subscription path.
						listLimit := 1
						if list.TimelineLimit != nil {
							listLimit = *list.TimelineLimit
						}
						if listLimit > cfg.timelineLimit || !sameRequiredState(list.RequiredState, cfg.requiredState) {
							forceInitial = true
						}
					}
					if rr := a.slidingRoomResult(ctx, entry, auth.UserID, auth.Localpart, since, maxStream, list.TimelineLimit, list.RequiredState, forceInitial); rr != nil {
						resp.Rooms[entry.roomID] = *rr
						cfg := ssDeliveredConfig{timelineLimit: 1}
						if list.TimelineLimit != nil {
							cfg.timelineLimit = *list.TimelineLimit
						}
						cfg.requiredState = list.RequiredState
						a.ssConns.markDelivered(auth.UserID, connID, entry.roomID, cfg)
					}
				}
			}
		}
		a.ssConns.setListWindow(auth.UserID, connID, window)
	}

	// ---- Extensions ----
	ext := a.slidingExtensions(ctx, auth, since, req)
	if ext != nil {
		resp.Extensions = ext
	}
	return resp
}

// roomEntry is a room in the sliding-window ordering.
type roomEntry struct {
	roomID     string
	membership string // join | invite | leave
	bump       int64  // recency stamp
	stream     int64  // membership stream ordering (for "new since" checks)
}

// slidingRoomEntries returns the user's joined + invited rooms ordered by
// recency (most recently active first), with invited rooms after joined ones.
func (a *API) slidingRoomEntries(ctx context.Context, userID string) []roomEntry {
	joined, _ := a.Store.RoomsForUser(ctx, userID)
	entries := make([]roomEntry, 0, len(joined)+8)
	for _, roomID := range joined {
		bump := int64(0)
		if ev, err := a.Store.LatestEvent(ctx, roomID); err == nil {
			bump = ev.StreamOrdering
		}
		m := membershipStream(ctx, a.Store, roomID, userID)
		entries = append(entries, roomEntry{roomID: roomID, membership: "join", bump: bump, stream: m})
	}
	sort.SliceStable(entries, func(i, j int) bool { return entries[i].bump > entries[j].bump })

	invited, _ := a.Store.InvitedRooms(ctx, userID)
	for _, roomID := range invited {
		entries = append(entries, roomEntry{roomID: roomID, membership: "invite", bump: 0, stream: membershipStream(ctx, a.Store, roomID, userID)})
	}
	return entries
}

// slidingRoomEntryFor fetches a single room's entry (for room subscriptions).
func (a *API) slidingRoomEntryFor(ctx context.Context, roomID, userID string) *roomEntry {
	m, err := a.Store.GetMembership(ctx, roomID, userID)
	if err != nil {
		return nil
	}
	switch m.Membership {
	case "join", "invite", "leave", "ban":
	default:
		return nil
	}
	bump := int64(0)
	if ev, err := a.Store.LatestEvent(ctx, roomID); err == nil {
		bump = ev.StreamOrdering
	}
	return &roomEntry{roomID: roomID, membership: m.Membership, bump: bump, stream: m.StreamOrdering}
}

func membershipStream(ctx context.Context, store *storage.Store, roomID, userID string) int64 {
	m, err := store.GetMembership(ctx, roomID, userID)
	if err != nil {
		return 0
	}
	return m.StreamOrdering
}

// filterRoomEntries applies a list's filters to the ordered room list.
func filterRoomEntries(entries []roomEntry, f *slidingSyncFilters) []roomEntry {
	if f == nil {
		return entries
	}
	var out []roomEntry
	for _, e := range entries {
		if f.IsInvite != nil && (*f.IsInvite) != (e.membership == "invite") {
			continue
		}
		out = append(out, e)
	}
	return out
}

// slidingRoomResult builds a room's result section. forceInitial forces the
// room to be treated as newly-returned (initial=true with a timeline anchored
// at the room's latest event) even on an incremental sync — used when a room
// subscription overrides the config a room was previously delivered with.
func (a *API) slidingRoomResult(ctx context.Context, entry roomEntry, userID, localpart string, since, maxStream int64, timelineLimit *int, requiredState [][2]string, forceInitial bool) *slidingSyncRoomResp {
	roomID := entry.roomID
	limit := 1
	if timelineLimit != nil && *timelineLimit >= 0 {
		limit = *timelineLimit
	}
	rr := &slidingSyncRoomResp{Membership: entry.membership, Timeline: []json.RawMessage{}}

	// initial: the first time the client sees the room (initial sync, or the
	// membership relationship is new), or the server is re-delivering it with a
	// different config.
	initial := since == 0 || entry.stream > since || forceInitial
	rr.Initial = initial

	// bump_stamp: latest event stream ordering (recency).
	if ev, err := a.Store.LatestEvent(ctx, roomID); err == nil {
		rr.BumpStamp = &ev.StreamOrdering
	}

	switch entry.membership {
	case "join":
		// Counts.
		joined, invited := 0, 0
		if members, err := a.Store.Members(ctx, roomID, ""); err == nil {
			for _, m := range members {
				switch m.Membership {
				case "join":
					joined++
				case "invite":
					invited++
				}
			}
		}
		rr.JoinedCount = &joined
		rr.InvitedCount = &invited

		// Unread notification counts (flat spec v5 room fields). Computed from
		// the user's push rules and read receipts, mirroring /v3/sync.
		if n, h, ok := a.syncEngine.SlidingUnreadCounts(ctx, roomID, userID, localpart); ok && (n > 0 || h > 0) {
			rr.NotificationCount = n
			rr.HighlightCount = h
		}

		// Timeline: on initial sync (or a room newly appearing in the sliding
		// window) return the most recent `limit` events; on a plain incremental
		// sync every event after `since`. The window is anchored at the room's own
		// latest event for new rooms — the shared sync stream advances globally
		// (key uploads, other rooms' events), so a `since` based on it can already
		// be past this room's newest events and would otherwise yield an empty
		// window. Fetch one extra event to detect a truncated window
		// (limited=true) precisely.
		newRoom := since == 0 || entry.stream > since || forceInitial
		var evs []storage.EventRow
		if newRoom {
			roomMax := maxStream
			if ev, err := a.Store.LatestEvent(ctx, roomID); err == nil {
				roomMax = ev.StreamOrdering
			}
			from := roomMax - int64(limit) - 1
			if from < 0 {
				from = 0
			}
			evs, _ = a.Store.EventsForRoom(ctx, roomID, from, roomMax, limit+1, "f")
		} else {
			// An incremental delta is never bounded by timeline_limit (the limit
			// only caps the initial window of a newly-delivered room, mirroring
			// the reference sliding-sync implementation, which appends every live
			// event to the timeline). The client must receive all missed events —
			// including membership transitions — or its view of the room drifts:
			// a member whose join was truncated out of the window is never
			// surfaced again (the timeline never re-delivers it, and $LAZY only
			// covers senders of the returned timeline).
			evs, _ = a.Store.EventsForRoom(ctx, roomID, since, maxStream, 1<<30, "f")
		}
		// Keep only the most recent `limit` events (the window is anchored at
		// the newest, so the extra event is the oldest). Truncation marks the
		// room limited=true and only applies to the initial window: an
		// incremental delta carries the full set of missed events.
		if newRoom && len(evs) > limit {
			rr.Limited = true
			evs = evs[len(evs)-limit:]
		}
		senders := map[string]bool{}
		render := a.ssPrevContentRenderer(ctx, roomID, maxStream)
		for i := range evs {
			ev := render(&evs[i])
			rr.Timeline = append(rr.Timeline, ev)
			senders[evs[i].Sender] = true
		}
		if len(evs) > 0 {
			if newRoom {
				// The oldest visible event's position; back-pagination goes
				// before it.
				rr.PrevBatch = "s" + strconv.FormatInt(evs[0].StreamOrdering-1, 10)
			} else {
				rr.PrevBatch = "s" + strconv.FormatInt(since, 10)
			}
		}
		// num_live: events that "just occurred" since the previous sync. Only
		// events returned on a plain incremental sync (the room was already in
		// the window) are live; a new room's timeline is historical.
		if !newRoom && since > 0 {
			numLive := int64(len(evs))
			rr.NumLive = &numLive
		}

		// Required state (expand $ME/$LAZY).
		rr.RequiredState = a.slidingRequiredState(ctx, roomID, userID, senders, requiredState, maxStream)

		// Room name/avatar from state.
		if id, err := a.Store.GetStateEvent(ctx, roomID, "m.room.name", ""); err == nil {
			if ev, err := a.Store.GetEvent(ctx, id); err == nil {
				var c struct {
					Name string `json:"name"`
				}
				if json.Unmarshal(ev.Content, &c) == nil && c.Name != "" {
					rr.Name = &c.Name
				}
			}
		}
		if id, err := a.Store.GetStateEvent(ctx, roomID, "m.room.avatar", ""); err == nil {
			if ev, err := a.Store.GetEvent(ctx, id); err == nil {
				var c struct {
					URL string `json:"url"`
				}
				if json.Unmarshal(ev.Content, &c) == nil && c.URL != "" {
					rr.Avatar = &c.URL
				}
			}
		}

	case "invite":
		// Invited rooms carry the stripped invite state; the client has no
		// timeline access yet.
		rr.StrippedState = a.slidingInviteState(ctx, roomID, maxStream)
	case "leave", "ban":
		// A leave just needs to be surfaced so clients drop the room.
		rr.Initial = true
	}

	return rr
}

// slidingRequiredState expands a required_state request into the matching
// current state events. $ME and $LAZY are expanded per MSC4186: $ME is the
// syncing user's own membership, $LAZY the memberships of timeline senders.
// "*" state keys match every state event of that type.
func (a *API) slidingRequiredState(ctx context.Context, roomID, userID string, timelineSenders map[string]bool, req [][2]string, maxStream int64) []json.RawMessage {
	if len(req) == 0 {
		return nil
	}
	stateRows, err := a.Store.GetState(ctx, roomID)
	if err != nil {
		return nil
	}
	ids := make([]string, 0, len(stateRows))
	for _, s := range stateRows {
		ids = append(ids, s.EventID)
	}
	evs, _ := a.Store.EventsByIDs(ctx, ids)
	byKey := map[string]*storage.EventRow{}
	var ordered []*storage.EventRow
	for i := range evs {
		ev := &evs[i]
		key := ev.Type + "\x00" + ev.StateKey
		if _, ok := byKey[key]; !ok {
			ordered = append(ordered, ev)
		}
		byKey[key] = ev
	}
	render := a.ssPrevContentRenderer(ctx, roomID, maxStream)
	seen := map[string]bool{}
	var out []json.RawMessage
	add := func(ev *storage.EventRow) {
		if ev == nil || seen[ev.EventID] {
			return
		}
		seen[ev.EventID] = true
		out = append(out, render(ev))
	}
	for _, rs := range req {
		typ, stateKey := rs[0], rs[1]
		switch stateKey {
		case "$ME":
			if typ == "m.room.member" {
				add(byKey["m.room.member\x00"+userID])
			}
		case "$LAZY":
			if typ == "m.room.member" {
				for sender := range timelineSenders {
					add(byKey["m.room.member\x00"+sender])
				}
			}
		case "*":
			for _, ev := range ordered {
				if ev.Type == typ {
					add(ev)
				}
			}
		default:
			add(byKey[typ+"\x00"+stateKey])
		}
	}
	return out
}

// slidingInviteState builds the stripped state for an invited room (the same
// events /v3/sync delivers under rooms.invite.<room>.invite_state).
func (a *API) slidingInviteState(ctx context.Context, roomID string, maxStream int64) []json.RawMessage {
	stateRows, err := a.Store.GetState(ctx, roomID)
	if err != nil {
		return nil
	}
	ids := make([]string, 0, len(stateRows))
	for _, s := range stateRows {
		ids = append(ids, s.EventID)
	}
	evs, _ := a.Store.EventsByIDs(ctx, ids)
	render := a.ssPrevContentRenderer(ctx, roomID, maxStream)
	out := make([]json.RawMessage, 0, len(evs))
	for i := range evs {
		out = append(out, render(&evs[i]))
	}
	return out
}

// slidingExtensions builds the extensions response.
func (a *API) slidingExtensions(ctx context.Context, auth *homeserver.Auth, since int64, req *slidingSyncRequest) *slidingSyncExtResp {
	if req == nil {
		return nil
	}
	var ext slidingSyncExtResp
	hasAny := false

	if req.Extensions.ToDevice != nil && req.Extensions.ToDevice.Enabled {
		tdSince := int64(0)
		if req.Extensions.ToDevice.Since != nil {
			if n, err := strconv.ParseInt(strings.TrimPrefix(*req.Extensions.ToDevice.Since, "s"), 10, 64); err == nil && n >= 0 {
				tdSince = n
			}
		}
		// Prune the messages the client already acknowledged (its incoming
		// cursor names the last delivered message); anything above the cursor
		// stays queued so a client killed before processing the previous
		// response still gets them on restart.
		_ = a.Store.PruneToDevice(ctx, auth.UserID, auth.DeviceID, tdSince)
		msgs, next, err := a.Store.DequeueToDeviceSince(ctx, auth.UserID, auth.DeviceID, tdSince)
		if err == nil {
			evs := make([]json.RawMessage, 0, len(msgs))
			for _, m := range msgs {
				evs = append(evs, ssToDeviceEvent(&m))
			}
			ext.ToDevice = &slidingSyncToDeviceResp{NextBatch: strconv.FormatInt(next, 10), Events: evs}
			hasAny = true
		}
	}

	if req.Extensions.E2EE != nil && req.Extensions.E2EE.Enabled {
		e := &slidingSyncE2EEResp{}
		// One-time key counts for the requesting device.
		counts, _ := a.Store.OneTimeKeyCounts(ctx, auth.UserID, auth.DeviceID)
		if len(counts) > 0 {
			e.DeviceOneTimeKeysCount = counts
		}
		// Unused fallback key algorithms (the SDK uploads a fallback key when
		// this is empty; its presence advertises fallback-key support).
		if algos, err := a.Store.UnusedFallbackKeyAlgorithms(ctx, auth.UserID, auth.DeviceID); err == nil {
			if len(algos) == 0 {
				algos = []string{}
			}
			e.DeviceUnusedFallbackKeyTypes = &algos
		}
		// Device-list changes for room peers (same semantics as /v3/sync).
		if changed, left, err := a.Store.DeviceListChangesSince(ctx, since); err == nil && (len(changed) > 0 || len(left) > 0) {
			peers := a.slidingRoomPeers(ctx, auth.UserID)
			peers[auth.UserID] = true
			var ch []string
			for _, u := range changed {
				if peers[u] {
					ch = append(ch, u)
				}
			}
			if len(ch) > 0 || len(left) > 0 {
				e.DeviceLists = slidingSyncDeviceLists{Changed: ch, Left: left}
			}
		}
		ext.E2EE = e
		hasAny = true
	}

	if req.Extensions.AccountData != nil && req.Extensions.AccountData.Enabled {
		rows, err := a.Store.AccountDataSince(ctx, auth.Localpart, since)
		if err == nil {
			ad := &slidingSyncAccountDataResp{}
			rooms := map[string][]json.RawMessage{}
			for _, arow := range rows {
				if since == 0 && storage.IsAccountDataDeleted(arow.Content) {
					continue
				}
				ev := ssAccountDataEvent(arow.Type, arow.Content)
				if arow.RoomID == "" {
					ad.Global = append(ad.Global, ev)
				} else {
					rooms[arow.RoomID] = append(rooms[arow.RoomID], ev)
				}
			}
			if len(rooms) > 0 {
				ad.Rooms = rooms
			}
			if len(ad.Global) > 0 || len(ad.Rooms) > 0 {
				ext.AccountData = ad
				hasAny = true
			}
		}
	}

	if req.Extensions.Receipts != nil && req.Extensions.Receipts.Enabled {
		receipts, err := a.Store.ReceiptsSince(ctx, auth.UserID, since)
		if err == nil && len(receipts) > 0 {
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
			rooms := map[string]json.RawMessage{}
			for roomID, evMap := range byRoom {
				eph, _ := json.Marshal(map[string]any{"type": "m.receipt", "content": evMap})
				rooms[roomID] = eph
			}
			ext.Receipts = &slidingSyncReceiptsResp{Rooms: rooms}
			hasAny = true
		}
	}

	if req.Extensions.Typing != nil && req.Extensions.Typing.Enabled {
		rooms := map[string]json.RawMessage{}
		for roomID := range a.typingRooms(ctx, auth.UserID) {
			users := a.Typing.TypingUsers(roomID)
			if len(users) == 0 {
				continue
			}
			eph, _ := json.Marshal(map[string]any{"type": "m.typing", "content": map[string]any{"user_ids": users}})
			rooms[roomID] = eph
		}
		if len(rooms) > 0 {
			ext.Typing = &slidingSyncTypingResp{Rooms: rooms}
			hasAny = true
		}
	}

	if req.Extensions.ThreadSubs != nil && req.Extensions.ThreadSubs.Enabled {
		subs, err := a.Store.ThreadSubscriptionsSince(ctx, auth.Localpart, since)
		if err == nil && len(subs) > 0 {
			subscribed := map[string]map[string]map[string]any{}
			for _, sub := range subs {
				room := subscribed[sub.RoomID]
				if room == nil {
					room = map[string]map[string]any{}
					subscribed[sub.RoomID] = room
				}
				room[sub.ThreadRootID] = map[string]any{
					"bump_stamp": sub.BumpStamp,
					"automatic":  sub.Automatic,
				}
			}
			ext.ThreadSubs = &slidingSyncThreadSubsResp{Subscribed: subscribed}
			hasAny = true
		}
	}

	if !hasAny {
		return nil
	}
	return &ext
}

// slidingRoomPeers returns the set of user IDs sharing a joined room with the
// given user.
func (a *API) slidingRoomPeers(ctx context.Context, userID string) map[string]bool {
	roomIDs, err := a.Store.RoomsForUser(ctx, userID)
	if err != nil {
		return nil
	}
	out := map[string]bool{}
	for _, roomID := range roomIDs {
		users, err := a.Store.JoinedUserIDs(ctx, roomID)
		if err != nil {
			continue
		}
		for _, u := range users {
			out[u] = true
		}
	}
	return out
}

// typingRooms returns the joined room IDs the user is in (the scope for the
// typing extension).
func (a *API) typingRooms(ctx context.Context, userID string) map[string]bool {
	roomIDs, err := a.Store.RoomsForUser(ctx, userID)
	if err != nil {
		return nil
	}
	out := make(map[string]bool, len(roomIDs))
	for _, id := range roomIDs {
		out[id] = true
	}
	return out
}

// ---- event rendering ----

// ssClientEvent renders a stored PDU as the client-visible sliding-sync event
// shape (type, content, sender, origin_server_ts, event_id, state_key).
func ssClientEvent(row *storage.EventRow) json.RawMessage {
	m := map[string]any{
		"type":             row.Type,
		"content":          json.RawMessage(row.Content),
		"sender":           row.Sender,
		"origin_server_ts": row.OriginServerTS,
		"event_id":         row.EventID,
	}
	// A state event always carries its state_key — even when empty (e.g.
	// m.room.encryption, m.room.name). The ruma deserializer only recognises
	// an event as state when the state_key field is present; omitting it (as an
	// empty string would be if skipped) drops the event entirely, which is how
	// the rust SDK ends up reporting an encrypted room as unencrypted.
	if row.StateKey != "" || isStateEventType(row.Type) {
		m["state_key"] = row.StateKey
	}
	b, _ := json.Marshal(m)
	return b
}

// ssPrevContentRenderer returns a function that renders an event row the same
// way ssClientEvent does, additionally attaching unsigned.prev_content and
// unsigned.prev_sender to m.room.member events: the content (and sender) of the
// previous member event for the same state_key. Per the spec, "Previous
// membership can be retrieved from the prev_content object on an event"; it
// lets clients render transitions (invite -> join, join -> leave). The /v3/sync
// engine does the same for its timeline/state events.
func (a *API) ssPrevContentRenderer(ctx context.Context, roomID string, upto int64) func(*storage.EventRow) json.RawMessage {
	history, err := a.Store.MemberEvents(ctx, roomID, upto)
	if err != nil {
		return ssClientEvent
	}
	return func(row *storage.EventRow) json.RawMessage {
		ev := ssClientEvent(row)
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

// isStateEventType reports whether the event type is a known state event type.
// State events carry a state_key field in the client-visible format even when
// the key is the empty string.
func isStateEventType(eventType string) bool {
	switch eventType {
	case "m.room.create", "m.room.power_levels", "m.room.join_rules",
		"m.room.history_visibility", "m.room.name", "m.room.topic",
		"m.room.member", "m.room.third_party_invite", "m.room.canonical_alias",
		"m.room.aliases", "m.room.encryption", "m.room.tombstone",
		"m.room.server_acl", "m.room.pinned_events", "m.room.avatar":
		return true
	}
	return false
}

// ssToDeviceEvent renders a to-device message in the extension's event shape.
func ssToDeviceEvent(m *storage.ToDeviceMessage) json.RawMessage {
	b, _ := json.Marshal(map[string]any{
		"type":    m.Type,
		"sender":  m.Sender,
		"content": json.RawMessage(m.Content),
	})
	return b
}

// ssAccountDataEvent renders an account-data row as an event.
func ssAccountDataEvent(eventType string, content []byte) json.RawMessage {
	b, _ := json.Marshal(map[string]any{
		"type":    eventType,
		"content": json.RawMessage(content),
	})
	return b
}
