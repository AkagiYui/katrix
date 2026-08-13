package csapi

import (
	"bytes"
	"context"
	"crypto/tls"
	"encoding/json"
	"fmt"
	"net/http"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/AkagiYui/katrix/internal/events"
	"github.com/AkagiYui/katrix/internal/homeserver"
	"github.com/AkagiYui/katrix/internal/httpx"
	"github.com/AkagiYui/katrix/internal/pushrules"
	"github.com/AkagiYui/katrix/internal/storage"
)

// This file implements the two halves of the Push Module's delivery side:
//
//  1. HTTP pusher delivery: when an event is persisted (client or federation
//     path), every joined local user whose push rules evaluate the event to
//     notify has their HTTP pushers POSTed to the configured push gateway with
//     the spec /_matrix/push/v1/notify payload. Delivery runs on a per-event
//     goroutine so a slow gateway never blocks the request that persisted the
//     event; rejected pushkeys (a gateway answering `{"rejected":[...]}`)
//     remove the pusher.
//
//  2. GET /notifications: the user's recent notifying events, shaped per the
//     spec (room_id, actions, event, read, ts, profile_tag).

// pushDispatcher delivers HTTP push notifications to pushers. It is a field on
// API so both the CS path (which owns the API) and the federation ingest path
// can trigger delivery. `a` is the owning API, set in New after construction.
type pushDispatcher struct {
	a  *API
	mu sync.Mutex
	// lastNotified tracks the highest event stream_ordering already dispatched
	// per room, so a path that sees an event already dispatched by another path
	// does not double-deliver. Guarded by mu.
	lastNotified map[string]int64
	http         *http.Client
}

func newPushDispatcher(insecure bool) *pushDispatcher {
	tr := http.DefaultTransport.(*http.Transport).Clone()
	if insecure {
		tr.TLSClientConfig = &tls.Config{InsecureSkipVerify: true} // #nosec G402 -- test-only flag
	}
	return &pushDispatcher{
		lastNotified: map[string]int64{},
		http:         &http.Client{Timeout: 15 * time.Second, Transport: tr},
	}
}

// NotifyInbound implements federation.PushNotifier: the federation ingest path
// calls it after persisting an inbound event so HTTP push notifications reach
// the room's local users and interested application services receive the event.
// The stream ordering is the persisted row's; the dispatcher dedups against the
// CS path (which may deliver the same event when it is a local echo).
func (d *pushDispatcher) NotifyInbound(ctx context.Context, ev *events.Event, roomID string, stream int64, rejected bool) {
	sk, _ := ev.StateKey()
	d.deliverNotifies(ctx, roomID, ev.EventID(), ev.Type(), ev.Sender(), sk, ev.Content(), stream, rejected)
	d.a.deliverASEvents(ctx, roomID, ev)
}

// deliverNotifies evaluates the event against each joined local user's push
// rules and dispatches to their HTTP pushers. It is called after the event has
// been persisted (and, for membership events, after the membership row
// reflects it). The dispatch is fire-and-forget: each notification runs on its
// own goroutine and the function returns immediately, so a slow or wedged push
// gateway never blocks the request that stored the event (a blocked handler
// would stall every later request on the same HTTP/1.1 keep-alive connection,
// and sytest's push tests time out waiting for their gateway). The dispatcher
// dedups against the federation ingest path via lastNotified before spawning.
//
// Rejected (soft-failed) events are never delivered (sytest "Rejected events
// are not pushed"), and an event never notifies its own sender (the evaluator
// drops sender==recipient).
func (d *pushDispatcher) deliverNotifies(ctx context.Context, roomID, eventID, eventType, sender, stateKey string, contentJSON []byte, stream int64, rejected bool) {
	if rejected || d.a == nil {
		return
	}
	a := d.a
	d.mu.Lock()
	if stream > 0 && d.lastNotified[roomID] >= stream {
		d.mu.Unlock()
		return
	}
	if stream > 0 {
		d.lastNotified[roomID] = stream
	}
	d.mu.Unlock()

	members, err := a.Store.Members(ctx, roomID, "join")
	if err != nil {
		return
	}
	joined := int64(len(members))
	var content map[string]any
	_ = json.Unmarshal(contentJSON, &content)

	evaluate := func(userID, localpart string) {
		if !a.IsLocalUser(userID) || userID == sender {
			return
		}
		rules := pushrules.LoadRules(ctx, a.Store, localpart)
		res := pushrules.Evaluate(rules, userID, localpart, pushrules.EventSnapshot{
			Type:        eventType,
			Sender:      sender,
			RoomID:      roomID,
			Content:     contentJSON,
			MemberCount: int(joined),
		})
		if !res.Notifies {
			return
		}
		// The dispatch runs after the persisting request has returned, so the
		// request's context is already cancelled by the time it fires. Use an
		// independent context or every push would fail at the first store/HTTP
		// call (sytest's push tests time out waiting for the gateway).
		go d.dispatchForUser(context.WithoutCancel(ctx), a, roomID, eventID, eventType, sender, stateKey, content, userID, localpart, res)
	}

	seen := map[string]bool{}
	for _, m := range members {
		seen[m.UserID] = true
		evaluate(m.UserID, a.LocalpartOf(m.UserID))
	}
	// An invite targets a user who is not (yet) a joined member, so the joined
	// scan above never reaches them — but they must still be pushed for the
	// invite (sytest "Invites are pushed" asserts the invitee's pusher fires
	// with user_is_target=true). A joined target (a displayname update, or the
	// user's own leave) is already covered by the joined scan.
	if eventType == "m.room.member" && stateKey != "" && stateKey != sender && !seen[stateKey] && a.IsLocalUser(stateKey) {
		evaluate(stateKey, a.LocalpartOf(stateKey))
	}
}

// dispatchForUser POSTs the notify payload to every HTTP pusher of the user.
// Per the spec, the payload carries the full notification (all event fields)
// plus the user's unread count and one devices entry per pusher (app_id,
// pushkey, pushkey_ts, data minus the url key, and the matched rule's tweaks).
func (d *pushDispatcher) dispatchForUser(ctx context.Context, a *API, roomID, eventID, eventType, sender, stateKey string, content map[string]any, userID, localpart string, res pushrules.EvalResult) {
	pushers, err := a.Store.ListPushers(ctx, localpart)
	if err != nil || len(pushers) == 0 {
		return
	}
	unread := a.totalUnreadCount(ctx, userID, localpart)

	notif := map[string]any{
		"event_id": eventID,
		"room_id":  roomID,
		"type":     eventType,
		"sender":   sender,
		"prio":     "high",
		"counts":   map[string]any{"unread": unread},
	}
	if eventType == "m.room.member" && stateKey != "" {
		var mc struct {
			Membership string `json:"membership"`
		}
		_ = json.Unmarshal(mustJSON(content), &mc)
		if mc.Membership != "" {
			notif["membership"] = mc.Membership
		}
		notif["user_is_target"] = stateKey == userID
	}
	if len(content) > 0 {
		notif["content"] = content
	}
	if name := a.pushRoomName(ctx, roomID); name != "" {
		notif["room_name"] = name
	}
	if dn := a.pushSenderDisplayName(ctx, roomID, sender); dn != "" {
		notif["sender_display_name"] = dn
	}

	for _, p := range pushers {
		switch p.Kind {
		case "email":
			// Email pushers (spec pusher kind "email") are not delivered inline:
			// the notification is recorded per room and the background worker
			// aggregates each user's pending rooms into one summary email once
			// the room's throttle window passes (see sendDueEmailSummaries).
			a.recordEmailPending(ctx, localpart, p.PushKey, roomID, eventID, eventType, sender, stateKey, content, notif)
			continue
		case "http":
		default:
			continue
		}
		var data map[string]any
		_ = json.Unmarshal(p.Data, &data)
		url, _ := data["url"].(string)
		if url == "" {
			continue
		}
		// The device's `data` is the pusher data minus the `url` key (spec).
		deviceData := map[string]any{}
		for k, v := range data {
			if k == "url" {
				continue
			}
			deviceData[k] = v
		}
		device := map[string]any{
			"app_id":     p.AppID,
			"pushkey":    p.PushKey,
			"pushkey_ts": p.CreatedTS / 1000,
			"data":       deviceData,
		}
		// The device's `tweaks` object is always present: the matched rule's
		// set_tweak values, with a default "highlight": false when the rule
		// carries no tweaks (mirror of Synapse's tweaks_for_actions, which
		// always includes highlight — sytest asserts the devices[0].tweaks key).
		tweaks := res.Tweaks
		if tweaks == nil {
			tweaks = map[string]any{}
		}
		if _, ok := tweaks["highlight"]; !ok {
			tweaks["highlight"] = false
		}
		device["tweaks"] = tweaks
		nn := make(map[string]any, len(notif)+2)
		for k, v := range notif {
			nn[k] = v
		}
		nn["id"] = eventID // deprecated but still sent by Synapse
		nn["devices"] = []any{device}
		body, _ := json.Marshal(map[string]any{"notification": nn})
		if d.postNotify(ctx, url, body) {
			// The gateway said the pushkey is not valid: remove the pusher
			// (spec: "Homeservers must cease sending notification requests for
			// these pushkeys and remove the associated pushers").
			_ = a.Store.DeletePusher(context.Background(), localpart, p.AppID, p.PushKey)
		}
	}
}

// ---- Email pushers ----
//
// The email pusher (spec pusher kind "email": "a pusher that emails the user
// with unread notifications") delivers summary emails through the configured
// SMTP server instead of a push gateway. Notifications are not emailed
// inline: dispatchForUser records the pending notification per room in
// email_pusher_state, and the background worker (StartEmailPushWorker /
// sendDueEmailSummaries) sends one aggregated email per user once a room's
// throttle window has passed — mirroring Synapse's email pusher, which
// coalesces everything that arrived during a room's throttle interval into a
// single mail and backs off (×144, capped at 24h, reset after a 12h quiet
// gap) so a busy room does not spam the inbox.

// recordEmailPending upserts the per-room pending state for a user's email
// pusher. The email address is the pusher's pushkey (spec); when the user has
// several email pushers the same summary email would be sent to each, so only
// the first is recorded. SMTP must be configured or the push is dropped
// (mirror of the /requestToken guard: email sending needs smtp.host).
func (a *API) recordEmailPending(ctx context.Context, localpart, pushkey, roomID, eventID, eventType, sender, stateKey string, content map[string]any, notif map[string]any) {
	if a.Config.SMTP.Host == "" {
		return
	}
	st := storage.EmailPushState{
		UserLocalpart:  localpart,
		RoomID:         roomID,
		PendingEventID: eventID,
		PendingSender:  sender,
		RoomName:       roomNameOf(notif),
		Unread:         unreadOf(notif),
	}
	// The sender's display name and the message body come from the event
	// content; membership events carry no body, so the summary falls back to a
	// membership description.
	var mc struct {
		Body       string `json:"body"`
		Membership string `json:"membership"`
	}
	_ = json.Unmarshal(mustJSON(content), &mc)
	if dn := senderDNOf(notif); dn != "" {
		st.PendingSenderDN = dn
	}
	switch {
	case mc.Body != "":
		st.PendingBody = mc.Body
	case eventType == "m.room.member" && mc.Membership != "":
		st.PendingBody = mc.Membership
	}
	// First pending event for the room is due immediately (the 1s base
	// throttle only applies to INSERT; later pending rows retain the row's
	// grown throttle via the upsert). The throttle bookkeeping is shared with
	// the worker: ready_at is pushed to last_sent + throttle after a mail
	// goes out, so a busy room coalesces instead of emailing per message.
	st.ThrottleMS = 1000
	st.ReadyAt = a.Now()
	st.LastEventTS = a.Now()
	_ = a.Store.UpsertEmailPushPending(ctx, st)
}

// roomNameOf returns the notification's room_name, if the dispatcher resolved
// one.
func roomNameOf(notif map[string]any) string {
	name, _ := notif["room_name"].(string)
	return name
}

// senderDNOf returns the notification's sender_display_name, if resolved.
func senderDNOf(notif map[string]any) string {
	dn, _ := notif["sender_display_name"].(string)
	return dn
}

// unreadOf returns the notification's counts.unread badge value.
func unreadOf(notif map[string]any) int64 {
	counts, _ := notif["counts"].(map[string]any)
	n, _ := counts["unread"].(float64)
	return int64(n)
}

// StartEmailPushWorker runs the background email-pusher worker: every
// emailPushInterval it sends the due summary emails (rooms whose ready_at has
// passed). It exits when ctx is cancelled.
func (a *API) StartEmailPushWorker(ctx context.Context) {
	go func() {
		ticker := time.NewTicker(a.emailPushInterval)
		defer ticker.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
				_ = a.sendDueEmailSummaries(context.WithoutCancel(ctx))
			}
		}
	}()
}

// sendDueEmailSummaries sends one aggregated summary email per user covering
// every pending room whose ready_at has passed, then advances the throttle
// bookkeeping. It is also the test hook: tests drive it directly (with a
// shortened interval) instead of waiting for the worker tick. A failed send
// backs the room off (throttle ×2, re-armed) so a wedged SMTP server does not
// cause a retry storm.
func (a *API) sendDueEmailSummaries(ctx context.Context) error {
	if a.Config.SMTP.Host == "" {
		return nil
	}
	due, err := a.Store.DueEmailPushStates(ctx, a.Now())
	if err != nil {
		return err
	}
	// Group the due rows per user; each user gets one email listing every room.
	byUser := map[string][]storage.EmailPushState{}
	for _, st := range due {
		byUser[st.UserLocalpart] = append(byUser[st.UserLocalpart], st)
	}
	for localpart, rooms := range byUser {
		// The user's email pushers: the summary is delivered to each pushkey
		// that has a pending room (a user may have multiple email addresses).
		pushers, err := a.Store.ListPushers(ctx, localpart)
		if err != nil {
			continue
		}
		emails := map[string]bool{}
		for _, p := range pushers {
			if p.Kind == "email" && p.PushKey != "" {
				emails[p.PushKey] = true
			}
		}
		subject, text := a.emailSummary(localpart, rooms)
		for to := range emails {
			if err := a.sendEmailMessage(ctx, to, subject, text); err == nil {
				// The mail went out: clear every pending room. Failed sends
				// leave the rows pending and back them off.
				for _, st := range rooms {
					_ = a.Store.ClearEmailPushPending(ctx, localpart, st.RoomID, a.Now())
				}
			} else {
				for _, st := range rooms {
					_ = a.Store.BackoffEmailPushPending(ctx, localpart, st.RoomID, a.Now())
				}
			}
		}
	}
	return nil
}

// emailSummary renders the subject and body of a user's aggregated summary
// email. A single room yields "[server] Alice says: hello" (the classic
// notification subject); several rooms yield a count subject and a body
// listing each room with its sender, message preview and unread count.
func (a *API) emailSummary(localpart string, rooms []storage.EmailPushState) (subject, text string) {
	app := a.ServerName()
	if len(rooms) == 1 {
		room := rooms[0]
		from := room.PendingSenderDN
		if from == "" {
			from = room.PendingSender
		}
		if from != "" {
			subject = fmt.Sprintf("[%s] %s says: %s", app, from, room.PendingBody)
		} else {
			subject = fmt.Sprintf("[%s] You have unread messages in %s", app, roomName(room))
		}
		var b strings.Builder
		fmt.Fprintf(&b, "Room: %s\n", roomName(room))
		if room.PendingSenderDN != "" {
			fmt.Fprintf(&b, "From: %s\n", room.PendingSenderDN)
		}
		if room.PendingBody != "" {
			fmt.Fprintf(&b, "Message: %s\n", room.PendingBody)
		}
		if room.Unread > 0 {
			fmt.Fprintf(&b, "Unread notifications: %d\n", room.Unread)
		}
		return subject, b.String()
	}
	subject = fmt.Sprintf("[%s] You have unread messages in %d rooms", app, len(rooms))
	var b strings.Builder
	for _, room := range rooms {
		fmt.Fprintf(&b, "• %s", roomName(room))
		if room.PendingSenderDN != "" {
			fmt.Fprintf(&b, " — %s: %s", room.PendingSenderDN, room.PendingBody)
		}
		if room.Unread > 0 {
			fmt.Fprintf(&b, " (%d unread)", room.Unread)
		}
		b.WriteString("\n")
	}
	return subject, b.String()
}

// roomName renders a room's display name for email summaries, falling back to
// the room ID.
func roomName(st storage.EmailPushState) string {
	if st.RoomName != "" {
		return st.RoomName
	}
	return st.RoomID
}

// postNotify POSTs the notify payload to the push gateway, returning whether
// the gateway rejected the pushkey. Best-effort: a transport error is not
// treated as a rejection (the spec says to retry with backoff, which a
// fire-and-forget dispatcher approximates by leaving the pusher in place).
func (d *pushDispatcher) postNotify(ctx context.Context, url string, body []byte) bool {
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, url, bytes.NewReader(body))
	if err != nil {
		return false
	}
	req.Header.Set("Content-Type", "application/json")
	resp, err := d.http.Do(req)
	if err != nil {
		return false
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return false
	}
	var out struct {
		Rejected []string `json:"rejected"`
	}
	_ = json.NewDecoder(resp.Body).Decode(&out)
	return len(out.Rejected) > 0
}

// refreshBadge re-dispatches a badge-count-only push to the user's HTTP
// pushers after their read receipt advances. The push gateway's badge (the
// notification `counts.unread`) must track reads even when no new event
// arrives, so the receipt is re-pushed with the updated count (sytest "Test
// that a message is pushed" asserts a second push with unread 0 after the
// user reads the message). The payload mirrors a normal notification, reusing
// the receipted event's identity, with the freshly computed unread count.
func (d *pushDispatcher) refreshBadge(ctx context.Context, a *API, roomID, userID, localpart, eventID string) {
	pushers, err := a.Store.ListPushers(ctx, localpart)
	if err != nil || len(pushers) == 0 {
		return
	}
	unread := a.totalUnreadCount(ctx, userID, localpart)

	ev, err := a.Store.GetEvent(ctx, eventID)
	if err != nil {
		return
	}
	notif := map[string]any{
		"event_id": eventID,
		"room_id":  roomID,
		"type":     ev.Type,
		"sender":   ev.Sender,
		"prio":     "high",
		"counts":   map[string]any{"unread": unread},
	}
	var content map[string]any
	_ = json.Unmarshal(ev.Content, &content)
	if len(content) > 0 {
		notif["content"] = content
	}
	if name := a.pushRoomName(ctx, roomID); name != "" {
		notif["room_name"] = name
	}
	if dn := a.pushSenderDisplayName(ctx, roomID, ev.Sender); dn != "" {
		notif["sender_display_name"] = dn
	}

	for _, p := range pushers {
		if p.Kind != "http" {
			continue
		}
		var data map[string]any
		_ = json.Unmarshal(p.Data, &data)
		url, _ := data["url"].(string)
		if url == "" {
			continue
		}
		deviceData := map[string]any{}
		for k, v := range data {
			if k == "url" {
				continue
			}
			deviceData[k] = v
		}
		device := map[string]any{
			"app_id":     p.AppID,
			"pushkey":    p.PushKey,
			"pushkey_ts": p.CreatedTS / 1000,
			"data":       deviceData,
		}
		nn := make(map[string]any, len(notif)+2)
		for k, v := range notif {
			nn[k] = v
		}
		nn["id"] = eventID
		nn["devices"] = []any{device}
		body, _ := json.Marshal(map[string]any{"notification": nn})
		if d.postNotify(ctx, url, body) {
			_ = a.Store.DeletePusher(context.Background(), localpart, p.AppID, p.PushKey)
		}
	}
}

// mustJSON re-encodes a map to JSON bytes, returning "{}" on failure.
func mustJSON(m map[string]any) []byte {
	b, err := json.Marshal(m)
	if err != nil {
		return []byte("{}")
	}
	return b
}

// totalUnreadCount computes the user's unread notification count summed across
// every room they are joined to (the push notification `counts.unread` badge,
// mirror of Synapse's badge count). Rooms without a read position or without a
// stored ruleset contribute nothing.
func (a *API) totalUnreadCount(ctx context.Context, userID, localpart string) int {
	roomIDs, err := a.Store.RoomsForUser(ctx, userID)
	if err != nil {
		return 0
	}
	total := 0
	for _, roomID := range roomIDs {
		if n, _, ok := a.syncEngine.SlidingUnreadCounts(ctx, roomID, userID, localpart); ok {
			total += n
		}
	}
	return total
}

// pushRoomName returns the room's display name for a push notification: the
// m.room.name value, else the m.room.canonical_alias value, else "" (the spec
// allows either; Synapse's push_tools uses the canonical "name" which prefers
// the name and falls back to an alias).
func (a *API) pushRoomName(ctx context.Context, roomID string) string {
	if id, err := a.Store.GetStateEvent(ctx, roomID, "m.room.name", ""); err == nil {
		if ev, err := a.Store.GetEvent(ctx, id); err == nil {
			var c struct {
				Name string `json:"name"`
			}
			if json.Unmarshal(ev.Content, &c) == nil && c.Name != "" {
				return c.Name
			}
		}
	}
	if id, err := a.Store.GetStateEvent(ctx, roomID, "m.room.canonical_alias", ""); err == nil {
		if ev, err := a.Store.GetEvent(ctx, id); err == nil {
			var c struct {
				Alias string `json:"alias"`
			}
			if json.Unmarshal(ev.Content, &c) == nil && c.Alias != "" {
				return c.Alias
			}
		}
	}
	return ""
}

// pushSenderDisplayName returns the sender's display name in the room (their
// m.room.member content displayname), falling back to "".
func (a *API) pushSenderDisplayName(ctx context.Context, roomID, sender string) string {
	if id, err := a.Store.GetStateEvent(ctx, roomID, "m.room.member", sender); err == nil {
		if ev, err := a.Store.GetEvent(ctx, id); err == nil {
			var c struct {
				DisplayName string `json:"displayname"`
			}
			if json.Unmarshal(ev.Content, &c) == nil && c.DisplayName != "" {
				return c.DisplayName
			}
		}
	}
	return ""
}

// ---- GET /notifications ----

// notificationsHandler handles GET /_matrix/client/v3/notifications (spec §Push
// Notifications: "Get notifications"). It returns the user's notifications for
// events after their latest read receipt, each shaped {room_id, actions, event,
// read, ts, profile_tag} (sytest "Notifications can be viewed with GET
// /notifications" asserts those keys, including profile_tag present).
func (a *API) notificationsHandler(w http.ResponseWriter, r *http.Request) {
	auth, _ := homeserver.AuthFrom(r.Context())
	roomIDs, err := a.Store.RoomsForUser(r.Context(), auth.UserID)
	if err != nil {
		httpx.WriteError(w, httpx.ErrUnknown(err.Error()))
		return
	}
	rules := pushrules.LoadRules(r.Context(), a.Store, auth.Localpart)

	type notifRow struct {
		RoomID  string
		Stream  int64
		TS      int64
		Event   storage.EventRow
		Actions []any
		Read    bool
		Tag     string
	}
	var notifs []notifRow

	for _, roomID := range roomIDs {
		readPos, hasRead := a.readPosition(r.Context(), roomID, auth.UserID)
		joined := int64(0)
		if users, err := a.Store.JoinedUserIDs(r.Context(), roomID); err == nil {
			joined = int64(len(users))
		}
		// Scan the room's recent events (newest first) and evaluate each against
		// the user's rules; notifying events become notifications.
		evs, err := a.Store.EventsForRoom(r.Context(), roomID, 0, 0, 500, "b")
		if err != nil {
			continue
		}
		for _, ev := range evs {
			// The user's own membership events (their join) do not notify them.
			if ev.Type == "m.room.member" && ev.StateKey == auth.UserID {
				continue
			}
			res := pushrules.Evaluate(rules, auth.UserID, auth.Localpart, pushrules.EventSnapshot{
				Type:        ev.Type,
				Sender:      ev.Sender,
				RoomID:      roomID,
				Content:     ev.Content,
				MemberCount: int(joined),
			})
			if !res.Notifies {
				continue
			}
			notifs = append(notifs, notifRow{
				RoomID:  roomID,
				Stream:  ev.StreamOrdering,
				TS:      ev.OriginServerTS,
				Event:   ev,
				Actions: actionsOf(res),
				Read:    hasRead && ev.StreamOrdering <= readPos,
			})
		}
	}

	// Newest first (the most recent notifications come first per Synapse).
	sort.Slice(notifs, func(i, j int) bool { return notifs[i].Stream > notifs[j].Stream })

	out := make([]map[string]any, 0, len(notifs))
	for _, n := range notifs {
		out = append(out, map[string]any{
			"room_id":     n.RoomID,
			"actions":     n.Actions,
			"event":       json.RawMessage(notifEventJSON(&n.Event)),
			"read":        n.Read,
			"ts":          n.TS,
			"profile_tag": n.Tag,
		})
	}
	httpx.WriteJSON(w, http.StatusOK, map[string]any{"notifications": out})
}

// readPosition returns the user's latest unthreaded m.read receipt stream
// position in a room (falling back to their join position), and whether one
// exists.
func (a *API) readPosition(ctx context.Context, roomID, userID string) (int64, bool) {
	rcs, err := a.Store.ReadReceiptsForUserInRoom(ctx, roomID, userID)
	if err != nil {
		return 0, false
	}
	best := int64(0)
	for _, rc := range rcs {
		if rc.ReceiptType != "m.read" || rc.ThreadID != "" {
			continue
		}
		pos := rc.StreamID
		if so, err := a.Store.EventStreamOrdering(ctx, rc.EventID); err == nil && so > 0 {
			pos = so
		}
		if pos > best {
			best = pos
		}
	}
	if best > 0 {
		return best, true
	}
	if m, err := a.Store.GetMembership(ctx, roomID, userID); err == nil && m.StreamOrdering > 0 {
		return m.StreamOrdering, true
	}
	return 0, false
}

// actionsOf renders the actions of a matched rule as a JSON-ready list: the
// actions GET /notifications returns per notification (spec: "The actions to
// perform when the conditions for this rule are met"). A highlight appends the
// highlight tweak.
func actionsOf(res pushrules.EvalResult) []any {
	actions := []any{"notify"}
	if res.Highlights {
		actions = append(actions, map[string]any{"set_tweak": "highlight", "value": true})
	}
	return actions
}

// notifEventJSON renders a stored event as a client event for the notifications
// response (the event without a room_id, matching Synapse's
// format_event_for_client_v2_without_room_id).
func notifEventJSON(ev *storage.EventRow) []byte {
	m := map[string]any{
		"type":             ev.Type,
		"content":          json.RawMessage(ev.Content),
		"sender":           ev.Sender,
		"origin_server_ts": ev.OriginServerTS,
		"event_id":         ev.EventID,
	}
	if ev.StateKey != "" {
		m["state_key"] = ev.StateKey
	}
	b, _ := json.Marshal(m)
	return b
}
