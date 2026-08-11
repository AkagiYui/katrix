package csapi

import (
	"context"

	"github.com/AkagiYui/katrix/internal/events"
)

// broadcastPDU queues a locally-created event for delivery to every remote
// server with users in the room (spec "Transaction delivery": servers must
// send the events they create to all servers in the room).
func (a *API) broadcastPDU(ctx context.Context, roomID string, ev *events.Event) {
	if a.fed == nil {
		return
	}
	a.fed.BroadcastPDUToRoom(ctx, roomID, ev)
}

// broadcastDeviceListUpdate queues an m.device_list_update EDU for userID to
// every remote server that shares a room with the user. Called whenever the
// user's device list changes: a device is added (login, key upload) or removed
// (logout, device deletion). The receiver treats the EDU as a hint to query
// /keys/query; the device's key bundle is included when known so it can skip
// the round-trip.
//
// Each EDU carries a per-user monotonic `stream_id` (advancing the local
// outbound counter) and, on every update after the user's first, a `prev_id`
// naming the stream_id of the previous update (spec "Device lists": the EDU's
// prev_id lets a receiving server detect a lost update and re-fetch the
// device list). A remote server that joins mid-stream has no baseline and
// simply re-fetches; the prev_id contract matters to servers that were already
// tracking the user.
//
// Partial-state rooms (MSC3902) are included with a destination set derived
// from the send_join's servers_in_room list ∪ the known membership (see
// serversForRooms): while the membership is incomplete, the room's servers
// still need the update so the joining server can track the user's devices
// (mirror of Synapse's get_current_hosts_in_room_or_partial_state_approximation,
// which uses the servers known at join plus the servers of known members). The
// resync completion replay (broadcastDeviceListStateToRoom) re-sends updates
// that went to the wrong destination set.
func (a *API) broadcastDeviceListUpdate(ctx context.Context, userID, deviceID string, deleted bool) {
	if a.fed == nil {
		return
	}
	rooms, err := a.Store.RoomsForUser(ctx, userID)
	if err != nil || len(rooms) == 0 {
		return
	}
	prevID, streamID, err := a.Store.NextDeviceListSendStream(ctx, userID)
	if err != nil {
		// Fall back to the wall clock so a broadcast is never silently dropped:
		// the receiver only treats stream_id as an ordering hint (mirror of the
		// pre-counter behaviour).
		streamID = a.Now()
	}
	content := map[string]any{
		"user_id":   userID,
		"device_id": deviceID,
		"deleted":   deleted,
		// An opaque, monotonically increasing ordering hint per the spec
		// ("stream_id" must increase between updates for the same device).
		"stream_id": streamID,
	}
	// The spec's prev_id contract: every update after the first names its
	// predecessor, so receivers can detect a lost EDU. The very first update
	// omits prev_id entirely.
	if prevID > 0 {
		content["prev_id"] = []int64{prevID}
	}
	if !deleted && deviceID != "" {
		keys, err := a.Store.DeviceKeysForUsers(ctx, []string{userID})
		if err == nil {
			for _, k := range keys {
				if k.DeviceID == deviceID {
					content["keys"] = k.KeyJSON
					break
				}
			}
		}
		// The receiver may skip a /keys/query round-trip entirely; include the
		// device display name so the fetched key bundle is complete (mirror of
		// the key upload flow in sytest's device-list tests).
		if d, err := a.Store.GetDevice(ctx, a.LocalpartOf(userID), deviceID); err == nil && d.DisplayName != "" {
			content["device_display_name"] = d.DisplayName
		}
	}
	a.fed.BroadcastEDUToRooms(ctx, "m.device_list_update", content, rooms)
}

// roomIsPartial reports whether the room is currently partial-state (MSC3902).
func (a *API) roomIsPartial(ctx context.Context, roomID string) bool {
	room, err := a.Store.GetRoom(ctx, roomID)
	return err == nil && room.PartialState
}

// broadcastDeviceListForUser queues one m.device_list_update EDU per device of
// a local user to every remote server sharing a room with them. Used when the
// user joins a room whose membership makes their device list newly visible to
// the room's remote servers (spec: servers must send m.device_list_update to
// all servers who share a room with a local user, including when that user
// joins a room containing servers which are not already receiving updates).
func (a *API) broadcastDeviceListForUser(ctx context.Context, userID string) {
	if a.fed == nil {
		return
	}
	if !a.IsLocalUser(userID) {
		return
	}
	devices, err := a.Store.ListDevices(ctx, a.LocalpartOf(userID))
	if err != nil {
		return
	}
	// The per-device broadcast itself re-checks partial rooms per room, so this
	// is a plain fan-out over the user's devices.
	for _, d := range devices {
		a.broadcastDeviceListUpdate(ctx, userID, d.DeviceID, false)
	}
}

// broadcastDeviceListDelete queues an m.device_list_update EDU marked deleted
// (device_lists.changed semantics: the user's device list shrank, never `left`,
// which is reserved for users who stopped sharing every encrypted room) to
// every remote server sharing roomID. It is used when a
// local user leaves/ban/unbans a room: the user's own room list no longer
// contains the room (the membership row was already updated), so the generic
// broadcastDeviceListUpdate would miss the room's servers. The room's other
// members (e.g. on the remote server) still need to learn the user is no
// longer sharing the room.
//
// Partial-state rooms (MSC3902) defer the deleted-EDU: the departure is
// communicated by the leave PDU itself (broadcast to the room's servers), and
// a speculative deleted-EDU to the incomplete server set would be an
// unexpected side effect (the un-partial replay only re-sends updates for
// users still in the room, so the leaver is simply not re-advertised — mirror
// of Synapse's deferred-updates behaviour).
func (a *API) broadcastDeviceListDelete(ctx context.Context, userID, roomID string) {
	if a.fed == nil {
		return
	}
	if a.roomIsPartial(ctx, roomID) {
		return
	}
	prevID, streamID, err := a.Store.NextDeviceListSendStream(ctx, userID)
	if err != nil {
		streamID = a.Now()
	}
	content := map[string]any{
		"user_id":   userID,
		"device_id": "",
		"deleted":   true,
		"stream_id": streamID,
	}
	// A deleted-EDU is a per-user device-list update like any other: it carries
	// the per-user stream counter and names its predecessor so receivers can
	// detect a lost update.
	if prevID > 0 {
		content["prev_id"] = []int64{prevID}
	}
	a.fed.BroadcastEDUToRooms(ctx, "m.device_list_update", content, []string{roomID})
}

// notifyDeviceListPeers wakes the /sync requests of every local user who
// shares a room with userID. When a user's device list changes (key upload,
// new login, logout, device deletion), all of their room peers must be told,
// so the e2ee extension can emit device_lists.changed/left and they re-query
// /keys/query for the new device set. Without this, a peer with a long-polled
// sync parked on the notifier would never learn about the change and would
// keep sharing room keys only with the stale device set (a brand-new device
// of the changed user would then be left unable to decrypt).
func (a *API) notifyDeviceListPeers(ctx context.Context, userID string) {
	rooms, err := a.Store.RoomsForUser(ctx, userID)
	if err != nil {
		return
	}
	users := map[string]bool{userID: true} // the user's own other devices too
	for _, roomID := range rooms {
		members, err := a.Store.JoinedUserIDs(ctx, roomID)
		if err != nil {
			continue
		}
		for _, u := range members {
			if !a.IsLocalUser(u) {
				continue
			}
			users[u] = true
		}
	}
	for u := range users {
		a.Notifier.NotifyUser(u)
	}
}

// recordDeviceRemoval records a device-list change after a device is deleted
// (logout, delete-device). Per the spec, `device_lists.left` in /sync reports
// users with whom we no longer share any encrypted rooms — a purely
// membership-driven condition. Deleting a device (even the user's last one) is
// a device-identity update, so the user is reported in `device_lists.changed`
// and their room peers re-fetch the (shorter or empty) device list; reporting
// `left` would wrongly tell clients the user stopped sharing rooms and drop
// their remaining devices' keys (sytest "Local delete device changes appear in
// v2 /sync" expects a last-device deletion to surface in `changed`). The
// outbound EDU still carries `deleted: true` — the flag is authoritative for
// the device itself and lets receivers evict its cached keys.
func (a *API) recordDeviceRemoval(ctx context.Context, userID, localpart, deviceID string) {
	_, _ = a.Store.RecordDeviceListChange(ctx, userID, false)
	a.broadcastDeviceListUpdate(ctx, userID, deviceID, true)
	a.notifyDeviceListPeers(ctx, userID)
}

// presenceContentFor builds the m.presence EDU content for userID. A user with
// no presence row is reported as "online": the spec's presence default ("If
// this parameter is omitted then the client is automatically marked as online
// when it uses this API"), and the row only appears once the user first
// /syncs or PUTs /presence — a peer who joins before that must still learn the
// user is online (sytest "New federated private chats get full presence
// information (SYN-115)" seeds the event-stream token via the legacy /events
// endpoint, which never writes a presence row). It returns nil only when the
// user is unknown to this server.
func (a *API) presenceContentFor(ctx context.Context, userID string) map[string]any {
	presence := "online"
	statusMsg := ""
	if p, err := a.Store.GetPresence(ctx, userID); err == nil && p != nil {
		presence = p.Presence
		statusMsg = p.StatusMsg
	}
	content := map[string]any{
		"user_id":   userID,
		"presence":  presence,
		"stream_id": a.Now(),
	}
	if statusMsg != "" {
		content["status_msg"] = statusMsg
	}
	return content
}

// broadcastLocalPresence queues an m.presence EDU for a LOCAL user to every
// remote server sharing a room with the user. Callers (a PUT /presence, a
// first /sync set_presence, a room join) need not carry the presence row; a
// user with no row yet is broadcast as online (see presenceContentFor).
func (a *API) broadcastLocalPresence(ctx context.Context, userID string) {
	if a.fed == nil || !a.IsLocalUser(userID) {
		return
	}
	rooms, err := a.Store.RoomsForUser(ctx, userID)
	if err != nil || len(rooms) == 0 {
		return
	}
	a.fed.BroadcastEDUToRooms(ctx, "m.presence", a.presenceContentFor(ctx, userID), rooms)
}
