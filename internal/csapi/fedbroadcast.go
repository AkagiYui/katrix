package csapi

import (
	"context"

	"github.com/AkagiYui/katrix/internal/events"
	"github.com/AkagiYui/katrix/internal/storage"
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
func (a *API) broadcastDeviceListUpdate(ctx context.Context, userID, deviceID string, deleted bool) {
	if a.fed == nil {
		return
	}
	rooms, err := a.Store.RoomsForUser(ctx, userID)
	if err != nil || len(rooms) == 0 {
		return
	}
	content := map[string]any{
		"user_id":   userID,
		"device_id": deviceID,
		"deleted":   deleted,
		// An opaque, monotonically increasing ordering hint per the spec
		// ("stream_id" must increase between updates for the same device).
		"stream_id": a.Now(),
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
	}
	a.fed.BroadcastEDUToRooms(ctx, "m.device_list_update", content, rooms)
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
	for _, d := range devices {
		a.broadcastDeviceListUpdate(ctx, userID, d.DeviceID, false)
	}
}

// broadcastDeviceListDelete queues an m.device_list_update EDU marked deleted
// (device_lists.left) to every remote server sharing roomID. It is used when a
// local user leaves/ban/unbans a room: the user's own room list no longer
// contains the room (the membership row was already updated), so the generic
// broadcastDeviceListUpdate would miss the room's servers. The room's other
// members (e.g. on the remote server) still need to learn the user is no
// longer sharing the room.
func (a *API) broadcastDeviceListDelete(ctx context.Context, userID, roomID string) {
	if a.fed == nil {
		return
	}
	content := map[string]any{
		"user_id":   userID,
		"device_id": "",
		"deleted":   true,
		"stream_id": a.Now(),
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
// users who "no longer share rooms or have stopped uploading device keys" —
// i.e. users whose whole device list is gone. Deleting one device while the
// user retains other devices must be reported as `changed`, or clients would
// (incorrectly) drop the user entirely and stop sharing room keys with the
// remaining devices. Only when the deletion leaves the user with no devices at
// all is it reported as `left`.
func (a *API) recordDeviceRemoval(ctx context.Context, userID, localpart, deviceID string) {
	devices, err := a.Store.ListDevices(ctx, localpart)
	if err != nil {
		return
	}
	// DeleteDevice was already called: if the user has no remaining devices,
	// their whole device list is gone.
	isDelete := len(devices) == 0
	_, _ = a.Store.RecordDeviceListChange(ctx, userID, isDelete)
	a.broadcastDeviceListUpdate(ctx, userID, deviceID, isDelete)
	a.notifyDeviceListPeers(ctx, userID)
}

// broadcastPresence queues an m.presence EDU for userID to every remote server
// sharing a room with the user.
func (a *API) broadcastPresence(ctx context.Context, userID string, p *storage.PresenceRow) {
	if a.fed == nil {
		return
	}
	rooms, err := a.Store.RoomsForUser(ctx, userID)
	if err != nil || len(rooms) == 0 {
		return
	}
	content := map[string]any{
		"user_id":   userID,
		"presence":  p.Presence,
		"stream_id": a.Now(),
	}
	if p.StatusMsg != "" {
		content["status_msg"] = p.StatusMsg
	}
	a.fed.BroadcastEDUToRooms(ctx, "m.presence", content, rooms)
}
