package csapi

import (
	"context"

	"github.com/AkagiYui/katrix/internal/storage"
)

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
