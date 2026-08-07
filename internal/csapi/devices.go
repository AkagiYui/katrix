package csapi

import (
	"encoding/json"
	"errors"
	"net/http"

	"github.com/AkagiYui/katrix/internal/homeserver"
	"github.com/AkagiYui/katrix/internal/httpx"
	"github.com/AkagiYui/katrix/internal/storage"
)

// registerDevices wires P1 device-management routes.
func (a *API) registerDevices(mux *http.ServeMux) {
	mux.HandleFunc("GET /_matrix/client/v3/devices", a.RequireUserAuth(a.ListDevices))
	mux.HandleFunc("GET /_matrix/client/v3/devices/{deviceID}", a.RequireUserAuth(a.GetDevice))
	mux.HandleFunc("PUT /_matrix/client/v3/devices/{deviceID}", a.RequireUserAuth(a.UpdateDevice))
	mux.HandleFunc("DELETE /_matrix/client/v3/devices/{deviceID}", a.RequireUserAuth(a.DeleteDevice))
}

// ListDevices handles GET /_matrix/client/v3/devices.
func (a *API) ListDevices(w http.ResponseWriter, r *http.Request) {
	auth, _ := homeserver.AuthFrom(r.Context())
	devs, err := a.Store.ListDevices(r.Context(), auth.Localpart)
	if err != nil {
		httpx.WriteError(w, httpx.ErrUnknown(err.Error()))
		return
	}
	out := make([]map[string]any, 0, len(devs))
	for _, d := range devs {
		out = append(out, deviceJSON(d))
	}
	httpx.WriteJSON(w, http.StatusOK, map[string]any{"devices": out})
}

// GetDevice handles GET /_matrix/client/v3/devices/{deviceID}.
func (a *API) GetDevice(w http.ResponseWriter, r *http.Request) {
	auth, _ := homeserver.AuthFrom(r.Context())
	deviceID := r.PathValue("deviceID")
	d, err := a.Store.GetDevice(r.Context(), auth.Localpart, deviceID)
	if err != nil {
		if errors.Is(err, storage.ErrNotFound) {
			httpx.WriteError(w, httpx.ErrNotFound("device not found"))
			return
		}
		httpx.WriteError(w, httpx.ErrUnknown(err.Error()))
		return
	}
	httpx.WriteJSON(w, http.StatusOK, deviceJSON(*d))
}

type updateDeviceRequest struct {
	DisplayName *string `json:"display_name"`
}

// UpdateDevice handles PUT /_matrix/client/v3/devices/{deviceID}.
func (a *API) UpdateDevice(w http.ResponseWriter, r *http.Request) {
	auth, _ := homeserver.AuthFrom(r.Context())
	deviceID := r.PathValue("deviceID")
	var req updateDeviceRequest
	if err := httpx.DecodeJSON(w, r, &req); err != nil {
		httpx.WriteError(w, err)
		return
	}
	name := ""
	if req.DisplayName != nil {
		name = *req.DisplayName
	}
	if err := a.Store.UpdateDeviceDisplayName(r.Context(), auth.Localpart, deviceID, name); err != nil {
		if errors.Is(err, storage.ErrNotFound) {
			httpx.WriteError(w, httpx.ErrNotFound("device not found"))
			return
		}
		httpx.WriteError(w, httpx.ErrUnknown(err.Error()))
		return
	}
	// A device display-name change alters the user's device list as seen by
	// other servers: broadcast an m.device_list_update EDU and record a local
	// device-list change so /sync delivers device_lists.changed (spec: servers
	// must send device-list updates whenever a device's metadata changes).
	_, _ = a.Store.RecordDeviceListChange(r.Context(), auth.UserID, false)
	a.broadcastDeviceListUpdate(r.Context(), auth.UserID, deviceID, false)
	a.notifyDeviceListPeers(r.Context(), auth.UserID)
	httpx.WriteJSON(w, http.StatusOK, httpx.EmptyJSON)
}

// DeleteDevice handles DELETE /_matrix/client/v3/devices/{deviceID}. Per spec
// this must pass User-Interactive Authentication; the client supplies an auth
// dict with their password (or completes an SSO stage). The current device may
// be deleted (it is logged out), which matches spec behaviour (use /logout for
// the convenience path).
//
// The UIA session is bound to the device being deleted: reusing a session
// started for a different device is rejected with 403 (spec "The operation
// must be consistent through an interactive authentication session").
func (a *API) DeleteDevice(w http.ResponseWriter, r *http.Request) {
	auth, _ := homeserver.AuthFrom(r.Context())
	deviceID := r.PathValue("deviceID")
	body, err := readBody(r)
	if err != nil {
		httpx.WriteError(w, err)
		return
	}
	var ad struct {
		Auth struct {
			Type     string `json:"type"`
			Session  string `json:"session"`
			Password string `json:"password"`
			// The identifier block (m.id.user) names whose password is being
			// supplied; the spec requires it to match the device owner.
			Identifier *struct {
				Type string `json:"type"`
				User string `json:"user"`
			} `json:"identifier"`
		} `json:"auth"`
	}
	if len(body) > 0 {
		_ = json.Unmarshal(body, &ad)
	}
	if ad.Auth.Type == "" && ad.Auth.Session == "" {
		// No auth supplied: issue a fresh UIA challenge describing the flows
		// (password always; SSO when CAS is configured), bound to this device
		// so the session cannot be replayed against another one.
		id, _ := a.uia.create("password", auth.Localpart, deviceID)
		flows := []uiaFlow{{Stages: []string{"m.login.password"}}}
		if a.casEnabled() {
			flows = append(flows, uiaFlow{Stages: []string{"m.login.sso"}})
		}
		httpx.WriteJSON(w, http.StatusUnauthorized, uiaChallenge{
			Flows:   flows,
			Params:  map[string]any{},
			Session: id,
		})
		return
	}
	// Reusing a session for a different device is forbidden outright (403):
	// the operation the session was started for must stay consistent.
	sess := a.uia.get(ad.Auth.Session)
	if sess != nil && sess.op == "password" && sess.localpart == auth.Localpart && sess.target != "" && sess.target != deviceID {
		httpx.WriteError(w, httpx.ErrForbidden("Requested operation has changed during the UI authentication session"))
		return
	}
	// An SSO-completed session authorises the deletion without a password
	// (spec: m.login.sso is a valid UIA stage). A session whose SSO stage
	// authenticated a different user is rejected (spec: the user must be
	// consistent through the session).
	if sess != nil && sess.completed["m.login.sso"] {
		if sess.ssoMismatch {
			httpx.WriteError(w, httpx.ErrForbidden("SSO user does not match the session owner"))
			return
		}
		if sess.localpart == auth.Localpart {
			a.deleteDevice(w, r, auth, deviceID)
			return
		}
	}
	// The UIA user (when specified) must match the device owner; otherwise the
	// request is forbidden outright (403), per "requires UI auth user to match
	// device owner".
	if ad.Auth.Identifier != nil && ad.Auth.Identifier.User != "" && ad.Auth.Identifier.User != auth.UserID {
		httpx.WriteError(w, httpx.ErrForbidden("UI auth user does not match the device owner"))
		return
	}
	if ad.Auth.Type != "m.login.password" || ad.Auth.Password == "" {
		// Issue a fresh UIA challenge describing the required flow, bound to
		// this device (so the session cannot be replayed against another one).
		id, _ := a.uia.create("password", auth.Localpart, deviceID)
		httpx.WriteJSON(w, http.StatusUnauthorized, uiaChallenge{
			Flows:   []uiaFlow{{Stages: []string{"m.login.password"}}},
			Params:  map[string]any{},
			Session: id,
		})
		return
	}
	// Verify the supplied password.
	user, err := a.Store.GetUser(r.Context(), auth.Localpart)
	if err != nil || user.PasswordHash == "" || !homeserver.CheckPassword(user.PasswordHash, ad.Auth.Password) {
		// UIA: a failed auth attempt is still a 401, and Complement expects the
		// challenge shape PLUS errcode/error (flows/params/session + error).
		id, _ := a.uia.create("password", auth.Localpart, deviceID)
		httpx.WriteJSON(w, http.StatusUnauthorized, map[string]any{
			"errcode": "M_FORBIDDEN",
			"error":   "Invalid password",
			"flows":   []uiaFlow{{Stages: []string{"m.login.password"}}},
			"params":  map[string]any{},
			"session": id,
		})
		return
	}
	a.deleteDevice(w, r, auth, deviceID)
}

// deleteDevice performs the device deletion and its side effects.
func (a *API) deleteDevice(w http.ResponseWriter, r *http.Request, auth *homeserver.Auth, deviceID string) {
	if err := a.Store.DeleteDevice(r.Context(), auth.Localpart, deviceID, a.ServerName()); err != nil {
		httpx.WriteError(w, httpx.ErrUnknown(err.Error()))
		return
	}
	// MSC3890: deleting a device clears its per-device local notification
	// settings account data (org.matrix.msc3890.local_notification_settings.<device>).
	_, _ = a.Store.DeleteAccountData(r.Context(), auth.Localpart, "", "org.matrix.msc3890.local_notification_settings."+deviceID)
	a.recordDeviceRemoval(r.Context(), auth.UserID, auth.Localpart, deviceID)
	httpx.WriteJSON(w, http.StatusOK, httpx.EmptyJSON)
}

// deviceJSON renders a storage.Device as the spec's device object.
func deviceJSON(d storage.Device) map[string]any {
	var ip any
	if d.LastSeenIP != "" {
		ip = d.LastSeenIP
	}
	return map[string]any{
		"device_id":    d.DeviceID,
		"display_name": d.DisplayName,
		"last_seen_ip": ip,
		"last_seen_ts": d.LastSeenTS,
		"created_ts":   d.CreatedTS,
	}
}
