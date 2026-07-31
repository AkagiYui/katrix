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
	httpx.WriteJSON(w, http.StatusOK, httpx.EmptyJSON)
}

// DeleteDevice handles DELETE /_matrix/client/v3/devices/{deviceID}. Per spec
// this must pass User-Interactive Authentication; the client supplies an auth
// dict with their password. The current device may be deleted (it is logged
// out), which matches spec behaviour (use /logout for the convenience path).
func (a *API) DeleteDevice(w http.ResponseWriter, r *http.Request) {
	auth, _ := homeserver.AuthFrom(r.Context())
	deviceID := r.PathValue("deviceID")
	body, err := readBody(r)
	if err != nil {
		httpx.WriteError(w, err)
		return
	}
	// UIA for device deletion uses m.login.password against the authenticated
	// user. checkUIA is single-stage (dummy) for registration; device deletion
	// needs the password stage, so verify it inline.
	var ad struct {
		Auth struct {
			Type     string `json:"type"`
			Session  string `json:"session"`
			Password string `json:"password"`
		} `json:"auth"`
	}
	if len(body) > 0 {
		_ = json.Unmarshal(body, &ad)
	}
	if ad.Auth.Type != "m.login.password" || ad.Auth.Password == "" {
		// Issue a fresh UIA challenge describing the required flow.
		id, _ := a.uia.create("password", auth.Localpart)
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
		id, _ := a.uia.create("password", auth.Localpart)
		httpx.WriteJSON(w, http.StatusUnauthorized, map[string]any{
			"errcode": "M_FORBIDDEN",
			"error":   "Invalid password",
			"flows":   []uiaFlow{{Stages: []string{"m.login.password"}}},
			"params":  map[string]any{},
			"session": id,
		})
		return
	}
	if err := a.Store.DeleteDevice(r.Context(), auth.Localpart, deviceID, a.ServerName()); err != nil {
		httpx.WriteError(w, httpx.ErrUnknown(err.Error()))
		return
	}
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
