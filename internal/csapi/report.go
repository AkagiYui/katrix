package csapi

import (
	"net/http"
	"strings"

	"github.com/AkagiYui/katrix/internal/homeserver"
	"github.com/AkagiYui/katrix/internal/httpx"
	"github.com/AkagiYui/katrix/internal/rooms"
	"github.com/AkagiYui/katrix/internal/storage"
)

// registerReporting wires the reporting-content endpoints (spec §Reporting
// content): report a room, an event, or a user as inappropriate. Reports are
// persisted for admin review; the spec leaves how they are delivered to the
// implementation.
func (a *API) registerReporting(mux *http.ServeMux) {
	mux.HandleFunc("POST /_matrix/client/v3/rooms/{roomID}/report/{eventID}", a.RequireAuth(a.ReportEvent))
	mux.HandleFunc("POST /_matrix/client/v3/rooms/{roomID}/report", a.RequireAuth(a.ReportRoom))
	mux.HandleFunc("POST /_matrix/client/v3/users/{userID}/report", a.RequireAuth(a.ReportUser))
}

// reportBody is the shared request body of the three reporting endpoints.
// reason may be blank; score was removed from the spec in v1.18 and is ignored.
type reportBody struct {
	Reason string `json:"reason"`
}

// ReportRoom handles POST /_matrix/client/v3/rooms/{roomID}/report. The caller
// is not required to be joined; unknown rooms are answered with 404
// (M_NOT_FOUND) per the spec, which also permits a conceal-200 — this
// implementation discloses existence, mirroring Synapse.
func (a *API) ReportRoom(w http.ResponseWriter, r *http.Request) {
	auth, _ := homeserver.AuthFrom(r.Context())
	roomID := r.PathValue("roomID")
	var req reportBody
	if err := httpx.DecodeJSON(w, r, &req); err != nil {
		httpx.WriteError(w, err)
		return
	}
	exists, err := a.Store.RoomExists(r.Context(), roomID)
	if err != nil {
		httpx.WriteError(w, httpx.ErrUnknown(err.Error()))
		return
	}
	if !exists {
		writeRoomErr(w, newRoomError(http.StatusNotFound, "M_NOT_FOUND", "The room was not found."))
		return
	}
	if err := a.Store.StoreReport(r.Context(), auth.UserID, storage.ReportRoom, roomID, req.Reason, a.Now()); err != nil {
		httpx.WriteError(w, httpx.ErrUnknown(err.Error()))
		return
	}
	httpx.WriteJSON(w, http.StatusOK, httpx.EmptyJSON)
}

// ReportEvent handles POST /_matrix/client/v3/rooms/{roomID}/report/{eventID}.
// The caller must be joined to the room (since spec v1.8). The 404 response
// deliberately does not distinguish a missing event from a non-joined caller,
// per the spec's enumeration-resistance note.
func (a *API) ReportEvent(w http.ResponseWriter, r *http.Request) {
	auth, _ := homeserver.AuthFrom(r.Context())
	roomID := r.PathValue("roomID")
	eventID := r.PathValue("eventID")
	var req reportBody
	if err := httpx.DecodeJSON(w, r, &req); err != nil {
		httpx.WriteError(w, err)
		return
	}
	if err := a.checkMembership(r.Context(), roomID, auth.UserID, rooms.MembershipJoin); err != nil {
		writeRoomErr(w, newRoomError(http.StatusNotFound, "M_NOT_FOUND",
			"The event was not found or you are not joined to the room where the event resides."))
		return
	}
	ev, err := a.Store.GetEvent(r.Context(), eventID)
	if err != nil || ev.RoomID != roomID {
		writeRoomErr(w, newRoomError(http.StatusNotFound, "M_NOT_FOUND",
			"The event was not found or you are not joined to the room where the event resides."))
		return
	}
	if err := a.Store.StoreReport(r.Context(), auth.UserID, storage.ReportEvent, eventID, req.Reason, a.Now()); err != nil {
		httpx.WriteError(w, httpx.ErrUnknown(err.Error()))
		return
	}
	httpx.WriteJSON(w, http.StatusOK, httpx.EmptyJSON)
}

// ReportUser handles POST /_matrix/client/v3/users/{userID}/report. The
// reported user must exist on this homeserver; otherwise 404 (M_NOT_FOUND).
func (a *API) ReportUser(w http.ResponseWriter, r *http.Request) {
	auth, _ := homeserver.AuthFrom(r.Context())
	userID := r.PathValue("userID")
	var req reportBody
	if err := httpx.DecodeJSON(w, r, &req); err != nil {
		httpx.WriteError(w, err)
		return
	}
	if !strings.HasPrefix(userID, "@") || !a.IsLocalUser(userID) {
		httpx.WriteError(w, httpx.ErrNotFound("The user was not found."))
		return
	}
	exists, err := a.Store.UserExists(r.Context(), a.LocalpartOf(userID))
	if err != nil {
		httpx.WriteError(w, httpx.ErrUnknown(err.Error()))
		return
	}
	if !exists {
		httpx.WriteError(w, httpx.ErrNotFound("The user was not found."))
		return
	}
	if err := a.Store.StoreReport(r.Context(), auth.UserID, storage.ReportUser, userID, req.Reason, a.Now()); err != nil {
		httpx.WriteError(w, httpx.ErrUnknown(err.Error()))
		return
	}
	httpx.WriteJSON(w, http.StatusOK, httpx.EmptyJSON)
}
