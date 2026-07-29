package federation

import (
	"context"

	"github.com/AkagiYui/katrix/internal/roomver"
)

// roomRules returns the room-version rules for a room, fetching the room's
// version from storage. Returns nil if the room is unknown or the version is
// unsupported. Used by the auth-chain walk to parse legacy event references.
func (a *API) roomRules(roomID string) *roomver.Rules {
	room, err := a.Store.GetRoom(context.Background(), roomID)
	if err != nil || room == nil {
		return nil
	}
	rules, ok := roomver.Get(roomver.Version(room.Version))
	if !ok {
		return nil
	}
	return &rules
}
