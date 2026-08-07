package federation

import (
	"context"

	"github.com/AkagiYui/katrix/internal/events"
)

// PushNotifier is invoked after an inbound event has been persisted by the
// federation ingest path (transactions, send_join/send_invite/send_leave). It
// is implemented by the CS API, which both delivers HTTP push notifications to
// the room's local users' pushers and pushes the event to interested
// application services. Wired in via SetPushDispatcher during server assembly
// (the CS API is constructed after the federation API, so the federation API
// cannot hold the concrete type).
type PushNotifier interface {
	NotifyInbound(ctx context.Context, ev *events.Event, roomID string, stream int64, rejected bool)
}
