package federation

import "context"

// PushNotifier delivers HTTP push notifications for inbound events. It is
// implemented by the CS API's push dispatcher and wired in via
// SetPushDispatcher during server assembly (the CS API is constructed after
// the federation API, so the federation API cannot hold the concrete type).
//
// NotifyInbound is called after an event has been persisted by the federation
// ingest path (transactions, send_join/send_invite/send_leave). The dispatcher
// evaluates the event against each joined local user's push rules (plus the
// target of a membership event) and POSTs their HTTP pushers.
type PushNotifier interface {
	NotifyInbound(ctx context.Context, roomID, eventID, eventType, sender, stateKey string, content []byte, stream int64, rejected bool)
}
