package storage

import (
	"context"
	"errors"

	"github.com/jackc/pgx/v5"
)

// ---- Thread subscriptions (MSC4306 / MSC4308) ----

// ThreadSubscription is a user's subscription to one thread. An automatic
// subscription is caused by a specific thread reply (AutomaticEventID);
// ConsumedUpto is the stream position below which replies count as already
// "consumed" by a previous subscription (re-using one conflicts with the
// unsubscribe). Unsubscribed keeps the row so the watermark survives.
type ThreadSubscription struct {
	UserLocalpart    string
	RoomID           string
	ThreadRootID     string
	Automatic        bool
	AutomaticEventID string
	BumpStamp        int64
	ConsumedUpto     int64
	Unsubscribed     bool
	CreatedStream    int64
}

// UpsertThreadSubscription activates a subscription (manual or automatic). The
// created_stream is only assigned on first insert so incremental sliding sync
// does not re-deliver an existing subscription.
func (s *Store) UpsertThreadSubscription(ctx context.Context, sub ThreadSubscription) error {
	_, err := s.pool.Exec(ctx,
		`INSERT INTO thread_subscriptions
		   (user_localpart, room_id, thread_root_id, automatic, automatic_event_id,
		    bump_stamp, consumed_upto, unsubscribed, created_stream)
		 VALUES ($1,$2,$3,$4,$5,$6,$7,FALSE,nextval('thread_subscription_stream_seq'))
		 ON CONFLICT (user_localpart, room_id, thread_root_id) DO UPDATE SET
		   automatic=EXCLUDED.automatic,
		   automatic_event_id=EXCLUDED.automatic_event_id,
		   bump_stamp=EXCLUDED.bump_stamp,
		   consumed_upto=EXCLUDED.consumed_upto,
		   unsubscribed=FALSE`,
		sub.UserLocalpart, sub.RoomID, sub.ThreadRootID, sub.Automatic, sub.AutomaticEventID,
		sub.BumpStamp, sub.ConsumedUpto)
	return err
}

// UnsubscribeThreadSubscription marks a subscription unsubscribed (idempotent)
// and raises the consumed watermark to consumedUpto (the highest reply stream
// seen so far).
func (s *Store) UnsubscribeThreadSubscription(ctx context.Context, localpart, roomID, threadRootID string, consumedUpto int64) error {
	_, err := s.pool.Exec(ctx,
		`INSERT INTO thread_subscriptions
		   (user_localpart, room_id, thread_root_id, automatic, automatic_event_id,
		    bump_stamp, consumed_upto, unsubscribed, created_stream)
		 VALUES ($1,$2,$3,FALSE,'',0,$4,TRUE,nextval('thread_subscription_stream_seq'))
		 ON CONFLICT (user_localpart, room_id, thread_root_id) DO UPDATE SET
		   consumed_upto=GREATEST(thread_subscriptions.consumed_upto, EXCLUDED.consumed_upto),
		   unsubscribed=TRUE`,
		localpart, roomID, threadRootID, consumedUpto)
	return err
}

// GetThreadSubscription returns a user's subscription row, or ErrNotFound.
func (s *Store) GetThreadSubscription(ctx context.Context, localpart, roomID, threadRootID string) (*ThreadSubscription, error) {
	var sub ThreadSubscription
	err := s.pool.QueryRow(ctx,
		`SELECT user_localpart, room_id, thread_root_id, automatic, automatic_event_id,
		        bump_stamp, consumed_upto, unsubscribed, created_stream
		 FROM thread_subscriptions WHERE user_localpart=$1 AND room_id=$2 AND thread_root_id=$3`,
		localpart, roomID, threadRootID,
	).Scan(&sub.UserLocalpart, &sub.RoomID, &sub.ThreadRootID, &sub.Automatic, &sub.AutomaticEventID,
		&sub.BumpStamp, &sub.ConsumedUpto, &sub.Unsubscribed, &sub.CreatedStream)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return nil, ErrNotFound
		}
		return nil, err
	}
	return &sub, nil
}

// ThreadSubscriptionsSince returns a user's active subscriptions whose
// created_stream exceeds since, for the MSC4308 sliding-sync extension. The
// bump_stamp field is the subscription's last activity timestamp.
func (s *Store) ThreadSubscriptionsSince(ctx context.Context, localpart string, since int64) ([]ThreadSubscription, error) {
	rows, err := s.pool.Query(ctx,
		`SELECT user_localpart, room_id, thread_root_id, automatic, automatic_event_id,
		        bump_stamp, consumed_upto, unsubscribed, created_stream
		 FROM thread_subscriptions
		 WHERE user_localpart=$1 AND unsubscribed=FALSE AND created_stream>$2
		 ORDER BY created_stream ASC`, localpart, since)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []ThreadSubscription
	for rows.Next() {
		var sub ThreadSubscription
		if err := rows.Scan(&sub.UserLocalpart, &sub.RoomID, &sub.ThreadRootID, &sub.Automatic, &sub.AutomaticEventID,
			&sub.BumpStamp, &sub.ConsumedUpto, &sub.Unsubscribed, &sub.CreatedStream); err != nil {
			return nil, err
		}
		out = append(out, sub)
	}
	return out, rows.Err()
}

// MaxThreadReplyStream returns the highest stream ordering among a thread's
// replies (0 when the thread has no replies).
func (s *Store) MaxThreadReplyStream(ctx context.Context, roomID, threadRootID string) (int64, error) {
	var max int64
	err := s.pool.QueryRow(ctx,
		`SELECT COALESCE(MAX(stream_ordering),0) FROM event_relations
		 WHERE room_id=$1 AND parent_event_id=$2 AND rel_type='m.thread'`,
		roomID, threadRootID).Scan(&max)
	return max, err
}
