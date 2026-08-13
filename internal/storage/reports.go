package storage

import "context"

// ReportKind enumerates the targets the reporting-content endpoints accept
// (spec §Reporting content): a room, an event, or a user.
type ReportKind string

const (
	ReportRoom  ReportKind = "room"
	ReportEvent ReportKind = "event"
	ReportUser  ReportKind = "user"
)

// StoreReport persists an abuse report (spec §Reporting content). Reports are
// stored for later admin review; the spec leaves how they are delivered to the
// implementation.
func (s *Store) StoreReport(ctx context.Context, reporter string, kind ReportKind, target, reason string, now int64) error {
	_, err := s.pool.Exec(ctx,
		`INSERT INTO reports(reporter, kind, target, reason, created_ts) VALUES ($1,$2,$3,$4,$5)`,
		reporter, string(kind), target, reason, now)
	return err
}
