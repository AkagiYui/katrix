-- User-signature change stream (mirror of Synapse's user_signature_stream):
-- when a user uploads signatures via POST /keys/signatures/upload, the users
-- they signed are surfaced in the signer's OWN /sync device_lists.changed
-- (spec "Cross-signing": a user who uploads new signatures of other users
-- must re-fetch their keys — sytest "Changing user-signing key notifies local
-- users" syncs until the signed user appears in the signer's changed list).
-- One row per (signer, signed target), advancing the shared sync stream; the
-- sync engine reports the targets of a syncer's signature uploads in changed.

CREATE TABLE signature_changes (
    signer_user TEXT NOT NULL,
    target_user TEXT NOT NULL,
    stream_id   BIGINT NOT NULL,
    PRIMARY KEY (signer_user, target_user)
);
CREATE INDEX idx_signature_changes_stream ON signature_changes(stream_id);
