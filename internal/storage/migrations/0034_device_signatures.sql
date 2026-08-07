-- Cross-signing signatures uploaded via POST /keys/signatures/upload (spec
-- "Cross-signing"): a signing key's signatures on a target device or
-- cross-signing key. Each row is one "ed25519:<key_id>" signature value from
-- one signer on one target device. Stored separately from device_keys so a
-- re-uploaded key bundle does not clobber signatures made by other users;
-- merged back into the target device's key bundle when serving /keys/query
-- and the federation /user/devices + /user/keys/query endpoints (mirror of
-- Synapse's e2e_cross_signing_signatures table).

CREATE TABLE device_signatures (
    target_user   TEXT NOT NULL,
    target_device TEXT NOT NULL,
    signer_user   TEXT NOT NULL,
    signature_key TEXT NOT NULL,   -- "ed25519:<key_id>"
    signature     TEXT NOT NULL,   -- the base64 signature value
    PRIMARY KEY (target_user, target_device, signer_user, signature_key)
);
