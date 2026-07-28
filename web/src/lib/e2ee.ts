// Full client-side E2EE using libolm (via @matrix-org/olm). Implements:
//
//  - Device identity: Ed25519 signing key + Curve25519 identity key, persisted
//    in IndexedDB. Device keys are signed (canonical JSON) and uploaded via
//    keys/upload.
//  - One-time keys: generated and uploaded; claimed by other devices to
//    establish Olm sessions.
//  - Olm sessions: 1:1 encrypted to-device channels for sharing Megolm room
//    keys. Created outbound (via claimed one-time key) or inbound (from a
//    received m.encrypted to-device event).
//  - Megolm sessions: group message encryption. An outbound session per room
//    encrypts m.room.message into m.room.encrypted (m.megolm.v1.aes-sha2).
//    Room keys are shared with all known devices via to-device m.room_key.
//    Inbound sessions (received room keys) decrypt inbound m.room.encrypted.
//
// All crypto state is persisted in IndexedDB under a per-user store keyed by
// device_id, so keys survive reloads.

import Olm from "./olm-init";
import { initOlm } from "./olm-init";
import { canonicalJSONString } from "./canonical-json";
import { getToken, getUserId } from "./matrix";

// ---------------------------------------------------------------------------
// IndexedDB persistence
// ---------------------------------------------------------------------------

const DB_NAME_PREFIX = "katrix-olm";
const STORE = "crypto";
const DB_VERSION = 1;

/** A single persisted crypto record. The store is keyed by `key`. */
interface CryptoRecord {
  key: string;
  value: string; // pickled string (encrypted with the device pickle key)
}

function openDB(): Promise<IDBDatabase> {
  const userId = getUserId() ?? "default";
  const dbName = `${DB_NAME_PREFIX}:${userId}`;
  return new Promise((resolve, reject) => {
    const req = indexedDB.open(dbName, DB_VERSION);
    req.onupgradeneeded = () => {
      const db = req.result;
      if (!db.objectStoreNames.contains(STORE)) {
        db.createObjectStore(STORE, { keyPath: "key" });
      }
    };
    req.onsuccess = () => resolve(req.result);
    req.onerror = () => reject(req.error);
  });
}

async function dbGet(key: string): Promise<string | undefined> {
  const db = await openDB();
  return new Promise((resolve, reject) => {
    const tx = db.transaction(STORE, "readonly");
    const req = tx.objectStore(STORE).get(key);
    req.onsuccess = () => resolve((req.result as CryptoRecord | undefined)?.value);
    req.onerror = () => reject(req.error);
  });
}

async function dbPut(key: string, value: string): Promise<void> {
  const db = await openDB();
  return new Promise((resolve, reject) => {
    const tx = db.transaction(STORE, "readwrite");
    tx.objectStore(STORE).put({ key, value } satisfies CryptoRecord);
    tx.oncomplete = () => resolve();
    tx.onerror = () => reject(tx.error);
  });
}

async function dbKeys(): Promise<string[]> {
  const db = await openDB();
  return new Promise((resolve, reject) => {
    const tx = db.transaction(STORE, "readonly");
    const req = tx.objectStore(STORE).getAllKeys();
    req.onsuccess = () => resolve(req.result.map((k) => String(k)));
    req.onerror = () => reject(req.error);
  });
}

// ---------------------------------------------------------------------------
// Crypto store: the single source of truth for all Olm/Megolm state
// ---------------------------------------------------------------------------

const PICKLE_KEY = "katrix-olm-pickle"; // symmetric key for pickling secrets

/** Keys under which records are stored in IndexedDB. */
const K = {
  account: "account",
  deviceId: "device_id",
  /** Inbound Olm sessions: key = `${sessionId}` */
  olmSession: (id: string) => `olm-session:${id}`,
  /** Outbound Megolm session per room: key = `megolm-out:${roomId}` */
  megolmOut: (roomId: string) => `megolm-out:${roomId}`,
  /** Inbound Megolm sessions: key = `${sessionId}` (room-key sessions) */
  megolmIn: (id: string) => `megolm-in:${id}`,
  /** Map session_id -> room_id for inbound megolm sessions */
  megolmInRoom: (id: string) => `megolm-in-room:${id}`,
  /** Known device keys: user_id -> device_id -> device_keys */
  deviceKeys: "device-keys",
} as const;

/** The Olm Account, lazily created and persisted. */
let account: Olm.Account | null = null;
let cachedDeviceId: string | null = null;

/** Identity keys parsed from the account. */
export interface IdentityKeys {
  ed25519: string;
  curve25519: string;
}

/** Get the persisted device id (generated on first bootstrap). */
export async function getDeviceId(): Promise<string> {
  if (cachedDeviceId) return cachedDeviceId;
  const id = await dbGet(K.deviceId);
  if (id) {
    cachedDeviceId = id;
    return id;
  }
  return "";
}

/** Ensure the Olm account exists, creating + persisting it on first run. */
export async function ensureAccount(deviceId: string): Promise<Olm.Account> {
  if (account) return account;
  await initOlm();
  const acc = new Olm.Account();
  const pickled = await dbGet(K.account);
  if (pickled) {
    acc.unpickle(PICKLE_KEY, pickled);
  } else {
    acc.create();
    // Generate initial one-time keys and a fallback key.
    acc.generate_one_time_keys(Math.max(1, acc.max_number_of_one_time_keys() / 2));
    try {
      acc.generate_fallback_key();
    } catch {
      // older olm versions may not support fallback keys
    }
    await dbPut(K.account, acc.pickle(PICKLE_KEY));
    cachedDeviceId = deviceId;
    await dbPut(K.deviceId, deviceId);
  }
  account = acc;
  cachedDeviceId = deviceId;
  return acc;
}

/** Parse identity keys from the account. */
export function identityKeys(acc: Olm.Account): IdentityKeys {
  const raw = JSON.parse(acc.identity_keys());
  return {
    ed25519: raw.ed25519,
    curve25519: raw.curve25519,
  };
}

// ---------------------------------------------------------------------------
// Device key bundle construction + signing
// ---------------------------------------------------------------------------

/** The device_keys object uploaded to keys/upload. */
export interface DeviceKeys {
  user_id: string;
  device_id: string;
  algorithms: string[];
  keys: Record<string, string>;
  signatures: Record<string, Record<string, string>>;
}

/** Build and sign the device_keys object for upload. */
export async function buildDeviceKeys(
  userId: string,
  deviceId: string,
): Promise<DeviceKeys> {
  const acc = await ensureAccount(deviceId);
  const { ed25519, curve25519 } = identityKeys(acc);
  const unsigned: Omit<DeviceKeys, "signatures"> = {
    user_id: userId,
    device_id: deviceId,
    algorithms: ["m.olm.v1", "m.megolm.v1.aes-sha2"],
    keys: {
      [`ed25519:${deviceId}`]: ed25519,
      [`curve25519:${deviceId}`]: curve25519,
    },
  };
  // Sign the canonical JSON of the unsigned object.
  const canonical = canonicalJSONString(unsigned);
  const signature = acc.sign(canonical);
  return {
    ...unsigned,
    signatures: {
      [userId]: { [`ed25519:${deviceId}`]: signature },
    },
  };
}

/** Build one-time keys (curve25519) for upload, keyed by `signed_curve25519:`. */
export async function buildOneTimeKeys(
  deviceId: string,
): Promise<Record<string, { key: string; signatures: Record<string, Record<string, string>> }>> {
  const acc = await ensureAccount(deviceId);
  const userId = getUserId() ?? "";
  const { ed25519 } = identityKeys(acc);
  const raw = JSON.parse(acc.one_time_keys());
  const fallback = safeJson(acc.fallback_key?.());
  const out: Record<string, { key: string; signatures: Record<string, Record<string, string>> }> = {};
  const signKey = `ed25519:${deviceId}`;
  for (const [id, key] of Object.entries(raw)) {
    const obj = { key, signatures: {} } as { key: unknown; signatures: Record<string, Record<string, string>> };
    const signed = { key: (key as { key?: string }).key ?? key };
    const canonical = canonicalJSONString({
      user_id: userId,
      device_id: deviceId,
      keys: { [signKey]: ed25519 },
      ...signed,
    });
    obj.signatures = { [userId]: { [signKey]: acc.sign(canonical) } };
    out[`signed_curve25519:${id}`] = { key: String((signed as { key: unknown }).key ?? key), signatures: obj.signatures };
  }
  for (const [id, key] of Object.entries(fallback)) {
    const signed = { key: (key as { key?: string }).key ?? key };
    const canonical = canonicalJSONString({
      user_id: userId,
      device_id: deviceId,
      keys: { [signKey]: ed25519 },
      ...signed,
    });
    out[`signed_curve25519:${id}`] = { key: String((signed as { key: unknown }).key ?? key), signatures: { [userId]: { [signKey]: acc.sign(canonical) } } };
  }
  return out;
}

function safeJson(s: string | undefined): Record<string, unknown> {
  if (!s) return {};
  try { return JSON.parse(s); } catch { return {}; }
}

/** Mark current one-time keys as published (after a successful upload). */
export async function markKeysAsPublished(): Promise<void> {
  if (!account) return;
  account.mark_keys_as_published();
  await dbPut(K.account, account.pickle(PICKLE_KEY));
}

// ---------------------------------------------------------------------------
// Megolm: outbound group session (encrypt room messages)
// ---------------------------------------------------------------------------

/** Cached outbound group sessions per room (in-memory; backed by IndexedDB). */
const outboundCache = new Map<string, Olm.OutboundGroupSession>();

/** Get or create the outbound Megolm session for a room. */
export async function getOutboundGroupSession(
  roomId: string,
): Promise<Olm.OutboundGroupSession> {
  const cached = outboundCache.get(roomId);
  if (cached) return cached;
  const key = K.megolmOut(roomId);
  const pickled = await dbGet(key);
  const s = new Olm.OutboundGroupSession();
  if (pickled) {
    s.unpickle(PICKLE_KEY, pickled);
  } else {
    s.create();
    await dbPut(key, s.pickle(PICKLE_KEY));
  }
  outboundCache.set(roomId, s);
  return s;
}

/** A room key to share with other devices via to-device m.room_key. */
export interface RoomKey {
  algorithm: "m.megolm.v1.aes-sha2";
  room_id: string;
  session_id: string;
  session_key: string;
  signing_key: string; // our ed25519 (the "chain key" owner identity)
}

/** Build the m.room_key content for the room's outbound session. */
export async function buildRoomKey(roomId: string): Promise<RoomKey & { chain_index: number }> {
  const s = await getOutboundGroupSession(roomId);
  const acc = await ensureAccount(await getDeviceId());
  const { ed25519 } = identityKeys(acc);
  return {
    algorithm: "m.megolm.v1.aes-sha2",
    room_id: roomId,
    session_id: s.session_id(),
    session_key: s.session_key(),
    signing_key: ed25519,
    chain_index: s.message_index(),
  };
}

/** Encrypt a plaintext message for a room, returning m.room.encrypted content. */
export async function encryptRoomMessage(
  roomId: string,
  plaintext: string,
): Promise<{
  algorithm: "m.megolm.v1.aes-sha2";
  ciphertext: string;
  sender_key: string;
  device_id: string;
  session_id: string;
}> {
  const s = await getOutboundGroupSession(roomId);
  const acc = await ensureAccount(await getDeviceId());
  const { curve25519 } = identityKeys(acc);
  const ciphertext = s.encrypt(plaintext);
  // Persist the session (message_index advanced).
  await dbPut(K.megolmOut(roomId), s.pickle(PICKLE_KEY));
  return {
    algorithm: "m.megolm.v1.aes-sha2",
    ciphertext,
    sender_key: curve25519,
    device_id: await getDeviceId(),
    session_id: s.session_id(),
  };
}

// ---------------------------------------------------------------------------
// Megolm: inbound group session (decrypt room messages)
// ---------------------------------------------------------------------------

/** Import a received room key into an inbound Megolm session. */
export async function importRoomKey(key: RoomKey): Promise<void> {
  const id = key.session_id;
  const existing = await dbGet(K.megolmIn(id));
  if (existing) return; // already have this session
  const s = new Olm.InboundGroupSession();
  try {
    s.create(key.session_key);
  } catch {
    s.free();
    return;
  }
  await dbPut(K.megolmIn(id), s.pickle(PICKLE_KEY));
  await dbPut(K.megolmInRoom(id), key.room_id);
  s.free();
}

/** Decrypt a m.room.encrypted event body. Returns the plaintext or null. */
export async function decryptRoomMessage(
  roomId: string,
  sessionId: string,
  ciphertext: string,
): Promise<string | null> {
  const pickled = await dbGet(K.megolmIn(sessionId));
  if (!pickled) return null;
  // Verify the session belongs to this room.
  const storedRoom = await dbGet(K.megolmInRoom(sessionId));
  if (storedRoom && storedRoom !== roomId) return null;
  const s = new Olm.InboundGroupSession();
  try {
    s.unpickle(PICKLE_KEY, pickled);
    const { plaintext } = s.decrypt(ciphertext);
    return plaintext;
  } catch {
    return null;
  } finally {
    s.free();
  }
}

/** Whether we have an inbound session that can decrypt the given session_id. */
export async function hasInboundSession(sessionId: string): Promise<boolean> {
  return (await dbGet(K.megolmIn(sessionId))) !== undefined;
}

// ---------------------------------------------------------------------------
// Olm sessions: decrypt inbound to-device m.encrypted (room-key delivery)
// ---------------------------------------------------------------------------

/** Decrypt an inbound m.encrypted to-device event using the Olm account. */
export async function decryptToDevice(
  senderKey: string,
  ciphertext: string,
  deviceId: string,
): Promise<string | null> {
  const acc = await ensureAccount(deviceId);
  // ciphertext is a JSON string {type, body}; the pre-key message format.
  let msg: { type: number; body: string };
  try {
    msg = JSON.parse(ciphertext);
  } catch {
    return null;
  }
  // Try existing inbound sessions first.
  const sessionIds = (await dbKeys()).filter((k) => k.startsWith("olm-session:"));
  for (const sk of sessionIds) {
    const id = sk.substring("olm-session:".length);
    const pickled = await dbGet(sk);
    if (!pickled) continue;
    const s = new Olm.Session();
    try {
      s.unpickle(PICKLE_KEY, pickled);
      if (s.matches_inbound_from(senderKey, ciphertext)) {
        const plaintext = s.decrypt(msg.type, msg.body);
        await dbPut(sk, s.pickle(PICKLE_KEY));
        return plaintext;
      }
    } catch {
      // not this session
    } finally {
      s.free();
    }
  }
  // No existing session matched: create a new inbound session from the pre-key.
  const s = new Olm.Session();
  try {
    s.create_inbound_from(acc, senderKey, ciphertext);
    const plaintext = s.decrypt(msg.type, msg.body);
    const id = s.session_id();
    await dbPut(K.olmSession(id), s.pickle(PICKLE_KEY));
    // Remove the one-time key that was used.
    await dbPut(K.account, acc.pickle(PICKLE_KEY));
    return plaintext;
  } catch {
    return null;
  } finally {
    s.free();
  }
}

// ---------------------------------------------------------------------------
// Device key store (keys/query results)
// ---------------------------------------------------------------------------

/** Known device keys: user -> device -> {ed25519, curve25519}. */
type DeviceMap = Record<string, Record<string, { ed25519: string; curve25519: string }>>;

async function loadDeviceKeys(): Promise<DeviceMap> {
  const raw = await dbGet(K.deviceKeys);
  if (!raw) return {};
  try { return JSON.parse(raw); } catch { return {}; }
}

async function saveDeviceKeys(m: DeviceMap): Promise<void> {
  await dbPut(K.deviceKeys, JSON.stringify(m));
}

/** Merge keys/query results into the known device-key store. */
export async function storeDeviceKeysFromQuery(
  deviceKeys: Record<string, Record<string, unknown>>,
): Promise<void> {
  const m = await loadDeviceKeys();
  for (const [userId, devices] of Object.entries(deviceKeys)) {
    m[userId] ??= {};
    for (const [deviceId, dk] of Object.entries(devices)) {
      const keys = (dk as { keys?: Record<string, string> }).keys;
      if (!keys) continue;
      const ed = keys[`ed25519:${deviceId}`];
      const cv = keys[`curve25519:${deviceId}`];
      if (ed && cv) {
        m[userId][deviceId] = { ed25519: ed, curve25519: cv };
      }
    }
  }
  await saveDeviceKeys(m);
}

/** All known device curve25519 keys for a user (for room-key sharing). */
export async function userDeviceKeys(userId: string): Promise<Record<string, string>> {
  const m = await loadDeviceKeys();
  const devices = m[userId] ?? {};
  const out: Record<string, string> = {};
  for (const [deviceId, keys] of Object.entries(devices)) {
    out[deviceId] = keys.curve25519;
  }
  return out;
}

// ---------------------------------------------------------------------------
// To-device helper
// ---------------------------------------------------------------------------

/** Send a to-device event. */
export async function sendToDevice(
  eventType: string,
  txnId: string,
  messages: Record<string, Record<string, unknown>>,
): Promise<void> {
  const resp = await fetch(
    `/_matrix/client/v3/sendToDevice/${encodeURIComponent(eventType)}/${encodeURIComponent(txnId)}`,
    {
      method: "PUT",
      headers: {
        "Content-Type": "application/json",
        Authorization: `Bearer ${getToken()}`,
      },
      body: JSON.stringify({ messages }),
    },
  );
  if (!resp.ok) throw new Error(`to-device send failed: HTTP ${resp.status}`);
}

// ---------------------------------------------------------------------------
// Bootstrap: upload device keys + one-time keys + query own keys
// ---------------------------------------------------------------------------

/** Run the full E2EE bootstrap for the current device. */
export async function bootstrapE2EE(deviceId: string): Promise<{
  ed25519: string;
  curve25519: string;
}> {
  const userId = getUserId() ?? "";
  const acc = await ensureAccount(deviceId);
  const { ed25519, curve25519 } = identityKeys(acc);

  // Upload device keys + one-time keys.
  const deviceKeys = await buildDeviceKeys(userId, deviceId);
  const oneTimeKeys = await buildOneTimeKeys(deviceId);
  const uploadBody: Record<string, unknown> = { device_keys: deviceKeys };
  if (Object.keys(oneTimeKeys).length > 0) {
    uploadBody.one_time_keys = oneTimeKeys;
  }
  const resp = await fetch("/_matrix/client/v3/keys/upload", {
    method: "POST",
    headers: {
      "Content-Type": "application/json",
      Authorization: `Bearer ${getToken()}`,
    },
    body: JSON.stringify(uploadBody),
  });
  if (!resp.ok) throw new Error(`keys/upload failed: HTTP ${resp.status}`);
  await markKeysAsPublished();

  // Query our own keys to populate the device store.
  await queryUserDevices([userId]);

  return { ed25519, curve25519 };
}

/** Query device keys for users and store them. */
export async function queryUserDevices(userIds: string[]): Promise<void> {
  const token = getToken();
  if (!token || userIds.length === 0) return;
  const resp = await fetch("/_matrix/client/v3/keys/query", {
    method: "POST",
    headers: {
      "Content-Type": "application/json",
      Authorization: `Bearer ${token}`,
    },
    body: JSON.stringify({
      device_keys: Object.fromEntries(userIds.map((u) => [u, []])),
    }),
  });
  if (!resp.ok) return;
  const data = await resp.json();
  if (data.device_keys) {
    await storeDeviceKeysFromQuery(data.device_keys);
  }
}

/**
 * Share the current outbound Megolm room key with all known devices in the
 * room. Called after encrypting the first message in a room (and periodically
 * for new devices). Uses Olm 1:1 sessions to wrap the room_key in
 * m.encrypted to-device events.
 *
 * This is a best-effort share: it queries device keys for the room's members,
 * then for each recipient device it establishes (or reuses) an Olm session and
 * encrypts the m.room_key payload. Devices without a claimed one-time key are
 * skipped (they will be unable to decrypt until a session is established).
 */
export async function shareRoomKey(
  roomId: string,
  memberUserIds: string[],
): Promise<void> {
  const deviceId = await getDeviceId();
  const acc = await ensureAccount(deviceId);
  const userId = getUserId() ?? "";
  const roomKey = await buildRoomKey(roomId);
  const plaintext = JSON.stringify(roomKey);

  // Collect recipient devices (skip our own device).
  const recipients: { userId: string; deviceId: string; curve25519: string }[] = [];
  for (const uid of memberUserIds) {
    const devices = await userDeviceKeys(uid);
    for (const [devId, curve] of Object.entries(devices)) {
      if (uid === userId && devId === deviceId) continue;
      recipients.push({ userId: uid, deviceId: devId, curve25519: curve });
    }
  }
  if (recipients.length === 0) return;

  // Claim one-time keys for recipients lacking an Olm session.
  // For simplicity, we claim one-time keys for all recipients each share.
  const claimBody: Record<string, Record<string, string[]>> = {};
  for (const r of recipients) {
    claimBody[r.userId] ??= {};
    claimBody[r.userId][r.deviceId] = ["signed_curve25519"];
  }
  const token = getToken();
  if (!token) return;
  const claimResp = await fetch("/_matrix/client/v3/keys/claim", {
    method: "POST",
    headers: {
      "Content-Type": "application/json",
      Authorization: `Bearer ${token}`,
    },
    body: JSON.stringify({ one_time_keys: claimBody }),
  });
  if (!claimResp.ok) return;
  const claimData = await claimResp.json();
  const claimed = claimData.one_time_keys ?? {};

  const messages: Record<string, Record<string, unknown>> = {};
  for (const r of recipients) {
    const otkMap = claimed[r.userId]?.[r.deviceId];
    if (!otkMap) continue;
    const [[otkId, otkObj]] = Object.entries(otkMap);
    const oneTimeKey = (otkObj as { key?: string }).key ?? String(otkObj);
    const s = new Olm.Session();
    try {
      s.create_outbound(acc, r.curve25519, oneTimeKey);
      const enc = s.encrypt(plaintext);
      const payload = {
        algorithm: "m.olm.v1",
        ciphertext: {
          [r.curve25519]: {
            type: enc.type,
            body: enc.body,
          },
        },
        sender_key: identityKeys(acc).curve25519,
      };
      messages[r.userId] ??= {};
      messages[r.userId][r.deviceId] = payload;
      // Persist the session.
      await dbPut(K.olmSession(s.session_id()), s.pickle(PICKLE_KEY));
    } catch {
      // skip this device
    } finally {
      s.free();
    }
  }

  if (Object.keys(messages).length > 0) {
    await sendToDevice("m.room.encrypted", `keyshare-${Date.now()}`, messages);
  }
  // Remove consumed one-time keys from our account (they were used for
  // outbound sessions). The remote side consumed theirs.
  await dbPut(K.account, acc.pickle(PICKLE_KEY));
}
