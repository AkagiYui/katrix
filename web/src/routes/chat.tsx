import { useEffect, useRef, useState, type ReactNode } from "react";
import {
  getToken,
  getUserId,
  setToken,
  createRoom as apiCreateRoom,
  joinRoom as apiJoinRoom,
  knockRoom as apiKnockRoom,
  leaveRoom as apiLeaveRoom,
  sendMessage as apiSendMessage,
  sendEncryptedMessage as apiSendEncrypted,
} from "../lib/matrix";
import {
  previewRoom,
  joinCandidates,
  type InvitedRoomSync,
  type KnockedRoomSync,
  type RoomPreview,
  type StrippedStateEvent,
} from "../lib/stripped-state";
import {
  bootstrapE2EE,
  getDeviceId,
  encryptRoomMessage,
  decryptRoomMessage,
  importRoomKey,
  shareRoomKey,
  queryUserDevices,
  decryptToDevice,
  getOutboundGroupSession,
} from "../lib/e2ee";
import type { MatrixEvent } from "../App";

interface JoinedRoom {
  timeline: { events: MatrixEvent[] };
  state?: { events: MatrixEvent[] };
  ephemeral?: { events: MatrixEvent[] };
}

interface ToDevice {
  events: MatrixEvent[];
}

interface SyncResponse {
  next_batch: string;
  rooms?: {
    join?: Record<string, JoinedRoom>;
    invite?: Record<string, InvitedRoomSync>;
    knock?: Record<string, KnockedRoomSync>;
    leave?: Record<string, unknown>;
  };
  to_device?: ToDevice;
}

type PendingRooms = Record<string, StrippedStateEvent[]>;

/** A room's current state, keyed by "type\u0000state_key". */
type StateMap = Record<string, MatrixEvent>;

/** The client's view of a joined room. */
interface RoomView {
  timeline: MatrixEvent[];
  state: StateMap;
}

/**
 * Fold a /sync joined-room section into the room's current state. `state`
 * carries the updates up to the start of the timeline, and state events in
 * the timeline apply after them (spec /sync). For a newly joined room whose
 * history fits in the timeline, all of its state arrives in the timeline.
 */
function applyState(prev: StateMap, r: JoinedRoom): StateMap {
  const next = { ...prev };
  for (const e of [...(r.state?.events ?? []), ...r.timeline.events]) {
    if (e.state_key === undefined) continue;
    next[`${e.type}\u0000${e.state_key}`] = e;
  }
  return next;
}

/** User IDs of the joined members in a room's current state. */
function joinedMembers(state: StateMap): string[] {
  return Object.values(state)
    .filter((e) => e.type === "m.room.member" && e.content.membership === "join")
    .map((e) => e.state_key ?? e.sender);
}

/**
 * Fold a sync response's pending memberships into the client's view. A room's
 * stripped state is only meaningful while the membership is pending: once the
 * user joins or leaves (declines / retracts, or is rejected) it is discarded
 * (spec §Stripped state), and an accepted knock turns into an invite.
 */
function foldPending(
  prev: PendingRooms,
  incoming: Record<string, StrippedStateEvent[]>,
  settled: string[],
): PendingRooms {
  if (settled.length === 0 && Object.keys(incoming).length === 0) return prev;
  const next = { ...prev };
  for (const id of settled) delete next[id];
  Object.assign(next, incoming);
  return next;
}

export function ChatPage() {
  const [rooms, setRooms] = useState<Record<string, RoomView>>({});
  const [invites, setInvites] = useState<PendingRooms>({});
  const [knocks, setKnocks] = useState<PendingRooms>({});
  const [pendingError, setPendingError] = useState("");
  const userId = getUserId();
  const [activeRoom, setActiveRoom] = useState<string | null>(null);
  const [since, setSince] = useState<string>("");
  const [roomInput, setRoomInput] = useState("");
  const [e2eeReady, setE2eeReady] = useState(false);
  const [e2eeError, setE2eeError] = useState("");
  // Track rooms we've shared the room key for (avoid re-sharing every message).
  const sharedRooms = useRef<Set<string>>(new Set());

  // Bootstrap E2EE on mount: init Olm, load/create account, upload device keys.
  useEffect(() => {
    let cancelled = false;
    (async () => {
      try {
        // Resolve the current device id from /whoami.
        const resp = await fetch("/_matrix/client/v3/account/whoami", {
          headers: { Authorization: `Bearer ${getToken()}` },
        });
        if (!resp.ok) return;
        const who = await resp.json();
        await bootstrapE2EE(who.device_id);
        if (!cancelled) setE2eeReady(true);
      } catch (e) {
        if (!cancelled) setE2eeError(e instanceof Error ? e.message : "E2EE init failed");
      }
    })();
    return () => { cancelled = true; };
  }, []);

  // Sync loop.
  useEffect(() => {
    let cancelled = false;
    const loop = async () => {
      while (!cancelled) {
        try {
          const path =
            `/_matrix/client/v3/sync?timeout=5000` +
            (since ? `&since=${since}` : "");
          const headers: Record<string, string> = {};
          const tok = getToken();
          if (tok) headers["Authorization"] = `Bearer ${tok}`;
          const resp = await fetch(path, { headers });
          if (!resp.ok) {
            if (resp.status === 401) {
              setToken(null);
              window.location.hash = "#/";
              window.location.reload();
              return;
            }
            await new Promise((r) => setTimeout(r, 1000));
            continue;
          }
          const data = (await resp.json()) as SyncResponse;
          setSince(data.next_batch ?? "");

          // Pending memberships: invites and knocks carry stripped state.
          const joinedIds = Object.keys(data.rooms?.join ?? {});
          const leftIds = Object.keys(data.rooms?.leave ?? {});
          const newInvites = Object.fromEntries(
            Object.entries(data.rooms?.invite ?? {}).map(([id, r]) => [id, r.invite_state?.events ?? []]),
          );
          const newKnocks = Object.fromEntries(
            Object.entries(data.rooms?.knock ?? {}).map(([id, r]) => [id, r.knock_state?.events ?? []]),
          );
          setInvites((prev) => foldPending(prev, newInvites, [...joinedIds, ...leftIds]));
          setKnocks((prev) =>
            foldPending(prev, newKnocks, [...joinedIds, ...leftIds, ...Object.keys(newInvites)]),
          );

          // Process to-device events: room keys (m.room_key via m.encrypted).
          if (data.to_device?.events) {
            for (const ev of data.to_device.events) {
              await handleToDevice(ev);
            }
          }

          if (data.rooms?.join) {
            // Collect member user ids across rooms for device-key queries.
            const deltas: Record<string, StateMap> = {};
            const allUsers = new Set<string>();
            for (const [id, r] of Object.entries(data.rooms.join)) {
              deltas[id] = applyState({}, r);
              for (const u of joinedMembers(deltas[id])) allUsers.add(u);
            }
            if (allUsers.size > 0) {
              queryUserDevices([...allUsers]).catch(() => {});
            }

            setRooms((prev) => {
              const next = { ...prev };
              for (const [id, r] of Object.entries(data.rooms!.join!)) {
                const cur = next[id] ?? { timeline: [], state: {} };
                next[id] = {
                  timeline: [...cur.timeline, ...r.timeline.events],
                  state: applyState(cur.state, r),
                };
              }
              return next;
            });

            // Share room keys for rooms we haven't shared yet.
            if (e2eeReady) {
              for (const id of Object.keys(data.rooms.join)) {
                if (sharedRooms.current.has(id)) continue;
                const members = joinedMembers(deltas[id]);
                if (members.length > 0) {
                  // Ensure an outbound session exists before sharing.
                  await getOutboundGroupSession(id).catch(() => {});
                  await shareRoomKey(id, members).catch(() => {});
                  sharedRooms.current.add(id);
                }
              }
            }
          }
        } catch {
          await new Promise((r) => setTimeout(r, 2000));
        }
      }
    };
    if (e2eeReady || e2eeError) loop();
    return () => { cancelled = true; };
  }, [since, e2eeReady, e2eeError]);

  const roomList = Object.keys(rooms);
  const roomName = (id: string): string =>
    rooms[id]?.state["m.room.name\u0000"]?.content?.name || id;

  const handleCreate = async () => {
    const name = roomInput || `Room ${roomList.length + 1}`;
    const { room_id } = await apiCreateRoom({ name, preset: "public_chat" });
    setRoomInput("");
    setActiveRoom(room_id);
  };

  const handleJoin = async () => {
    if (!roomInput) return;
    const { room_id } = await apiJoinRoom(roomInput);
    setActiveRoom(room_id);
    setRoomInput("");
  };

  const handleKnock = async () => {
    if (!roomInput) return;
    setPendingError("");
    try {
      await apiKnockRoom(roomInput);
      setRoomInput("");
    } catch (e) {
      setPendingError(e instanceof Error ? e.message : "Knock failed");
    }
  };

  const acceptInvite = async (preview: RoomPreview) => {
    setPendingError("");
    try {
      const { room_id } = await apiJoinRoom(preview.roomId, joinCandidates(preview));
      setActiveRoom(room_id);
    } catch (e) {
      setPendingError(e instanceof Error ? e.message : "Could not join the room");
    }
  };

  const leavePending = async (preview: RoomPreview) => {
    setPendingError("");
    try {
      await apiLeaveRoom(preview.roomId);
    } catch (e) {
      setPendingError(e instanceof Error ? e.message : "Could not leave the room");
    }
  };

  const invitePreviews = Object.entries(invites).map(([id, evs]) => previewRoom(id, evs, userId));
  const knockPreviews = Object.entries(knocks).map(([id, evs]) => previewRoom(id, evs, userId));

  return (
    <>
      <div style={{ padding: "12px 16px", borderBottom: "1px solid var(--border)" }}>
        <div className="row gap-12">
          <input className="input" placeholder="room id / name" value={roomInput}
            onChange={(e) => setRoomInput(e.target.value)} />
          <button className="btn btn-sm" onClick={handleCreate}>+ Create</button>
          <button className="btn btn-sm" onClick={handleJoin}>Join</button>
          <button className="btn btn-sm" onClick={handleKnock} title="Ask to join a room with join rule “knock”">
            Knock
          </button>
        </div>
        {e2eeError && (
          <div className="muted" style={{ fontSize: 11, marginTop: 6 }}>
            E2EE unavailable: {e2eeError}. Messages will be sent in plaintext.
          </div>
        )}
        {e2eeReady && (
          <div className="muted" style={{ fontSize: 11, marginTop: 6 }}>
            🔒 End-to-end encryption active
          </div>
        )}
      </div>
      <div className="row" style={{ gap: 0, flex: 1, minHeight: 0, alignItems: "stretch" }}>
        <div style={{ width: 240, borderRight: "1px solid var(--border)", overflowY: "auto", padding: 8 }}>
          {pendingError && <div className="error" style={{ margin: "0 4px 8px" }}>{pendingError}</div>}
          {invitePreviews.length > 0 && (
            <PendingSection title="Invites">
              {invitePreviews.map((p) => (
                <PendingRoomCard key={p.roomId} preview={p} kind="invite"
                  onAccept={() => acceptInvite(p)} onLeave={() => leavePending(p)} />
              ))}
            </PendingSection>
          )}
          {knockPreviews.length > 0 && (
            <PendingSection title="Requests to join">
              {knockPreviews.map((p) => (
                <PendingRoomCard key={p.roomId} preview={p} kind="knock" onLeave={() => leavePending(p)} />
              ))}
            </PendingSection>
          )}
          {(invitePreviews.length > 0 || knockPreviews.length > 0) && (
            <div className="muted" style={{ padding: "8px 4px 4px", fontWeight: 600 }}>Rooms</div>
          )}
          {roomList.map((id) => (
            <div key={id} className={`room-list-item${activeRoom === id ? " active" : ""}`}
              onClick={() => setActiveRoom(id)}>
              <span>{roomName(id)}</span>
            </div>
          ))}
          {roomList.length === 0 && <div className="muted" style={{ padding: 8 }}>No rooms yet</div>}
        </div>
        <div style={{ flex: 1, display: "flex", flexDirection: "column", minWidth: 0 }}>
          {activeRoom ? (
            <ChatView roomId={activeRoom} room={rooms[activeRoom]} e2eeReady={e2eeReady} />
          ) : (
            <div style={{ flex: 1, display: "flex", alignItems: "center", justifyContent: "center" }}
              className="muted">
              Select or create a room
            </div>
          )}
        </div>
      </div>
    </>
  );
}

function PendingSection({ title, children }: { title: string; children: ReactNode }) {
  return (
    <div style={{ marginBottom: 8 }}>
      <div className="muted" style={{ padding: "4px 4px 6px", fontWeight: 600 }}>{title}</div>
      <div className="col" style={{ gap: 6 }}>{children}</div>
    </div>
  );
}

/**
 * A room the user has a pending membership in, previewed from its stripped
 * state: the room is not joined yet, so nothing but the stripped state is
 * known about it.
 */
function PendingRoomCard({
  preview,
  kind,
  onAccept,
  onLeave,
}: {
  preview: RoomPreview;
  kind: "invite" | "knock";
  onAccept?: () => Promise<void>;
  onLeave: () => Promise<void>;
}) {
  const [busy, setBusy] = useState(false);
  const run = (fn?: () => Promise<void>) => async () => {
    if (!fn) return;
    setBusy(true);
    try {
      await fn();
    } finally {
      setBusy(false);
    }
  };
  const badges: string[] = [];
  if (preview.roomType === "m.space") badges.push("Space");
  if (preview.isDirect) badges.push("Direct");
  if (preview.encrypted) badges.push("🔒 Encrypted");
  if (preview.joinRule && preview.joinRule !== "invite") badges.push(preview.joinRule);

  return (
    <div className="card" style={{ padding: 10 }}>
      <div style={{ fontWeight: 600, wordBreak: "break-word" }} title={preview.roomId}>{preview.name}</div>
      {kind === "invite" && preview.inviter && (
        <div className="muted" style={{ fontSize: 12 }}>
          Invited by {preview.inviterDisplayName ?? preview.inviter}
        </div>
      )}
      {kind === "knock" && <div className="muted" style={{ fontSize: 12 }}>Waiting for a member to let you in</div>}
      {preview.topic && (
        <div className="muted" style={{ fontSize: 12, marginTop: 4, wordBreak: "break-word" }}>{preview.topic}</div>
      )}
      {preview.reason && (
        <div className="muted" style={{ fontSize: 12, marginTop: 4, fontStyle: "italic" }}>“{preview.reason}”</div>
      )}
      {badges.length > 0 && (
        <div className="row" style={{ gap: 4, flexWrap: "wrap", marginTop: 6 }}>
          {badges.map((b) => <span key={b} className="badge badge-muted">{b}</span>)}
        </div>
      )}
      <div className="row" style={{ gap: 6, marginTop: 8 }}>
        {kind === "invite" && (
          <button className="btn btn-sm btn-primary" disabled={busy} onClick={run(onAccept)}>Accept</button>
        )}
        <button className="btn btn-sm btn-danger" disabled={busy} onClick={run(onLeave)}>
          {kind === "invite" ? "Decline" : "Cancel request"}
        </button>
      </div>
    </div>
  );
}

/** Handle an inbound to-device event: decrypt m.encrypted to extract room keys. */
async function handleToDevice(ev: MatrixEvent): Promise<void> {
  if (ev.type !== "m.encrypted") return;
  const content = ev.content;
  if (!content.ciphertext || !content.sender_key) return;
  // content.ciphertext is a map of device curve25519 -> {type, body}.
  const myDeviceId = await getDeviceId();
  if (!myDeviceId) return;
  // Try each ciphertext entry (only ours will decrypt with our account).
  const ciphertextMap = typeof content.ciphertext === "string"
    ? null
    : parseCiphertextMap(content.ciphertext as unknown as string);
  if (ciphertextMap) {
    for (const [deviceKey, entry] of Object.entries(ciphertextMap)) {
      const plaintext = await decryptToDevice(
        ev.sender,
        JSON.stringify(entry),
        myDeviceId,
      ).catch(() => null);
      if (plaintext) {
        try {
          const roomKey = JSON.parse(plaintext);
          if (roomKey.type === "m.room_key" || roomKey.algorithm === "m.megolm.v1.aes-sha2") {
            await importRoomKey(roomKey);
          }
        } catch { /* ignore malformed */ }
        break;
      }
    }
  }
}

/** Parse the ciphertext object from a to-device m.encrypted event. */
function parseCiphertextMap(raw: string): Record<string, { type: number; body: string }> | null {
  try {
    return JSON.parse(raw);
  } catch {
    return null;
  }
}

function ChatView({ roomId, room, e2eeReady }: { roomId: string; room?: RoomView; e2eeReady: boolean }) {
  const [text, setText] = useState("");
  const [decrypted, setDecrypted] = useState<Record<string, string>>({});
  const events = room?.timeline ?? [];

  // Decrypt any m.room.encrypted events we have inbound sessions for.
  useEffect(() => {
    let cancelled = false;
    (async () => {
      const updates: Record<string, string> = {};
      for (const e of events) {
        if (e.type !== "m.room.encrypted") continue;
        if (!e.content.session_id || !e.content.ciphertext) continue;
        if (decrypted[e.event_id ?? ""]) continue;
        const pt = await decryptRoomMessage(roomId, e.content.session_id, e.content.ciphertext);
        if (pt && !cancelled) updates[e.event_id ?? ""] = pt;
      }
      if (!cancelled && Object.keys(updates).length > 0) {
        setDecrypted((prev) => ({ ...prev, ...updates }));
      }
    })();
    return () => { cancelled = true; };
  }, [events, roomId]);

  const messages = events.filter((e) => e.type === "m.room.message" || e.type === "m.room.encrypted");
  const send = async () => {
    if (!text.trim()) return;
    const txnId = `m${Date.now()}`;
    if (e2eeReady) {
      // Encrypt and send as m.room.encrypted.
      try {
        const plaintext = JSON.stringify({ body: text.trim(), msgtype: "m.text" });
        const enc = await encryptRoomMessage(roomId, plaintext);
        await apiSendEncrypted(roomId, txnId, enc);
      } catch {
        // Fall back to plaintext if encryption fails.
        await apiSendMessage(roomId, txnId, { body: text.trim(), msgtype: "m.text" });
      }
    } else {
      await apiSendMessage(roomId, txnId, { body: text.trim(), msgtype: "m.text" });
    }
    setText("");
  };

  const renderBody = (e: MatrixEvent): string => {
    if (e.type === "m.room.message") return e.content.body ?? "";
    if (e.type === "m.room.encrypted") {
      const id = e.event_id ?? "";
      if (decrypted[id]) {
        try {
          const parsed = JSON.parse(decrypted[id]);
          return parsed.body ?? decrypted[id];
        } catch {
          return decrypted[id];
        }
      }
      return "🔒 Unable to decrypt (waiting for room key…)";
    }
    return "";
  };

  return (
    <>
      <div className="messages">
        {messages.length === 0 && <div className="muted">No messages yet. Say hello!</div>}
        {messages.map((e, i) => (
          <div className="msg" key={e.event_id ?? i}>
            <div className="sender">{e.sender}</div>
            <div className="body">{renderBody(e)}</div>
          </div>
        ))}
      </div>
      <div className="composer">
        <input className="input" value={text}
          onChange={(e) => setText(e.target.value)}
          onKeyDown={(e) => { if (e.key === "Enter") send(); }}
          placeholder="Type a message…" />
        <button className="btn btn-primary" onClick={send}>Send</button>
      </div>
    </>
  );
}
