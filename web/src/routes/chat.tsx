import { useEffect, useRef, useState } from "react";
import {
  getToken,
  setToken,
  createRoom as apiCreateRoom,
  joinRoom as apiJoinRoom,
  sendMessage as apiSendMessage,
  sendEncryptedMessage as apiSendEncrypted,
} from "../lib/matrix";
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
    invite?: Record<string, unknown>;
  };
  to_device?: ToDevice;
}

export function ChatPage() {
  const [rooms, setRooms] = useState<Record<string, JoinedRoom>>({});
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

          // Process to-device events: room keys (m.room_key via m.encrypted).
          if (data.to_device?.events) {
            for (const ev of data.to_device.events) {
              await handleToDevice(ev);
            }
          }

          if (data.rooms?.join) {
            // Collect member user ids across rooms for device-key queries.
            const allUsers = new Set<string>();
            for (const r of Object.values(data.rooms.join!)) {
              for (const e of r.state?.events ?? []) {
                if (e.type === "m.room.member" && e.content.membership === "join") {
                  allUsers.add(e.state_key ?? e.sender);
                }
              }
            }
            if (allUsers.size > 0) {
              queryUserDevices([...allUsers]).catch(() => {});
            }

            setRooms((prev) => {
              const next = { ...prev };
              for (const [id, r] of Object.entries(data.rooms!.join!)) {
                if (next[id]) {
                  next[id] = {
                    ...next[id],
                    timeline: {
                      events: [...next[id].timeline.events, ...r.timeline.events],
                    },
                    state: next[id].state ?? r.state,
                  };
                } else {
                  next[id] = r;
                }
              }
              return next;
            });

            // Share room keys for rooms we haven't shared yet.
            if (e2eeReady) {
              for (const [id, r] of Object.entries(data.rooms.join)) {
                if (sharedRooms.current.has(id)) continue;
                const members = (r.state?.events ?? [])
                  .filter((e) => e.type === "m.room.member" && e.content.membership === "join")
                  .map((e) => e.state_key ?? e.sender);
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
  const roomName = (id: string): string => {
    const stateEvents = rooms[id]?.state?.events ?? [];
    const nameEv = stateEvents.find((e) => e.type === "m.room.name");
    return nameEv?.content?.name || id;
  };

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

  return (
    <>
      <div style={{ padding: "12px 16px", borderBottom: "1px solid var(--border)" }}>
        <div className="row gap-12">
          <input className="input" placeholder="room id / name" value={roomInput}
            onChange={(e) => setRoomInput(e.target.value)} />
          <button className="btn btn-sm" onClick={handleCreate}>+ Create</button>
          <button className="btn btn-sm" onClick={handleJoin}>Join</button>
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
      <div className="row" style={{ gap: 0, flex: 1, minHeight: 0 }}>
        <div style={{ width: 240, borderRight: "1px solid var(--border)", overflowY: "auto", padding: 8 }}>
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

function ChatView({ roomId, room, e2eeReady }: { roomId: string; room?: JoinedRoom; e2eeReady: boolean }) {
  const [text, setText] = useState("");
  const [decrypted, setDecrypted] = useState<Record<string, string>>({});
  const events = room?.timeline.events ?? [];

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
