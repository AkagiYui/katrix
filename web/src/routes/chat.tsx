import { useEffect, useRef, useState } from "react";
import {
  getToken,
  setToken,
  createRoom as apiCreateRoom,
  joinRoom as apiJoinRoom,
  sendMessage as apiSendMessage,
} from "../lib/matrix";
import type { MatrixEvent } from "../App";

interface JoinedRoom {
  timeline: { events: MatrixEvent[] };
  state?: { events: MatrixEvent[] };
}

interface SyncResponse {
  next_batch: string;
  rooms?: {
    join?: Record<string, JoinedRoom>;
    invite?: Record<string, unknown>;
  };
}

export function ChatPage() {
  const [rooms, setRooms] = useState<Record<string, JoinedRoom>>({});
  const [activeRoom, setActiveRoom] = useState<string | null>(null);
  const [since, setSince] = useState<string>("");
  const [roomInput, setRoomInput] = useState("");

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
          if (data.rooms?.join) {
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
          }
        } catch {
          await new Promise((r) => setTimeout(r, 2000));
        }
      }
    };
    loop();
    return () => { cancelled = true; };
  }, [since]);

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
            <ChatView roomId={activeRoom} room={rooms[activeRoom]} />
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

function ChatView({ roomId, room }: { roomId: string; room?: JoinedRoom }) {
  const [text, setText] = useState("");
  const events = room?.timeline.events ?? [];
  const messages = events.filter((e) => e.type === "m.room.message");
  const send = async () => {
    if (!text.trim()) return;
    const txnId = `m${Date.now()}`;
    await apiSendMessage(roomId, txnId, text.trim());
    setText("");
  };
  return (
    <>
      <div className="messages">
        {messages.length === 0 && <div className="muted">No messages yet. Say hello!</div>}
        {messages.map((e, i) => (
          <div className="msg" key={e.event_id ?? i}>
            <div className="sender">{e.sender}</div>
            <div className="body">{e.content.body}</div>
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
