import { useEffect, useRef, useState } from "react";
import {
  getToken,
  getUserId,
  setToken,
  login as apiLogin,
  register as apiRegister,
  logout as apiLogout,
  whoami,
  createRoom as apiCreateRoom,
  joinRoom as apiJoinRoom,
  sendMessage as apiSendMessage,
} from "./lib/matrix";

interface JoinedRoom {
  timeline: { events: MatrixEvent[] };
  state?: { events: MatrixEvent[] };
}

interface MatrixEvent {
  type: string;
  state_key?: string;
  sender: string;
  content: { body?: string; name?: string; membership?: string };
  origin_server_ts?: number;
  event_id?: string;
}

interface SyncResponse {
  next_batch: string;
  rooms?: {
    join?: Record<string, JoinedRoom>;
    invite?: Record<string, unknown>;
  };
}

export function App() {
  const [authed, setAuthed] = useState<boolean>(!!getToken());
  const [userId, setUserId] = useState<string | null>(getUserId());
  const [rooms, setRooms] = useState<Record<string, JoinedRoom>>({});
  const [activeRoom, setActiveRoom] = useState<string | null>(null);
  const [since, setSince] = useState<string>("");
  const [roomInput, setRoomInput] = useState("");
  const syncRef = useRef<number | null>(null);

  // Verify token on mount.
  useEffect(() => {
    if (getToken()) {
      whoami().then(() => setAuthed(true)).catch(() => {
        setToken(null);
        setAuthed(false);
      });
    }
  }, []);

  // Sync loop.
  useEffect(() => {
    if (!authed) return;
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
              setAuthed(false);
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
    return () => {
      cancelled = true;
      if (syncRef.current) window.clearTimeout(syncRef.current);
    };
  }, [authed, since]);

  if (!authed) {
    return <AuthView onAuthed={(uid) => { setAuthed(true); setUserId(uid); }} />;
  }

  const roomList = Object.keys(rooms);
  const roomName = (id: string): string => {
    const stateEvents = rooms[id]?.state?.events ?? [];
    const nameEv = stateEvents.find((e) => e.type === "m.room.name");
    return nameEv?.content?.name || id;
  };

  const handleLogout = async () => {
    try { await apiLogout(); } catch { /* ignore */ }
    setToken(null);
    setAuthed(false);
    setUserId(null);
    setRooms({});
    setSince("");
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
    <div className="app">
      <div className="sidebar">
        <div style={{ display: "flex", justifyContent: "space-between", alignItems: "center", marginBottom: 12 }}>
          <strong>{userId ?? "Katrix"}</strong>
          <button onClick={handleLogout}>Logout</button>
        </div>
        <div style={{ display: "flex", gap: 6, marginBottom: 12 }}>
          <input
            placeholder="room id / name"
            value={roomInput}
            onChange={(e) => setRoomInput(e.target.value)}
          />
        </div>
        <div style={{ display: "flex", gap: 6, marginBottom: 12 }}>
          <button style={{ flex: 1 }} onClick={handleCreate}>+ Create</button>
          <button style={{ flex: 1 }} onClick={handleJoin}>Join</button>
        </div>
        {roomList.map((id) => (
          <div
            key={id}
            className={`room-list-item${activeRoom === id ? " active" : ""}`}
            onClick={() => setActiveRoom(id)}
          >
            {roomName(id)}
          </div>
        ))}
      </div>
      <div className="main">
        {activeRoom ? (
          <ChatView roomId={activeRoom} room={rooms[activeRoom]} />
        ) : (
          <div style={{ flex: 1, display: "flex", alignItems: "center", justifyContent: "center", color: "#6b7280" }}>
            Select or create a room
          </div>
        )}
      </div>
    </div>
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
        {messages.length === 0 && (
          <div style={{ color: "#6b7280" }}>No messages yet. Say hello!</div>
        )}
        {messages.map((e, i) => (
          <div className="msg" key={e.event_id ?? i}>
            <div className="sender">{e.sender}</div>
            <div className="body">{e.content.body}</div>
          </div>
        ))}
      </div>
      <div className="composer">
        <input
          value={text}
          onChange={(e) => setText(e.target.value)}
          onKeyDown={(e) => { if (e.key === "Enter") send(); }}
          placeholder="Type a message…"
        />
        <button onClick={send}>Send</button>
      </div>
    </>
  );
}

function AuthView({ onAuthed }: { onAuthed: (userId: string) => void }) {
  const [mode, setMode] = useState<"login" | "register">("login");
  const [username, setUsername] = useState("");
  const [password, setPassword] = useState("");
  const [error, setError] = useState("");
  const [busy, setBusy] = useState(false);

  const submit = async (e: React.FormEvent) => {
    e.preventDefault();
    setError("");
    setBusy(true);
    try {
      const fn = mode === "login" ? apiLogin : apiRegister;
      const res = await fn(username, password);
      setToken(res.access_token, res.user_id);
      onAuthed(res.user_id);
    } catch (err) {
      setError(err instanceof Error ? err.message : "Authentication failed");
    } finally {
      setBusy(false);
    }
  };

  return (
    <div className="auth-card">
      <h1>Katrix</h1>
      <form onSubmit={submit}>
        <input
          placeholder="username"
          value={username}
          onChange={(e) => setUsername(e.target.value)}
          autoFocus
        />
        <input
          type="password"
          placeholder="password"
          value={password}
          onChange={(e) => setPassword(e.target.value)}
        />
        <button disabled={busy || !username || !password}>
          {mode === "login" ? "Login" : "Register"}
        </button>
      </form>
      {error && <div className="error">{error}</div>}
      <div className="auth-toggle">
        {mode === "login" ? (
          <>No account? <a onClick={() => setMode("register")}>Register</a></>
        ) : (
          <>Have an account? <a onClick={() => setMode("login")}>Login</a></>
        )}
      </div>
    </div>
  );
}
