import { useEffect, useState } from "react";
import {
  createRouter,
  createRootRoute,
  createRoute,
  RouterProvider,
  Outlet,
} from "@tanstack/react-router";
import {
  getToken,
  getUserId,
  setToken,
  login as apiLogin,
  register as apiRegister,
  logout as apiLogout,
  whoami,
} from "./lib/matrix";
import { ChatPage } from "./routes/chat";
import { AdminPage } from "./routes/admin";
import { DevicesPage } from "./routes/devices";

interface JoinedRoom {
  timeline: { events: MatrixEvent[] };
  state?: { events: MatrixEvent[] };
}

export interface MatrixEvent {
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

// Auth context shared across routes.
function useAuth() {
  const [authed, setAuthed] = useState<boolean>(!!getToken());
  const [userId, setUserId] = useState<string | null>(getUserId());

  useEffect(() => {
    if (getToken()) {
      whoami().then(() => setAuthed(true)).catch(() => {
        setToken(null);
        setAuthed(false);
      });
    }
  }, []);

  return { authed, userId, setAuthed, setUserId };
}

const rootRoute = createRootRoute({
  component: RootView,
});

function RootView() {
  const { authed, userId, setAuthed, setUserId } = useAuth();

  if (!authed) {
    return <AuthView onAuthed={(uid) => { setAuthed(true); setUserId(uid); }} />;
  }
  return (
    <div className="app">
      <Sidebar userId={userId} onLogout={async () => {
        try { await apiLogout(); } catch { /* ignore */ }
        setToken(null);
        setAuthed(false);
        setUserId(null);
      }} />
      <div className="main">
        <Outlet />
      </div>
    </div>
  );
}

function Sidebar({ userId, onLogout }: { userId: string | null; onLogout: () => void }) {
  return (
    <div className="sidebar">
      <div className="row" style={{ justifyContent: "space-between", marginBottom: 12 }}>
        <strong>{userId ?? "Katrix"}</strong>
        <button className="btn btn-sm" onClick={onLogout}>Logout</button>
      </div>
      <a className="nav-link" href="#/">Chat</a>
      <a className="nav-link" href="#/devices">Devices & E2EE</a>
      <a className="nav-link" href="#/admin">Admin</a>
    </div>
  );
}

const chatRoute = createRoute({
  getParentRoute: () => rootRoute,
  path: "/",
  component: ChatPage,
});

const devicesRoute = createRoute({
  getParentRoute: () => rootRoute,
  path: "/devices",
  component: DevicesPage,
});

const adminRoute = createRoute({
  getParentRoute: () => rootRoute,
  path: "/admin",
  component: AdminPage,
});

const routeTree = rootRoute.addChildren([chatRoute, devicesRoute, adminRoute]);

const router = createRouter({ routeTree });

declare module "@tanstack/react-router" {
  interface Register {
    router: typeof router;
  }
}

export function App() {
  return <RouterProvider router={router} />;
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
      <form onSubmit={submit} className="col">
        <input className="input" placeholder="username" value={username}
          onChange={(e) => setUsername(e.target.value)} autoFocus />
        <input className="input" type="password" placeholder="password" value={password}
          onChange={(e) => setPassword(e.target.value)} />
        <button className="btn btn-primary" disabled={busy || !username || !password}>
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
