import { useEffect, useState } from "react";
import {
  adminListUsers,
  adminListRooms,
  adminStatistics,
  adminDeactivate,
  adminSetPassword,
  type AdminUser,
  type AdminRoom,
  type AdminStats,
} from "../lib/matrix";
import { Card, Badge, Button, Input, Table } from "../components/ui/primitives";

export function AdminPage() {
  const [stats, setStats] = useState<AdminStats | null>(null);
  const [users, setUsers] = useState<AdminUser[]>([]);
  const [rooms, setRooms] = useState<AdminRoom[]>([]);
  const [error, setError] = useState("");
  const [pwUser, setPwUser] = useState("");
  const [pwValue, setPwValue] = useState("");

  const load = async () => {
    setError("");
    try {
      const [s, u, r] = await Promise.all([adminStatistics(), adminListUsers(), adminListRooms()]);
      setStats(s);
      setUsers(u.users);
      setRooms(r.rooms);
    } catch (e) {
      setError(e instanceof Error ? e.message : "Admin access requires an admin account");
    }
  };

  useEffect(() => { load(); }, []);

  return (
    <div className="page">
      <h1>Admin Panel</h1>
      {error && <div className="error">{error}</div>}
      <div className="grid-2">
        <Card title="Statistics">
          {stats ? (
            <div className="col">
              <div className="row" style={{ justifyContent: "space-between" }}>
                <span className="muted">Users</span><strong>{stats.users}</strong>
              </div>
              <div className="row" style={{ justifyContent: "space-between" }}>
                <span className="muted">Rooms</span><strong>{stats.rooms}</strong>
              </div>
              <div className="row" style={{ justifyContent: "space-between" }}>
                <span className="muted">Events</span><strong>{stats.events}</strong>
              </div>
            </div>
          ) : <span className="muted">loading…</span>}
        </Card>
        <Card title="Set password">
          <div className="col">
            <Input placeholder="@user:server" value={pwUser}
              onChange={(e) => setPwUser(e.target.value)} />
            <Input type="password" placeholder="new password" value={pwValue}
              onChange={(e) => setPwValue(e.target.value)} />
            <Button variant="primary" onClick={async () => {
              try { await adminSetPassword(pwUser, pwValue); setPwValue(""); }
              catch (e) { setError(e instanceof Error ? e.message : "failed"); }
            }}>Set password</Button>
          </div>
        </Card>
      </div>

      <Card title="Users" className="mt">
        <Table head={["User", "Admin", "Deactivated", "Guest", "Actions"]}>
          {users.map((u) => (
            <tr key={u.name}>
              <td>{u.name}</td>
              <td>{u.admin ? <Badge>admin</Badge> : <Badge variant="muted">no</Badge>}</td>
              <td>{u.deactivated ? <Badge variant="danger">yes</Badge> : <Badge variant="muted">no</Badge>}</td>
              <td>{u.is_guest ? <Badge variant="muted">guest</Badge> : "—"}</td>
              <td>
                {!u.deactivated && (
                  <Button variant="danger" className="btn-sm"
                    onClick={async () => { await adminDeactivate(u.name); load(); }}>
                    Deactivate
                  </Button>
                )}
              </td>
            </tr>
          ))}
          {users.length === 0 && <tr><td colSpan={5} className="muted">no users</td></tr>}
        </Table>
      </Card>

      <Card title="Rooms" className="mt">
        <Table head={["Room ID", "Version", "Creator", "Public"]}>
          {rooms.map((r) => (
            <tr key={r.room_id}>
              <td style={{ fontFamily: "monospace", fontSize: 12 }}>{r.room_id}</td>
              <td>{r.version}</td>
              <td>{r.creator}</td>
              <td>{r.is_public ? <Badge>public</Badge> : <Badge variant="muted">private</Badge>}</td>
            </tr>
          ))}
          {rooms.length === 0 && <tr><td colSpan={4} className="muted">no rooms</td></tr>}
        </Table>
      </Card>
    </div>
  );
}
