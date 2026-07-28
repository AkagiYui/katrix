import { useEffect, useState } from "react";
import {
  getToken,
  getUserId,
  listDevices,
  mintLoginToken,
  loginWithToken,
  type Device,
} from "../lib/matrix";
import {
  bootstrapE2EE,
  queryUserDevices,
} from "../lib/e2ee";
import { Card, Badge, Button, Table } from "../components/ui/primitives";

export function DevicesPage() {
  const [devices, setDevices] = useState<Device[]>([]);
  const [e2eeStatus, setE2eeStatus] = useState<{ ed25519: string; curve25519: string } | null>(null);
  const [bootstrapping, setBootstrapping] = useState(false);
  const [error, setError] = useState("");
  const [qrToken, setQrToken] = useState("");
  const [qrMsg, setQrMsg] = useState("");

  const load = async () => {
    setError("");
    try {
      const d = await listDevices();
      setDevices(d.devices);
    } catch (e) {
      setError(e instanceof Error ? e.message : "failed");
    }
  };
  useEffect(() => { load(); }, []);

  const bootstrap = async () => {
    setError("");
    setBootstrapping(true);
    try {
      // Resolve the current device id from /whoami.
      const resp = await fetch("/_matrix/client/v3/account/whoami", {
        headers: { Authorization: `Bearer ${getToken()}` },
      });
      const who = await resp.json();
      const status = await bootstrapE2EE(who.device_id);
      setE2eeStatus(status);
    } catch (e) {
      setError(e instanceof Error ? e.message : "bootstrap failed");
    } finally {
      setBootstrapping(false);
    }
  };

  const refreshKeys = async () => {
    setError("");
    try {
      const userId = getUserId() ?? "";
      await queryUserDevices([userId]);
      await load();
    } catch (e) {
      setError(e instanceof Error ? e.message : "query failed");
    }
  };

  const mintQR = async () => {
    setError("");
    setQrMsg("");
    try {
      const r = await mintLoginToken();
      setQrToken(r.token);
      setQrMsg(`Token valid for ${r.expires_in}s. Scan from another device.`);
    } catch (e) {
      setError(e instanceof Error ? e.message : "mint failed");
    }
  };

  const consumeQR = async () => {
    setError("");
    try {
      await loginWithToken(qrToken, "QRNEWDEVICE");
      setQrMsg("Logged in a new device via the token.");
      load();
    } catch (e) {
      setError(e instanceof Error ? e.message : "consume failed");
    }
  };

  return (
    <div className="page">
      <h1>Devices &amp; E2EE</h1>
      {error && <div className="error">{error}</div>}

      <Card title="Devices" className="mt">
        <Table head={["Device ID", "Display name", "Last seen"]}>
          {devices.map((d) => (
            <tr key={d.device_id}>
              <td style={{ fontFamily: "monospace", fontSize: 12 }}>{d.device_id}</td>
              <td>{d.display_name ?? "-"}</td>
              <td className="muted">{d.last_seen_ts ? new Date(d.last_seen_ts).toLocaleString() : "-"}</td>
            </tr>
          ))}
          {devices.length === 0 && <tr><td colSpan={3} className="muted">no devices</td></tr>}
        </Table>
      </Card>

      <div className="grid-2 mt">
        <Card title="E2EE (Olm / Megolm)">
          <p className="muted">
            Generates a real Ed25519 signing key + Curve25519 identity key via
            libolm (WASM), persists them in IndexedDB, and uploads signed device
            keys + one-time keys. Room messages are encrypted with Megolm
            (m.megolm.v1.aes-sha2); room keys are shared via Olm 1:1 to-device.
          </p>
          <div className="row gap-12">
            <Button onClick={bootstrap} disabled={bootstrapping}>
              {bootstrapping ? "Bootstrapping…" : e2eeStatus ? "Re-bootstrap keys" : "Bootstrap E2EE"}
            </Button>
            <Button variant="primary" onClick={refreshKeys}>Refresh device keys</Button>
          </div>
          {e2eeStatus && (
            <div className="mt" style={{ fontSize: 12, fontFamily: "monospace", wordBreak: "break-all" }}>
              <div>ed25519: {e2eeStatus.ed25519}</div>
              <div>curve25519: {e2eeStatus.curve25519}</div>
            </div>
          )}
          {e2eeStatus && <div className="mt"><Badge>device keys uploaded · Megolm active</Badge></div>}
        </Card>

        <Card title="QR / token login">
          <p className="muted">
            Mint a single-use login token from this device, then consume it to
            log in a second device (MSC4108 "sign in with another device"
            foundation).
          </p>
          <div className="row gap-12">
            <Button onClick={mintQR}>Mint login token</Button>
            <Button onClick={consumeQR} disabled={!qrToken}>Consume (new device)</Button>
          </div>
          {qrToken && (
            <div className="mt" style={{ fontSize: 12, fontFamily: "monospace", wordBreak: "break-all" }}>
              token: {qrToken}
            </div>
          )}
          {qrMsg && <div className="muted mt">{qrMsg}</div>}
        </Card>
      </div>
    </div>
  );
}
