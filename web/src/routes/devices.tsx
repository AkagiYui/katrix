import { useEffect, useState } from "react";
import {
  getToken,
  getUserId,
  listDevices,
  bootstrapDeviceKeys,
  uploadDeviceKeys,
  queryKeys,
  mintLoginToken,
  loginWithToken,
  type Device,
  type DeviceKeyBundle,
} from "../lib/matrix";
import { Card, Badge, Button, Input, Table } from "../components/ui/primitives";

export function DevicesPage() {
  const [devices, setDevices] = useState<Device[]>([]);
  const [bundle, setBundle] = useState<DeviceKeyBundle | null>(null);
  const [uploaded, setUploaded] = useState(false);
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
    try {
      const userId = getUserId() ?? "";
      // whoami to resolve the current device id.
      const resp = await fetch("/_matrix/client/v3/account/whoami", {
        headers: { Authorization: `Bearer ${getToken()}` },
      });
      const who = await resp.json();
      const b = await bootstrapDeviceKeys(userId, who.device_id);
      setBundle(b);
      setUploaded(false);
    } catch (e) {
      setError(e instanceof Error ? e.message : "bootstrap failed");
    }
  };

  const upload = async () => {
    if (!bundle) return;
    setError("");
    try {
      await uploadDeviceKeys(bundle);
      setUploaded(true);
    } catch (e) {
      setError(e instanceof Error ? e.message : "upload failed");
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
        <Card title="E2EE device-key bootstrap">
          <p className="muted">
            Generates an Ed25519 fingerprint key + Curve25519 identity key and
            uploads them so the session is E2EE-ready at the protocol level.
            Full Olm/Megolm message encryption requires a libolm WASM build
            (not yet integrated).
          </p>
          <div className="row gap-12">
            <Button onClick={bootstrap}>Generate keys</Button>
            <Button variant="primary" onClick={upload} disabled={!bundle || uploaded}>
              {uploaded ? "Uploaded" : "Upload keys"}
            </Button>
          </div>
          {bundle && (
            <div className="mt" style={{ fontSize: 12, fontFamily: "monospace", wordBreak: "break-all" }}>
              <div>ed25519: {bundle.keys["ed25519:" + bundle.device_id]}</div>
              <div>curve25519: {bundle.keys["curve25519:" + bundle.device_id]}</div>
            </div>
          )}
          {uploaded && <div className="mt"><Badge>device keys uploaded</Badge></div>}
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
