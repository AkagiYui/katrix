// Minimal Matrix Client-Server API client. Stores the access token in
// localStorage and wraps the endpoints Katrix implements.
export const TOKEN_KEY = "katrix.access_token";
export const USER_KEY = "katrix.user_id";

export function getToken(): string | null {
  return localStorage.getItem(TOKEN_KEY);
}

export function setToken(token: string | null, userId?: string) {
  if (token) {
    localStorage.setItem(TOKEN_KEY, token);
    if (userId) localStorage.setItem(USER_KEY, userId);
  } else {
    localStorage.removeItem(TOKEN_KEY);
    localStorage.removeItem(USER_KEY);
  }
}

export function getUserId(): string | null {
  return localStorage.getItem(USER_KEY);
}

interface MatrixError extends Error {
  errcode?: string;
}

async function request<T>(
  method: string,
  path: string,
  body?: unknown,
  token?: string,
): Promise<T> {
  const headers: Record<string, string> = {};
  if (body !== undefined) headers["Content-Type"] = "application/json";
  const tok = token ?? getToken();
  if (tok) headers["Authorization"] = `Bearer ${tok}`;
  const resp = await fetch(path, {
    method,
    headers,
    body: body !== undefined ? JSON.stringify(body) : undefined,
  });
  const text = await resp.text();
  let data: unknown = undefined;
  if (text) {
    try {
      data = JSON.parse(text);
    } catch {
      data = text;
    }
  }
  if (!resp.ok) {
    const err = new Error(
      (data && typeof data === "object" && "error" in data
        ? String((data as { error: string }).error)
        : `HTTP ${resp.status}`),
    ) as MatrixError;
    if (data && typeof data === "object" && "errcode" in data) {
      err.errcode = String((data as { errcode: string }).errcode);
    }
    throw err;
  }
  return data as T;
}

export interface LoginResponse {
  access_token: string;
  user_id: string;
  device_id: string;
  home_server?: string;
  well_known?: { "m.homeserver"?: { base_url?: string } };
}

// Register drives the m.login.dummy UIA flow automatically.
export async function register(username: string, password: string): Promise<LoginResponse> {
  // Step 1: trigger UIA challenge.
  const challenge = await request<{ session: string }>(
    "POST",
    "/_matrix/client/v3/register",
    { username, password },
  );
  // Step 2: complete m.login.dummy.
  return request<LoginResponse>("POST", "/_matrix/client/v3/register", {
    username,
    password,
    auth: { type: "m.login.dummy", session: challenge.session },
  });
}

export async function login(username: string, password: string): Promise<LoginResponse> {
  return request<LoginResponse>("POST", "/_matrix/client/v3/login", {
    type: "m.login.password",
    user: username,
    password,
  });
}

export async function whoami(): Promise<{ user_id: string; device_id: string; is_guest: boolean }> {
  return request("GET", "/_matrix/client/v3/account/whoami");
}

export async function logout(): Promise<void> {
  await request("POST", "/_matrix/client/v3/logout", {});
}

export async function createRoom(opts: {
  name?: string;
  preset?: string;
  invite?: string[];
}): Promise<{ room_id: string }> {
  return request("POST", "/_matrix/client/v3/createRoom", opts);
}

export async function joinRoom(roomIdOrAlias: string): Promise<{ room_id: string }> {
  return request("POST", `/_matrix/client/v3/join/${encodeURIComponent(roomIdOrAlias)}`, {});
}

export async function sendMessage(
  roomId: string,
  txnId: string,
  body: unknown,
): Promise<{ event_id: string }> {
  return request(
    "PUT",
    `/_matrix/client/v3/rooms/${roomId}/send/m.room.message/${txnId}`,
    body,
  );
}

/** Send a m.room.encrypted event (for E2EE rooms). */
export async function sendEncryptedMessage(
  roomId: string,
  txnId: string,
  encrypted: unknown,
): Promise<{ event_id: string }> {
  return request(
    "PUT",
    `/_matrix/client/v3/rooms/${roomId}/send/m.room.encrypted/${txnId}`,
    encrypted,
  );
}

export async function getRoomState(roomId: string): Promise<unknown[]> {
  return request("GET", `/_matrix/client/v3/rooms/${roomId}/state`);
}

export async function getMembers(
  roomId: string,
): Promise<{ chunk: Record<string, { membership: string; displayname?: string }> }> {
  return request("GET", `/_matrix/client/v3/rooms/${roomId}/members`);
}

// ---- Admin API ----

export interface AdminUser {
  name: string;
  admin: boolean;
  deactivated: boolean;
  is_guest: boolean;
}
export interface AdminRoom {
  room_id: string;
  version: string;
  creator: string;
  is_public: boolean;
}
export interface AdminStats {
  users: number;
  rooms: number;
  events: number;
}

export async function adminListUsers(): Promise<{ users: AdminUser[] }> {
  return request("GET", "/_matrix/client/v3/admin/users");
}
export async function adminListRooms(): Promise<{ rooms: AdminRoom[]; total_rooms: number }> {
  return request("GET", "/_matrix/client/v3/admin/rooms");
}
export async function adminStatistics(): Promise<AdminStats> {
  return request("GET", "/_matrix/client/v3/admin/statistics");
}
export async function adminDeactivate(userId: string): Promise<void> {
  await request("POST", `/_matrix/client/v3/admin/users/${encodeURIComponent(userId)}/deactivate`, {});
}
export async function adminSetPassword(userId: string, newPassword: string): Promise<void> {
  await request("POST", `/_matrix/client/v3/admin/user/${encodeURIComponent(userId)}/password`, {
    new_password: newPassword,
  });
}

// ---- Devices ----

export interface Device {
  device_id: string;
  display_name?: string;
  last_seen_ip?: string;
  last_seen_ts?: number;
}
export async function listDevices(): Promise<{ devices: Device[] }> {
  return request("GET", "/_matrix/client/v3/devices");
}

// ---- E2EE client ----
// The full Olm/Megolm implementation lives in ./e2ee.ts (using @matrix-org/olm).
// This file exposes the raw key-upload/query endpoints the E2EE module uses.

export async function uploadDeviceKeys(
  deviceKeys: unknown,
  oneTimeKeys?: unknown,
): Promise<{ one_time_key_counts: Record<string, number> }> {
  const body: Record<string, unknown> = { device_keys: deviceKeys };
  if (oneTimeKeys !== undefined) body.one_time_keys = oneTimeKeys;
  return request("POST", "/_matrix/client/v3/keys/upload", body);
}
export async function queryKeys(
  deviceKeys: Record<string, string[]>,
): Promise<{ device_keys: Record<string, Record<string, unknown>> }> {
  return request("POST", "/_matrix/client/v3/keys/query", { device_keys: deviceKeys });
}

// ---- QR / token login ----

export interface LoginTokenResponse {
  token: string;
  expires_in: number;
  user_id: string;
  home_server: string;
}
export async function mintLoginToken(): Promise<LoginTokenResponse> {
  return request("POST", "/_matrix/client/v3/login/token", {});
}
export async function loginWithToken(token: string, deviceId?: string): Promise<LoginResponse> {
  return request("POST", "/_matrix/client/v3/login", { type: "m.login.token", token, device_id: deviceId });
}

