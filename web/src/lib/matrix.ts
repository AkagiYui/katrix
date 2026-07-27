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
  body: string,
): Promise<{ event_id: string }> {
  return request(
    "PUT",
    `/_matrix/client/v3/rooms/${roomId}/send/m.room.message/${txnId}`,
    { body, msgtype: "m.text" },
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
