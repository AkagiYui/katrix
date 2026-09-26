// Stripped state (spec §Stripped state, MSC4311): the prejoin view of a room a
// user has been invited to or has knocked on. The server delivers it under
// /sync rooms.invite.<id>.invite_state and rooms.knock.<id>.knock_state as
// stripped state events — type, state_key, sender and content only.
//
// Stripped state is informational and unsigned: it is only used until the
// user joins, after which the room's real state replaces it.

/** A stripped state event: exactly the four fields a client may see. */
export interface StrippedStateEvent {
  type: string;
  state_key: string;
  sender: string;
  content: Record<string, unknown>;
}

export interface InvitedRoomSync {
  invite_state?: { events?: StrippedStateEvent[] };
}

export interface KnockedRoomSync {
  knock_state?: { events?: StrippedStateEvent[] };
}

/** What a client can say about a room before joining it. */
export interface RoomPreview {
  roomId: string;
  name: string;
  topic?: string;
  avatarUrl?: string;
  canonicalAlias?: string;
  joinRule?: string;
  encrypted: boolean;
  /** m.room.create content.type, e.g. "m.space". */
  roomType?: string;
  roomVersion?: string;
  /** The membership event of the current user (the invite or the knock). */
  membership?: StrippedStateEvent;
  /** Who invited the user (the invite event's sender). */
  inviter?: string;
  inviterDisplayName?: string;
  isDirect: boolean;
  reason?: string;
}

function stateEvent(
  events: StrippedStateEvent[],
  type: string,
  stateKey = "",
): StrippedStateEvent | undefined {
  return events.find((e) => e.type === type && e.state_key === stateKey);
}

function str(v: unknown): string | undefined {
  return typeof v === "string" && v !== "" ? v : undefined;
}

/** The server name of a Matrix user ID (everything after the first colon). */
export function serverOf(userId: string | undefined): string | undefined {
  if (!userId) return undefined;
  const i = userId.indexOf(":");
  return i >= 0 ? userId.slice(i + 1) : undefined;
}

/**
 * Summarise a room's stripped state for display. `userId` is the current
 * user, whose membership event carries the invite / knock.
 */
export function previewRoom(
  roomId: string,
  events: StrippedStateEvent[] | undefined,
  userId: string | null,
): RoomPreview {
  const evs = (events ?? []).filter(
    (e) => e && typeof e.type === "string" && typeof e.state_key === "string",
  );
  const create = stateEvent(evs, "m.room.create");
  const membership = userId ? stateEvent(evs, "m.room.member", userId) : undefined;
  const isInvite = membership?.content.membership === "invite";
  const inviter = isInvite ? membership?.sender : undefined;
  const inviterMember = inviter ? stateEvent(evs, "m.room.member", inviter) : undefined;
  const inviterDisplayName = str(inviterMember?.content.displayname);
  const canonicalAlias = str(stateEvent(evs, "m.room.canonical_alias")?.content.alias);

  const name =
    str(stateEvent(evs, "m.room.name")?.content.name) ??
    canonicalAlias ??
    // A direct-message invite is usually unnamed: show who it is from.
    (isInvite && inviter ? inviterDisplayName ?? inviter : undefined) ??
    roomId;

  return {
    roomId,
    name,
    topic: str(stateEvent(evs, "m.room.topic")?.content.topic),
    avatarUrl: str(stateEvent(evs, "m.room.avatar")?.content.url),
    canonicalAlias,
    joinRule: str(stateEvent(evs, "m.room.join_rules")?.content.join_rule),
    encrypted: !!stateEvent(evs, "m.room.encryption"),
    roomType: str(create?.content.type),
    roomVersion: str(create?.content.room_version),
    membership,
    inviter,
    inviterDisplayName,
    isDirect: membership?.content.is_direct === true,
    reason: str(membership?.content.reason),
  };
}

/**
 * Servers to try when joining a room the user was invited to. Room IDs carry
 * no server name from room version 12 (MSC4291), so the inviter's server —
 * which is necessarily in the room — is the reliable candidate.
 */
export function joinCandidates(preview: RoomPreview): string[] {
  const out: string[] = [];
  const inviterServer = serverOf(preview.inviter);
  if (inviterServer) out.push(inviterServer);
  const aliasServer = serverOf(preview.canonicalAlias);
  if (aliasServer && !out.includes(aliasServer)) out.push(aliasServer);
  return out;
}
