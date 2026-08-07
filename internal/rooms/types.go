// Package rooms implements the room state machine: creation, membership
// transitions, power-level evaluation, and the authorization rules that gate
// whether a given event may be persisted. It is the policy layer consulted by
// the csapi room handlers; storage persistence is delegated to *storage.Store.
//
// The authorization rules follow the Matrix spec ("Authorization rules for
// events"), with per-room-version behaviour selected via internal/roomver.
package rooms

import (
	"encoding/json"
	"fmt"

	"github.com/AkagiYui/katrix/internal/roomver"
)

// Membership values for m.room.member.content.membership.
const (
	MembershipJoin   = "join"
	MembershipLeave  = "leave"
	MembershipInvite = "invite"
	MembershipBan    = "ban"
	MembershipKnock  = "knock"
)

// PowerLevels is a parsed m.room.power_levels event content.
type PowerLevels struct {
	Users         map[string]int64 `json:"users"`
	UsersDefault  int64            `json:"users_default"`
	Events        map[string]int64 `json:"events"`
	EventsDefault int64            `json:"events_default"`
	StateDefault  int64            `json:"state_default"`
	Ban           int64            `json:"ban"`
	Kick          int64            `json:"kick"`
	Redact        int64            `json:"redact"`
	Invite        int64            `json:"invite"`
	// Notifications is honoured by room versions 6+ (roomver rules flag it).
	Notifications map[string]int64 `json:"notifications,omitempty"`
}

// UserLevel returns the effective power level of a user.
func (p *PowerLevels) UserLevel(userID string) int64 {
	if l, ok := p.Users[userID]; ok {
		return l
	}
	return p.UsersDefault
}

// EventLevel returns the required level to send an event of the given type as
// state (isState) or message.
func (p *PowerLevels) EventLevel(eventType string, isState bool) int64 {
	if l, ok := p.Events[eventType]; ok {
		return l
	}
	if isState {
		return p.StateDefault
	}
	return p.EventsDefault
}

// ParsePowerLevels decodes raw m.room.power_levels content, applying the spec's
// field defaults: ban, kick, redact and state_default default to 50; invite,
// events_default and users_default default to 0. When the event is absent
// (empty raw) the caller's StateSnapshot must still apply the "no
// m.room.power_levels event" defaults — the creator has power 100, everyone
// else 0 — which the create event's privileged-creator rule covers for v12+;
// for pre-v12 rooms the caller seeds the creator's power explicitly.
func ParsePowerLevels(raw json.RawMessage) (*PowerLevels, error) {
	pl := &PowerLevels{
		StateDefault: 50,
		Ban:          50,
		Kick:         50,
		Redact:       50,
	}
	if len(raw) == 0 {
		return pl, nil
	}
	if err := json.Unmarshal(raw, pl); err != nil {
		return nil, fmt.Errorf("rooms: parse power levels: %w", err)
	}
	return pl, nil
}

// JoinRules values for m.room.join_rules.content.join_rule.
const (
	JoinRulePublic          = "public"
	JoinRuleInvite          = "invite"
	JoinRuleKnock           = "knock"
	JoinRuleRestricted      = "restricted"
	JoinRuleKnockRestricted = "knock_restricted"
)

// JoinRule extracts the join_rule from m.room.join_rules content.
func JoinRule(raw json.RawMessage) string {
	var c struct {
		JoinRule string `json:"join_rule"`
	}
	_ = json.Unmarshal(raw, &c)
	if c.JoinRule == "" {
		return JoinRuleInvite // spec default
	}
	return c.JoinRule
}

// AllowEntry is one entry in m.room.join_rules.content.allow (MSC3083). Only
// "m.room_membership" entries with a room_id count as valid; anything else is
// filtered out when evaluating a restricted join.
type AllowEntry struct {
	Type   string   `json:"type"`
	RoomID string   `json:"room_id"`
	Via    []string `json:"via,omitempty"`
}

// AllowRooms returns the valid allowed room IDs from m.room.join_rules content.
// A malformed allow key (wrong shape, e.g. a string or a list of strings)
// yields no valid entries, which means restricted joins are not authorised.
func AllowRooms(raw json.RawMessage) []string {
	var c struct {
		Allow []AllowEntry `json:"allow"`
	}
	if err := json.Unmarshal(raw, &c); err != nil {
		return nil
	}
	var out []string
	for _, e := range c.Allow {
		if e.Type == "m.room_membership" && e.RoomID != "" {
			out = append(out, e.RoomID)
		}
	}
	return out
}

// HistoryVisibility extracts the history_visibility (default "shared").
func HistoryVisibility(raw json.RawMessage) string {
	var c struct {
		HistoryVisibility string `json:"history_visibility"`
	}
	_ = json.Unmarshal(raw, &c)
	if c.HistoryVisibility == "" {
		return "shared"
	}
	return c.HistoryVisibility
}

// CreateContent is the parsed m.room.create event content.
type CreateContent struct {
	Creator     string          `json:"creator"`
	RoomVersion roomver.Version `json:"room_version"`
	Predecessor json.RawMessage `json:"predecessor,omitempty"`
	Type        string          `json:"type,omitempty"`
	MSCFederate *bool           `json:"m.federate"`
	// AdditionalCreators (MSC4289, room version 12): users granted the same
	// implicit "infinite" power as the creator. They must not appear in the
	// m.room.power_levels `users` map and cannot be kicked/baned by finite
	// power levels.
	AdditionalCreators []string `json:"additional_creators,omitempty"`
}

// ParseCreate decodes m.room.create content.
func ParseCreate(raw json.RawMessage) (*CreateContent, error) {
	var c CreateContent
	if len(raw) == 0 {
		return nil, fmt.Errorf("rooms: empty create content")
	}
	if err := json.Unmarshal(raw, &c); err != nil {
		return nil, fmt.Errorf("rooms: parse create: %w", err)
	}
	if c.RoomVersion == "" {
		c.RoomVersion = roomver.Default
	}
	return &c, nil
}

// CreatorOf returns the room creator's user ID: the content's `creator`
// property for room versions that carry it, or the m.room.create event's
// `sender` when the version omits the property (room version 11+ removed
// `creator` from create content; the creator is always the create event's
// sender — spec "Remove the creator property of m.room.create events").
// createSender must be the sender of the m.room.create event.
func (c *CreateContent) CreatorOf(createSender string) string {
	if c.Creator != "" {
		return c.Creator
	}
	return createSender
}

// IsPrivileged reports whether userID is the room creator or one of the
// additional creators (MSC4289). Such users hold effectively infinite power in
// room version 12. createSender is the m.room.create event's sender, which
// is the creator for room versions that omit the content `creator` property.
func (c *CreateContent) IsPrivileged(userID, createSender string) bool {
	if userID == "" {
		return false
	}
	if userID == c.CreatorOf(createSender) {
		return true
	}
	for _, ac := range c.AdditionalCreators {
		if ac == userID {
			return true
		}
	}
	return false
}

// MemberContent is the parsed m.room.member event content.
type MemberContent struct {
	Membership  string          `json:"membership"`
	AvatarURL   string          `json:"avatar_url,omitempty"`
	DisplayName string          `json:"displayname,omitempty"`
	Reason      string          `json:"reason,omitempty"`
	ThirdParty  json.RawMessage `json:"third_party_invite,omitempty"`
	IsDirect    *bool           `json:"is_direct,omitempty"`
	// JoinAuthorisedViaUsersServer (MSC3083): the sender of a restricted-rule
	// join, naming a joined user who authorised the join.
	JoinAuthorisedViaUsersServer string `json:"join_authorised_via_users_server,omitempty"`
}

// ParseMember decodes m.room.member content.
func ParseMember(raw json.RawMessage) (*MemberContent, error) {
	var c MemberContent
	if len(raw) == 0 {
		return nil, fmt.Errorf("rooms: empty member content")
	}
	if err := json.Unmarshal(raw, &c); err != nil {
		return nil, fmt.Errorf("rooms: parse member: %w", err)
	}
	return &c, nil
}

// CanonicalAliasContent is the parsed m.room.canonical_alias event content.
// Per the spec, `alias` and each entry of `alt_aliases` must be a valid room
// alias pointing at the room the event is being sent in; servers validate this
// before accepting the event (rejecting with M_INVALID_PARAM otherwise).
type CanonicalAliasContent struct {
	Alias      string   `json:"alias,omitempty"`
	AltAliases []string `json:"alt_aliases,omitempty"`
}

// ParseCanonicalAlias decodes m.room.canonical_alias content.
func ParseCanonicalAlias(raw json.RawMessage) (*CanonicalAliasContent, error) {
	var c CanonicalAliasContent
	if len(raw) == 0 {
		return &c, nil
	}
	if err := json.Unmarshal(raw, &c); err != nil {
		return nil, fmt.Errorf("rooms: parse canonical_alias: %w", err)
	}
	return &c, nil
}
