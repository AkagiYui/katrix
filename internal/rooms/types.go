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

// ParsePowerLevels decodes raw m.room.power_levels content, defaulting the
// whole struct to zero values when raw is empty/invalid.
func ParsePowerLevels(raw json.RawMessage) (*PowerLevels, error) {
	pl := &PowerLevels{}
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

// MemberContent is the parsed m.room.member event content.
type MemberContent struct {
	Membership  string          `json:"membership"`
	AvatarURL   string          `json:"avatar_url,omitempty"`
	DisplayName string          `json:"displayname,omitempty"`
	Reason      string          `json:"reason,omitempty"`
	ThirdParty  json.RawMessage `json:"third_party_invite,omitempty"`
	IsDirect    *bool           `json:"is_direct,omitempty"`
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
