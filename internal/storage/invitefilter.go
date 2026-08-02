package storage

import (
	"context"
	"encoding/json"
	"path"
	"strings"
)

// ---- MSC4155 invite filtering ----
//
// The invitee's global account data `org.matrix.msc4155.invite_permission_config`
// (stable name m.invite_permission_config) decides whether an incoming invite is
// allowed, ignored (accepted but hidden from /sync), or blocked (rejected with
// M_INVITE_BLOCKED). Evaluation order per the MSC: allowed_users, ignored_users,
// blocked_users, then allowed_servers, ignored_servers, blocked_servers — within
// each level allowed is checked before ignored before blocked. Missing fields are
// empty arrays; `enabled: false` allows everything.

// InviteFilterVerdict is the outcome of evaluating an invite against the
// invitee's permission config.
type InviteFilterVerdict int

const (
	// InviteFilterAllow delivers the invite normally.
	InviteFilterAllow InviteFilterVerdict = iota
	// InviteFilterIgnore accepts the invite (the inviter gets no feedback) but
	// hides it from /sync and push.
	InviteFilterIgnore
	// InviteFilterBlock rejects the invite with M_INVITE_BLOCKED (403).
	InviteFilterBlock
)

// inviteFilterConfig is the parsed m.invite_permission_config account data.
type inviteFilterConfig struct {
	Enabled        *bool    `json:"enabled"`
	AllowedUsers   []string `json:"allowed_users"`
	IgnoredUsers   []string `json:"ignored_users"`
	BlockedUsers   []string `json:"blocked_users"`
	AllowedServers []string `json:"allowed_servers"`
	IgnoredServers []string `json:"ignored_servers"`
	BlockedServers []string `json:"blocked_servers"`
}

// EvaluateInviteFilter applies the invitee's MSC4155 permission config to an
// incoming invite from senderUserID (on senderServer) and returns the verdict.
// A missing config yields InviteFilterAllow.
func (s *Store) EvaluateInviteFilter(ctx context.Context, inviteeLocalpart, senderUserID, senderServer string) (InviteFilterVerdict, error) {
	cfg, err := s.inviteFilterConfig(ctx, inviteeLocalpart)
	if err != nil || cfg == nil {
		return InviteFilterAllow, err
	}
	if cfg.Enabled != nil && !*cfg.Enabled {
		return InviteFilterAllow, nil
	}
	// Server names drop any port suffix before glob matching (like server ACLs).
	server := senderServer
	if c := strings.LastIndex(server, ":"); c > 0 && c < len(server)-1 {
		host := server[:c]
		if strings.IndexByte(host, ':') < 0 && strings.IndexByte(host, '.') < 0 && strings.IndexByte(host, '-') < 0 {
			server = host
		}
	}
	match := func(patterns []string, value string) bool {
		for _, p := range patterns {
			if p == "" {
				continue
			}
			if ok, _ := path.Match(p, value); ok {
				return true
			}
		}
		return false
	}
	switch {
	case match(cfg.AllowedUsers, senderUserID):
		return InviteFilterAllow, nil
	case match(cfg.IgnoredUsers, senderUserID):
		return InviteFilterIgnore, nil
	case match(cfg.BlockedUsers, senderUserID):
		return InviteFilterBlock, nil
	case match(cfg.AllowedServers, server):
		return InviteFilterAllow, nil
	case match(cfg.IgnoredServers, server):
		return InviteFilterIgnore, nil
	case match(cfg.BlockedServers, server):
		return InviteFilterBlock, nil
	}
	return InviteFilterAllow, nil
}

// inviteFilterConfig loads and parses the invitee's permission config, checking
// both the unstable org.matrix.msc4155 key (used by Complement) and the stable
// m.invite_permission_config key. A missing or malformed config yields nil.
func (s *Store) inviteFilterConfig(ctx context.Context, localpart string) (*inviteFilterConfig, error) {
	for _, key := range []string{"org.matrix.msc4155.invite_permission_config", "m.invite_permission_config"} {
		raw, err := s.GetAccountData(ctx, localpart, "", key)
		if err != nil || len(raw) == 0 {
			continue
		}
		var cfg inviteFilterConfig
		if err := json.Unmarshal(raw, &cfg); err != nil {
			continue
		}
		return &cfg, nil
	}
	return nil, nil
}

// InviteIsHiddenFromSync reports whether an invite sent by senderUserID (on
// senderServer) to inviteeLocalpart must be hidden from /sync per the MSC4155
// ignored rules (and the classic m.ignored_user_list).
func (s *Store) InviteIsHiddenFromSync(ctx context.Context, inviteeLocalpart, senderUserID, senderServer string) (bool, error) {
	// m.ignored_user_list (existing behaviour).
	if raw, err := s.GetAccountData(ctx, inviteeLocalpart, "", "m.ignored_user_list"); err == nil && len(raw) > 0 {
		var data struct {
			IgnoredUsers map[string]json.RawMessage `json:"ignored_users"`
		}
		if json.Unmarshal(raw, &data) == nil {
			if _, ok := data.IgnoredUsers[senderUserID]; ok {
				return true, nil
			}
		}
	}
	// MSC4155 ignore verdicts.
	verdict, err := s.EvaluateInviteFilter(ctx, inviteeLocalpart, senderUserID, senderServer)
	return verdict == InviteFilterIgnore, err
}
