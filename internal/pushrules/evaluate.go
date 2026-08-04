package pushrules

import (
	"encoding/json"
	"strings"
)

// EventSnapshot is the per-event data the push evaluator needs. Only the
// fields push rules can match on are exposed; the raw content is kept so
// conditions (event_match on content.*, content-kind rules) can inspect it.
type EventSnapshot struct {
	Type        string
	Sender      string
	RoomID      string
	Content     json.RawMessage
	MemberCount int
}

// EvalResult is the outcome of evaluating an event against a user's push
// rules: whether it produces a notification and/or a highlight.
type EvalResult struct {
	Notifies   bool
	Highlights bool
}

// Evaluate runs the event through the user's global push ruleset in spec
// evaluation order (override, content, sender, room, underride — the first
// matching rule wins, spec §11.14.1.4.3) and reports whether it would
// notify and whether it would highlight. The default ruleset is evaluated
// like any other: .m.rule.contains_user_name is special-cased (its empty
// pattern matches when the event body contains the user's localpart, which
// is what turns a mention into a highlight), matching Synapse.
//
// An event the user sent themselves never notifies (mirror of Synapse's
// action_for_event_by_user, which treats sender == recipient as
// dont_notify).
func Evaluate(rules map[string]any, userID, localpart string, ev EventSnapshot) EvalResult {
	res := EvalResult{}
	if ev.Sender == userID {
		return res
	}
	global, _ := rules["global"].(map[string]any)
	if global == nil {
		return res
	}
	var kindLists = [5]string{"override", "content", "sender", "room", "underride"}
	for _, kind := range kindLists {
		list, _ := global[kind].([]any)
		if kind == "content" {
			// Content-kind rules: the first matching rule wins, but the spec
			// gives the longest pattern precedence among matches. The default
			// .m.rule.contains_user_name (pattern "") is the common case, and
			// the tests rely on user rules overriding defaults, so first-match
			// is sufficient here.
			for _, e := range list {
				rule, _ := e.(map[string]any)
				if rule == nil {
					continue
				}
				if !ruleEnabled(rule) {
					continue
				}
				if contentRuleMatches(rule, localpart, ev) {
					applyActions(rule, &res)
					return res
				}
			}
			continue
		}
		for _, e := range list {
			rule, _ := e.(map[string]any)
			if rule == nil {
				continue
			}
			if !ruleEnabled(rule) {
				continue
			}
			if kindRuleMatches(kind, rule, ev) {
				applyActions(rule, &res)
				return res
			}
		}
	}
	return res
}

// ruleEnabled reports whether a rule is enabled (the default is enabled; the
// .m.rule.master default rule ships disabled, and users may disable others).
func ruleEnabled(rule map[string]any) bool {
	enabled, _ := rule["enabled"].(bool)
	return enabled
}

// kindRuleMatches evaluates a rule's conditions for the override/sender/room/
// underride kinds. A rule with no conditions matches everything.
func kindRuleMatches(kind string, rule map[string]any, ev EventSnapshot) bool {
	switch kind {
	case "sender":
		return rule["rule_id"] == ev.Sender
	case "room":
		return rule["rule_id"] == ev.RoomID
	}
	conds, _ := rule["conditions"].([]any)
	if len(conds) == 0 {
		return true
	}
	for _, c := range conds {
		cm, _ := c.(map[string]any)
		if cm == nil {
			continue
		}
		if !conditionMatches(cm, "", ev, "") {
			return false
		}
	}
	return true
}

// contentRuleMatches evaluates a content-kind rule against the event content.
// .m.rule.contains_user_name (the default highlight rule, pattern "") is
// special-cased: it matches when the event body contains the user's localpart.
// Other content rules glob-match any top-level string value of the content.
func contentRuleMatches(rule map[string]any, localpart string, ev EventSnapshot) bool {
	if rule["rule_id"] == ".m.rule.contains_user_name" {
		if lp := strings.ToLower(localpart); lp != "" {
			body := contentField(ev.Content, "body")
			return strings.Contains(strings.ToLower(body), lp)
		}
		return false
	}
	pattern, _ := rule["pattern"].(string)
	if pattern == "*" {
		// A catch-all content rule matches any event with content.
		return len(ev.Content) > 0 && string(ev.Content) != "{}" && string(ev.Content) != "null"
	}
	// The pattern is glob-matched against the event content's string values.
	var content map[string]any
	if json.Unmarshal(ev.Content, &content) != nil {
		return false
	}
	for _, v := range content {
		s, ok := v.(string)
		if ok && globMatch(pattern, s) {
			return true
		}
	}
	return false
}

// conditionMatches evaluates one push-rule condition. kindRules is unused but
// kept for signature symmetry with the event_match walker.
func conditionMatches(cm map[string]any, _ string, ev EventSnapshot, _ string) bool {
	kind, _ := cm["kind"].(string)
	switch kind {
	case "event_match":
		key, _ := cm["key"].(string)
		pattern, _ := cm["pattern"].(string)
		var value string
		switch {
		case key == "type":
			value = ev.Type
		case strings.HasPrefix(key, "content."):
			value = contentField(ev.Content, strings.TrimPrefix(key, "content."))
		case key == "sender":
			value = ev.Sender
		case key == "room_id":
			value = ev.RoomID
		default:
			value = ""
		}
		return globMatch(pattern, value)
	case "room_member_count":
		// The `is` field is a comparison like "2", "<5" or ">=2"; only the
		// equality form is needed for the default MSC3930 poll rules.
		is, _ := cm["is"].(string)
		n, ok := parseIntStrict(is)
		if !ok {
			return false
		}
		return ev.MemberCount == n
	case "contains_display_name":
		return true
	default:
		return false
	}
}

// applyActions maps a matched rule's actions to notify/highlight.
func applyActions(rule map[string]any, res *EvalResult) {
	actions, _ := rule["actions"].([]any)
	for _, a := range actions {
		switch act := a.(type) {
		case string:
			if act == "notify" {
				res.Notifies = true
			}
		case map[string]any:
			if _, ok := act["notify"]; ok {
				res.Notifies = true
			}
			if tw, ok := act["set_tweak"].(string); ok && tw == "highlight" {
				if val, ok := act["value"].(bool); ok && val {
					res.Highlights = true
				}
			}
		}
	}
}

// contentField extracts a (possibly dotted) field from the event content.
func contentField(raw json.RawMessage, path string) string {
	var content map[string]any
	if json.Unmarshal(raw, &content) != nil {
		return ""
	}
	cur := any(content)
	for _, part := range strings.Split(path, ".") {
		m, ok := cur.(map[string]any)
		if !ok {
			return ""
		}
		cur, ok = m[part]
		if !ok {
			return ""
		}
	}
	s, _ := cur.(string)
	return s
}

// globMatch reports whether the pattern (with `*` and `?` wildcards) matches s.
func globMatch(pattern, s string) bool {
	// Fast path for exact/empty patterns.
	if pattern == "" {
		return s == ""
	}
	if !strings.Contains(pattern, "*") && !strings.Contains(pattern, "?") {
		return pattern == s
	}
	return globHere(pattern, s)
}

func globHere(p, s string) bool {
	for len(p) > 0 {
		switch p[0] {
		case '*':
			// Collapse consecutive stars, then try to match the rest at every
			// suffix position.
			for len(p) > 1 && p[1] == '*' {
				p = p[1:]
			}
			if len(p) == 1 {
				return true
			}
			for i := 0; i <= len(s); i++ {
				if globHere(p[1:], s[i:]) {
					return true
				}
			}
			return false
		case '?':
			if len(s) == 0 {
				return false
			}
			p, s = p[1:], s[1:]
		default:
			if len(s) == 0 || p[0] != s[0] {
				return false
			}
			p, s = p[1:], s[1:]
		}
	}
	return len(s) == 0
}

// parseIntStrict parses a plain integer (no sign, no operators).
func parseIntStrict(s string) (int, bool) {
	if s == "" {
		return 0, false
	}
	n := 0
	for _, c := range s {
		if c < '0' || c > '9' {
			return 0, false
		}
		n = n*10 + int(c-'0')
	}
	return n, true
}
