package sync

import (
	"encoding/json"
	"testing"
)

func TestParseSyncFilterEventFormat(t *testing.T) {
	f := ParseSyncFilter(json.RawMessage(`{"event_format":"federation"}`))
	if f == nil || !f.EventFormatFederation {
		t.Fatal("event_format=federation must set EventFormatFederation")
	}
	if f == nil || !f.anySet() {
		t.Fatal("a federation-format filter must not collapse to nil")
	}
	if g := ParseSyncFilter(json.RawMessage(`{"event_format":"client"}`)); g != nil && g.EventFormatFederation {
		t.Fatal("event_format=client must not set EventFormatFederation")
	}
}

func TestParseSyncFilterAccountData(t *testing.T) {
	f := ParseSyncFilter(json.RawMessage(`{"account_data":{"types":["my.test.type"]}}`))
	if f == nil || !f.AccountDataTypesSet {
		t.Fatal("account_data.types must be captured")
	}
	if len(f.AccountDataTypes) != 1 || f.AccountDataTypes[0] != "my.test.type" {
		t.Fatalf("unexpected types: %v", f.AccountDataTypes)
	}
	// A present-but-empty list still restricts.
	if f.keepAccountData("m.push_rules") {
		t.Fatal("type not in the types list must be filtered out")
	}
	if !f.keepAccountData("my.test.type") {
		t.Fatal("type in the types list must pass")
	}

	n := ParseSyncFilter(json.RawMessage(`{"account_data":{"not_types":["m.push_rules"]}}`))
	if n == nil || !n.AccountDataNotTypesSet {
		t.Fatal("account_data.not_types must be captured")
	}
	if n.keepAccountData("m.push_rules") {
		t.Fatal("not_types match must be filtered out")
	}
	if !n.keepAccountData("my.test.type") {
		t.Fatal("unlisted type must pass not_types")
	}

	// Empty types list matches nothing (standard RoomEventFilter semantics).
	e := ParseSyncFilter(json.RawMessage(`{"account_data":{"types":[]}}`))
	if e == nil || !e.AccountDataTypesSet {
		t.Fatal("empty account_data.types must be a restriction")
	}
	if e.keepAccountData("anything") {
		t.Fatal("empty types list must match nothing")
	}
}

func TestParseSyncFilterEmptyAccountDataCollapsesToNil(t *testing.T) {
	if f := ParseSyncFilter(json.RawMessage(`{}`)); f != nil {
		t.Fatal("an empty filter must collapse to nil")
	}
}
