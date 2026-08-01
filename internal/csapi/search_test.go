package csapi

import (
	"fmt"
	"net/http"
	"net/http/httptest"
	"net/url"
	"testing"
)

// TestSearchContext verifies /search context windows: events_before is
// newest-first excluding the match, events_after oldest-first, honouring the
// event_context before/after limits (Complement apidoc_search_test.go).
func TestSearchContext(t *testing.T) {
	_, srv := testAPI(t)
	tok := registerUser(t, srv, "search-ctx-alice", "pw")
	roomID := createRoom(t, srv, tok, map[string]any{"preset": "public_chat"})
	for i := 1; i <= 7; i++ {
		_, _ = doJSON(t, srv, http.MethodPut, fmt.Sprintf("/_matrix/client/v3/rooms/%s/send/m.room.message/tx%d", roomID, i), tok,
			map[string]any{"msgtype": "m.text", "body": fmt.Sprintf("Message number %d", i)})
	}
	code, resp := doJSON(t, srv, http.MethodPost, "/_matrix/client/v3/search", tok,
		map[string]any{"search_categories": map[string]any{"room_events": map[string]any{
			"keys": []string{"content.body"}, "search_term": "Message 4", "order_by": "recent",
			"filter":        map[string]any{"limit": 1, "rooms": []string{roomID}},
			"event_context": map[string]any{"before_limit": 2, "after_limit": 2},
		}}})
	if code != 200 {
		t.Fatalf("search: %d %v", code, resp)
	}
	ctx := resp["search_categories"].(map[string]any)["room_events"].(map[string]any)["results"].([]any)[0].(map[string]any)["context"].(map[string]any)
	before := ctx["events_before"].([]any)
	after := ctx["events_after"].([]any)
	bodyOf := func(a any) string { return a.(map[string]any)["content"].(map[string]any)["body"].(string) }
	if len(before) != 2 || bodyOf(before[0]) != "Message number 3" || bodyOf(before[1]) != "Message number 2" {
		t.Fatalf("events_before wrong: %v", before)
	}
	if len(after) != 2 || bodyOf(after[0]) != "Message number 5" || bodyOf(after[1]) != "Message number 6" {
		t.Fatalf("events_after wrong: %v", after)
	}
}

// TestSearchBackpaginate verifies /search back-pagination: full pages carry a
// next_batch, the last page is empty with no next_batch, and count reflects all
// matches (Complement apidoc_search_test.go back-pagination subtest).
func TestSearchBackpaginate(t *testing.T) {
	_, srv := testAPI(t)
	tok := registerUser(t, srv, "search-pg-alice", "pw")
	roomID := createRoom(t, srv, tok, map[string]any{"preset": "public_chat"})
	var ids []string
	for i := 0; i <= 19; i++ {
		_, b := doJSON(t, srv, http.MethodPut, fmt.Sprintf("/_matrix/client/v3/rooms/%s/send/m.room.message/tx%d", roomID, i), tok,
			map[string]any{"msgtype": "m.text", "body": fmt.Sprintf("Message number %d", i)})
		ids = append(ids, b["event_id"].(string))
	}
	req := map[string]any{"search_categories": map[string]any{"room_events": map[string]any{
		"keys": []string{"content.body"}, "search_term": "Message", "order_by": "recent",
		"filter": map[string]any{"limit": 10, "rooms": []string{roomID}},
	}}}
	re := searchPage(t, srv, tok, req, "")
	if re["count"].(float64) != 20 {
		t.Fatalf("page1 count = %v want 20", re["count"])
	}
	if first := re["results"].([]any)[0].(map[string]any)["result"].(map[string]any)["event_id"]; first != ids[19] {
		t.Fatalf("page1 first = %v want %v", first, ids[19])
	}
	nb, _ := re["next_batch"].(string)
	if nb == "" {
		t.Fatalf("no next_batch on full page")
	}
	re = searchPage(t, srv, tok, req, nb)
	if re["count"].(float64) != 20 {
		t.Fatalf("page2 count = %v want 20", re["count"])
	}
	if first := re["results"].([]any)[0].(map[string]any)["result"].(map[string]any)["event_id"]; first != ids[9] {
		t.Fatalf("page2 first = %v want %v", first, ids[9])
	}
	nb, _ = re["next_batch"].(string)
	if nb == "" {
		t.Fatalf("no next_batch on page2")
	}
	re = searchPage(t, srv, tok, req, nb)
	if re["count"].(float64) != 20 {
		t.Fatalf("page3 count = %v want 20", re["count"])
	}
	if results, ok := re["results"].([]any); !ok || len(results) != 0 {
		t.Fatalf("page3 results should be empty: %v", re)
	}
	if _, ok := re["next_batch"]; ok {
		t.Fatalf("page3 should have no next_batch: %v", re)
	}
}

// searchPage runs a /search request with an optional next_batch query.
func searchPage(t *testing.T, srv *httptest.Server, tok string, req map[string]any, nb string) map[string]any {
	t.Helper()
	path := "/_matrix/client/v3/search"
	if nb != "" {
		path += "?" + url.Values{"next_batch": []string{nb}}.Encode()
	}
	code, resp := doJSON(t, srv, http.MethodPost, path, tok, req)
	if code != 200 {
		t.Fatalf("search: %d %v", code, resp)
	}
	return resp["search_categories"].(map[string]any)["room_events"].(map[string]any)
}

// TestSearchExcludesRedacted verifies /search never returns redacted events,
// for both rank and recent ordering.
func TestSearchExcludesRedacted(t *testing.T) {
	_, srv := testAPI(t)
	tok := registerUser(t, srv, "search-red-alice", "pw")
	roomID := createRoom(t, srv, tok, map[string]any{"preset": "public_chat"})
	_, b := doJSON(t, srv, http.MethodPut, fmt.Sprintf("/_matrix/client/v3/rooms/%s/send/m.room.message/r1", roomID), tok,
		map[string]any{"msgtype": "m.text", "body": "This message is going to be redacted"})
	redEvent := b["event_id"].(string)
	_, _ = doJSON(t, srv, http.MethodPut, fmt.Sprintf("/_matrix/client/v3/rooms/%s/send/m.room.message/r2", roomID), tok,
		map[string]any{"msgtype": "m.text", "body": "This message is not going to be redacted"})
	_, _ = doJSON(t, srv, http.MethodPut, fmt.Sprintf("/_matrix/client/v3/rooms/%s/redact/%s/red1", roomID, redEvent), tok, map[string]any{"reason": "testing"})

	for _, order := range []string{"rank", "recent"} {
		code, resp := doJSON(t, srv, http.MethodPost, "/_matrix/client/v3/search", tok,
			map[string]any{"search_categories": map[string]any{"room_events": map[string]any{
				"keys": []string{"content.body"}, "search_term": "redacted", "order_by": order,
				"filter": map[string]any{"rooms": []string{roomID}},
			}}})
		if code != 200 {
			t.Fatalf("search: %d", code)
		}
		re := resp["search_categories"].(map[string]any)["room_events"].(map[string]any)
		results, _ := re["results"].([]any)
		if re["count"].(float64) != 1 || len(results) != 1 {
			t.Fatalf("order=%s: count=%v results=%d want 1/1: %v", order, re["count"], len(results), re)
		}
		if results[0].(map[string]any)["result"].(map[string]any)["event_id"] == redEvent {
			t.Fatalf("order=%s: redacted event returned", order)
		}
	}
}

// TestRelationsPaginationEndToEnd verifies /relations pagination in both
// directions: backward pages continue strictly below the next_batch token and
// forward pages strictly above it (Complement room_relations_test.go).
func TestRelationsPaginationEndToEnd(t *testing.T) {
	_, srv := testAPI(t)
	tok := registerUser(t, srv, "rel-pg-alice", "pw")
	roomID := createRoom(t, srv, tok, map[string]any{"preset": "public_chat"})
	_, b := doJSON(t, srv, http.MethodPut, "/_matrix/client/v3/rooms/"+roomID+"/send/m.room.message/root", tok,
		map[string]any{"msgtype": "m.text", "body": "root"})
	rootID := b["event_id"].(string)
	var ids []string
	for i := 0; i < 10; i++ {
		_, r := doJSON(t, srv, http.MethodPut, fmt.Sprintf("/_matrix/client/v3/rooms/%s/send/m.room.message/r%d", roomID, i), tok,
			map[string]any{"msgtype": "m.text", "body": fmt.Sprintf("reply %d", i),
				"m.relates_to": map[string]any{"rel_type": "m.thread", "event_id": rootID}})
		ids = append(ids, r["event_id"].(string))
	}
	rel := "/_matrix/client/v1/rooms/" + roomID + "/relations/" + rootID

	// Backward page 1: newest 3.
	code, res := doJSON(t, srv, http.MethodGet, rel+"?limit=3", tok, nil)
	if code != 200 {
		t.Fatalf("rel b1: %d", code)
	}
	chunk := res["chunk"].([]any)
	if len(chunk) != 3 || chunk[0].(map[string]any)["event_id"] != ids[9] || chunk[2].(map[string]any)["event_id"] != ids[7] {
		t.Fatalf("b1 chunk wrong: %v", res)
	}
	nb, _ := res["next_batch"].(string)
	if nb == "" {
		t.Fatalf("b1 no next_batch")
	}
	// Backward page 2: next 3 below the token.
	code, res = doJSON(t, srv, http.MethodGet, rel+"?limit=3&dir=b&from="+nb, tok, nil)
	chunk = res["chunk"].([]any)
	if len(chunk) != 3 || chunk[0].(map[string]any)["event_id"] != ids[6] || chunk[2].(map[string]any)["event_id"] != ids[4] {
		t.Fatalf("b2 chunk wrong: %v", res)
	}

	// Forward page 1: oldest 3.
	code, res = doJSON(t, srv, http.MethodGet, rel+"?limit=3&dir=f", tok, nil)
	chunk = res["chunk"].([]any)
	if len(chunk) != 3 || chunk[0].(map[string]any)["event_id"] != ids[0] || chunk[2].(map[string]any)["event_id"] != ids[2] {
		t.Fatalf("f1 chunk wrong: %v", res)
	}
	nb, _ = res["next_batch"].(string)
	if nb == "" {
		t.Fatalf("f1 no next_batch")
	}
	// Forward page 2.
	code, res = doJSON(t, srv, http.MethodGet, rel+"?limit=3&dir=f&from="+nb, tok, nil)
	chunk = res["chunk"].([]any)
	if len(chunk) != 3 || chunk[0].(map[string]any)["event_id"] != ids[3] || chunk[2].(map[string]any)["event_id"] != ids[5] {
		t.Fatalf("f2 chunk wrong: %v", res)
	}
}
