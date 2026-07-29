package csapi

import (
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/AkagiYui/katrix/internal/httpx"
)

func TestValidateEventJSON(t *testing.T) {
	cases := []struct {
		name    string
		body    string
		wantErr bool
	}{
		{"empty object", `{}`, false},
		{"string", `"hello"`, false},
		{"integer", `42`, false},
		{"fractional", `1.5`, true}, // canonical JSON forbids non-integers
		{"array", `[1,2,3]`, false},
		{"nested", `{"a":{"b":[1,2,{"c":true}]}}`, false},
		{"nested fractional", `{"a":{"b":[1,2.5,{"c":true}]}}`, true},
		{"NaN", `{"v":NaN}`, true},
		{"Infinity", `{"v":Infinity}`, true},
		{"-Infinity", `{"v":-Infinity}`, true},
		{"trailing data", `{}garbage`, true},
		{"malformed", `{not json`, true},
		{"number too large", `{"v":1e400}`, true},          // +Inf
		{"int at safe limit", `9007199254740991`, false},   // 2^53-1
		{"int over safe limit", `9007199254740993`, true},  // 2^53+1
		{"huge int", `1e30`, true},                         // finite but > 2^53
		{"negative safe int", `-9007199254740991`, false},  // -(2^53-1)
		{"negative over limit", `-9007199254740993`, true}, // -(2^53+1)
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			err := validateEventJSON([]byte(c.body))
			if c.wantErr && err == nil {
				t.Fatalf("expected error, got nil")
			}
			if !c.wantErr && err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
		})
	}
}

func TestReadEventContentSizeLimit(t *testing.T) {
	// A body larger than maxEventBytes must yield M_TOO_LARGE (413).
	big := `{"v":"` + strings.Repeat("x", maxEventBytes) + `"}` // > 65536
	req := httptest.NewRequest(http.MethodPut, "/x", strings.NewReader(big))
	_, err := readEventContent(req)
	if err == nil {
		t.Fatal("expected too-large error, got nil")
	}
	var me *httpx.MatrixError
	if !errors.As(err, &me) {
		t.Fatalf("error is not a *MatrixError: %T", err)
	}
	if me.Status() != http.StatusRequestEntityTooLarge {
		t.Fatalf("expected 413, got %d", me.Status())
	}
}

func TestReadEventContentValidJSON(t *testing.T) {
	req := httptest.NewRequest(http.MethodPut, "/x", strings.NewReader(`{"body":"hi","msgtype":"m.text"}`))
	raw, err := readEventContent(req)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	var m map[string]any
	if err := json.Unmarshal(raw, &m); err != nil {
		t.Fatalf("returned bytes not valid JSON: %v", err)
	}
	if m["body"] != "hi" {
		t.Fatalf("unexpected body: %v", m["body"])
	}
}
