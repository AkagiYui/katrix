package httpx

import (
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

func TestErrUnknownTokenWritesSoftLogout(t *testing.T) {
	for _, sl := range []bool{true, false} {
		w := httptest.NewRecorder()
		WriteError(w, ErrUnknownToken(sl))
		var body map[string]json.RawMessage
		if err := json.Unmarshal(w.Body.Bytes(), &body); err != nil {
			t.Fatal(err)
		}
		got, ok := body["soft_logout"]
		if !ok {
			t.Fatalf("soft_logout missing for sl=%v", sl)
		}
		var b bool
		if err := json.Unmarshal(got, &b); err != nil {
			t.Fatal(err)
		}
		if b != sl {
			t.Fatalf("soft_logout=%v, want %v", b, sl)
		}
		if w.Code != http.StatusUnauthorized {
			t.Fatalf("status=%d, want 401", w.Code)
		}
		if string(body["errcode"]) != `"M_UNKNOWN_TOKEN"` {
			t.Fatalf("errcode=%s, want M_UNKNOWN_TOKEN", body["errcode"])
		}
	}
}

func TestWriteErrorCoercesNonMatrix(t *testing.T) {
	w := httptest.NewRecorder()
	WriteError(w, errors.New("some internal detail: connection refused"))
	if w.Code != http.StatusInternalServerError {
		t.Fatalf("status=%d, want 500", w.Code)
	}
	var body map[string]string
	_ = json.Unmarshal(w.Body.Bytes(), &body)
	if body["errcode"] != "M_UNKNOWN" {
		t.Fatalf("errcode=%s, want M_UNKNOWN", body["errcode"])
	}
}

func TestWriteJSONSetsHeaders(t *testing.T) {
	w := httptest.NewRecorder()
	WriteJSON(w, http.StatusOK, map[string]string{"hi": "there"})
	if ct := w.Header().Get("Content-Type"); ct != "application/json" {
		t.Fatalf("content-type=%q", ct)
	}
	if w.Header().Get("Access-Control-Allow-Origin") != "*" {
		t.Fatal("missing CORS header")
	}
	if w.Code != http.StatusOK {
		t.Fatalf("status=%d", w.Code)
	}
	var body map[string]string
	_ = json.Unmarshal(w.Body.Bytes(), &body)
	if body["hi"] != "there" {
		t.Fatalf("body=%v", body)
	}
}

func TestDecodeJSONEmptyBody(t *testing.T) {
	r := httptest.NewRequest(http.MethodPost, "/", nil)
	var v map[string]any
	if err := DecodeJSON(httptest.NewRecorder(), r, &v); err != nil {
		t.Fatalf("empty body should be ok, got %v", err)
	}
}

func TestDecodeJSONTooLarge(t *testing.T) {
	big := make([]byte, 2<<20) // 2 MB, over the 1 MB limit
	r := httptest.NewRequest(http.MethodPost, "/", strings.NewReader(string(big)))
	err := DecodeJSON(httptest.NewRecorder(), r, &struct{}{})
	if err == nil {
		t.Fatal("expected too-large error")
	}
	var me *MatrixError
	if !errors.As(err, &me) || me.Code != "M_TOO_LARGE" {
		t.Fatalf("err=%v, want M_TOO_LARGE", err)
	}
}

func TestDecodeJSONBadJSON(t *testing.T) {
	r := httptest.NewRequest(http.MethodPost, "/", strings.NewReader("{not json"))
	err := DecodeJSON(httptest.NewRecorder(), r, &struct{}{})
	var me *MatrixError
	if !errors.As(err, &me) || me.Code != "M_BAD_JSON" {
		t.Fatalf("err=%v, want M_BAD_JSON", err)
	}
}
