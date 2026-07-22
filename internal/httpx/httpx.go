// Package httpx contains shared HTTP helpers: Matrix standard error encoding,
// JSON response writing and request body decoding with limits.
package httpx

import (
	"encoding/json"
	"errors"
	"io"
	"net/http"
)

// MatrixError is the standard Matrix error body ({errcode, error, ...}).
type MatrixError struct {
	Code       string `json:"errcode"`
	Message    string `json:"error"`
	status     int
	RetryAfter int64 `json:"retry_after_ms,omitempty"`
}

func (e *MatrixError) Error() string { return e.Code + ": " + e.Message }

// Status returns the HTTP status code associated with the error.
func (e *MatrixError) Status() int {
	if e.status == 0 {
		return http.StatusBadRequest
	}
	return e.status
}

// NewError builds a MatrixError with an explicit status.
func NewError(status int, code, msg string) *MatrixError {
	return &MatrixError{Code: code, Message: msg, status: status}
}

// Common Matrix errors. These match the spec's M_ error codes.
func ErrUnknown(msg string) *MatrixError {
	return NewError(http.StatusInternalServerError, "M_UNKNOWN", msg)
}
func ErrForbidden(msg string) *MatrixError { return NewError(http.StatusForbidden, "M_FORBIDDEN", msg) }
func ErrUnknownToken(softLogout bool) *MatrixError {
	e := NewError(http.StatusUnauthorized, "M_UNKNOWN_TOKEN", "Invalid access token")
	return e
}
func ErrMissingToken() *MatrixError {
	return NewError(http.StatusUnauthorized, "M_MISSING_TOKEN", "Missing access token")
}
func ErrBadJSON(msg string) *MatrixError { return NewError(http.StatusBadRequest, "M_BAD_JSON", msg) }
func ErrNotJSON() *MatrixError {
	return NewError(http.StatusBadRequest, "M_NOT_JSON", "Content not JSON")
}
func ErrNotFound(msg string) *MatrixError { return NewError(http.StatusNotFound, "M_NOT_FOUND", msg) }
func ErrInvalidParam(msg string) *MatrixError {
	return NewError(http.StatusBadRequest, "M_INVALID_PARAM", msg)
}
func ErrMissingParam(msg string) *MatrixError {
	return NewError(http.StatusBadRequest, "M_MISSING_PARAM", msg)
}
func ErrUserInUse() *MatrixError {
	return NewError(http.StatusBadRequest, "M_USER_IN_USE", "Username already taken")
}
func ErrInvalidUsername(msg string) *MatrixError {
	return NewError(http.StatusBadRequest, "M_INVALID_USERNAME", msg)
}
func ErrUnknownRoomVersion(msg string) *MatrixError {
	return NewError(http.StatusBadRequest, "M_UNSUPPORTED_ROOM_VERSION", msg)
}
func ErrLimitExceeded(retryMs int64) *MatrixError {
	return &MatrixError{Code: "M_LIMIT_EXCEEDED", Message: "Too many requests", status: http.StatusTooManyRequests, RetryAfter: retryMs}
}
func ErrTooLarge(msg string) *MatrixError {
	return NewError(http.StatusRequestEntityTooLarge, "M_TOO_LARGE", msg)
}
func ErrThreepidDenied() *MatrixError {
	return NewError(http.StatusForbidden, "M_THREEPID_DENIED", "Third party identifier not allowed")
}

// WriteJSON writes v as an HTTP JSON response with the given status.
func WriteJSON(w http.ResponseWriter, status int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.Header().Set("Access-Control-Allow-Origin", "*")
	body, err := json.Marshal(v)
	if err != nil {
		w.WriteHeader(http.StatusInternalServerError)
		_, _ = w.Write([]byte(`{"errcode":"M_UNKNOWN","error":"failed to encode response"}`))
		return
	}
	w.WriteHeader(status)
	_, _ = w.Write(body)
}

// WriteError writes an error, coercing non-Matrix errors to M_UNKNOWN.
func WriteError(w http.ResponseWriter, err error) {
	var me *MatrixError
	if errors.As(err, &me) {
		WriteJSON(w, me.Status(), me)
		return
	}
	WriteJSON(w, http.StatusInternalServerError, ErrUnknown(err.Error()))
}

// DecodeJSON reads and decodes a JSON request body with a size limit. An empty
// body decodes to the zero value (Matrix treats missing body as {}).
func DecodeJSON(w http.ResponseWriter, r *http.Request, v any) error {
	limited := http.MaxBytesReader(w, r.Body, 1<<20)
	data, err := io.ReadAll(limited)
	if err != nil {
		return ErrTooLarge("request body too large")
	}
	if len(data) == 0 {
		return nil
	}
	if err := json.Unmarshal(data, v); err != nil {
		return ErrBadJSON("could not decode JSON: " + err.Error())
	}
	return nil
}

// EmptyJSON is the canonical empty success body {}.
var EmptyJSON = struct{}{}
