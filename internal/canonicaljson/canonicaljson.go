// Package canonicaljson implements Matrix Canonical JSON.
//
// Canonical JSON is the byte-exact serialization Matrix uses as the input to
// all signing and hashing. The rules (see the Matrix appendices) are:
//
//   - Objects have their keys sorted lexicographically by Unicode code point.
//   - No insignificant whitespace.
//   - Integers are rendered without a decimal point or exponent and must be in
//     the inclusive range [-(2^53-1), 2^53-1].
//   - Floats are not permitted in canonical JSON (Matrix events never contain
//     them; we reject rather than silently corrupt).
//   - Unicode is rendered as UTF-8, with only the characters JSON requires to
//     be escaped being escaped, using the shortest form.
package canonicaljson

import (
	"bytes"
	"encoding/json"
	"fmt"
	"math"
	"sort"
	"strconv"
	"unicode/utf8"
)

// Marshal returns the canonical JSON encoding of v. v is first marshalled with
// encoding/json, then re-serialized canonically, so any Go value that round
// trips through encoding/json is accepted.
func Marshal(v any) ([]byte, error) {
	raw, err := json.Marshal(v)
	if err != nil {
		return nil, err
	}
	return Canonical(raw)
}

// Canonical converts an already-valid JSON document into canonical form.
func Canonical(input []byte) ([]byte, error) {
	dec := json.NewDecoder(bytes.NewReader(input))
	dec.UseNumber()
	var v any
	if err := dec.Decode(&v); err != nil {
		return nil, err
	}
	if dec.More() {
		return nil, fmt.Errorf("canonicaljson: trailing data after JSON value")
	}
	var buf bytes.Buffer
	if err := encode(&buf, v); err != nil {
		return nil, err
	}
	return buf.Bytes(), nil
}

// Equal reports whether two JSON documents are canonically equal, i.e. their
// canonical forms are byte-identical (so key order and whitespace do not
// matter, but 1 and 1.0 are distinct).
func Equal(a, b []byte) bool {
	ca, err := Canonical(a)
	if err != nil {
		return false
	}
	cb, err := Canonical(b)
	if err != nil {
		return false
	}
	return bytes.Equal(ca, cb)
}

func encode(buf *bytes.Buffer, v any) error {
	switch val := v.(type) {
	case nil:
		buf.WriteString("null")
	case bool:
		if val {
			buf.WriteString("true")
		} else {
			buf.WriteString("false")
		}
	case string:
		encodeString(buf, val)
	case json.Number:
		return encodeNumber(buf, val)
	case float64:
		// Reachable only when callers hand us a decoded value that used
		// float64 instead of json.Number. Reject non-integers.
		return encodeNumber(buf, json.Number(strconv.FormatFloat(val, 'g', -1, 64)))
	case []any:
		buf.WriteByte('[')
		for i, item := range val {
			if i > 0 {
				buf.WriteByte(',')
			}
			if err := encode(buf, item); err != nil {
				return err
			}
		}
		buf.WriteByte(']')
	case map[string]any:
		keys := make([]string, 0, len(val))
		for k := range val {
			keys = append(keys, k)
		}
		sort.Slice(keys, func(i, j int) bool { return keys[i] < keys[j] })
		buf.WriteByte('{')
		for i, k := range keys {
			if i > 0 {
				buf.WriteByte(',')
			}
			encodeString(buf, k)
			buf.WriteByte(':')
			if err := encode(buf, val[k]); err != nil {
				return err
			}
		}
		buf.WriteByte('}')
	default:
		return fmt.Errorf("canonicaljson: unsupported type %T", v)
	}
	return nil
}

func encodeNumber(buf *bytes.Buffer, n json.Number) error {
	s := n.String()
	// Integer fast path.
	if i, err := strconv.ParseInt(s, 10, 64); err == nil {
		if i > (1<<53-1) || i < -(1<<53-1) {
			return fmt.Errorf("canonicaljson: integer %d out of range", i)
		}
		buf.WriteString(strconv.FormatInt(i, 10))
		return nil
	}
	// A JSON number that is not a plain integer. Matrix canonical JSON forbids
	// fractional/exponent numbers in signed content; reject it.
	f, err := strconv.ParseFloat(s, 64)
	if err != nil {
		return fmt.Errorf("canonicaljson: invalid number %q", s)
	}
	if math.Trunc(f) != f || math.IsInf(f, 0) || math.IsNaN(f) {
		return fmt.Errorf("canonicaljson: non-integer number %q not allowed", s)
	}
	if f > (1<<53-1) || f < -(1<<53-1) {
		return fmt.Errorf("canonicaljson: integer %v out of range", f)
	}
	buf.WriteString(strconv.FormatInt(int64(f), 10))
	return nil
}

// encodeString writes s as a canonical JSON string. Only the mandatory escapes
// are applied; all other characters are emitted as UTF-8.
func encodeString(buf *bytes.Buffer, s string) {
	buf.WriteByte('"')
	for i := 0; i < len(s); {
		c := s[i]
		if c < 0x80 {
			switch c {
			case '"':
				buf.WriteString(`\"`)
			case '\\':
				buf.WriteString(`\\`)
			case '\b':
				buf.WriteString(`\b`)
			case '\f':
				buf.WriteString(`\f`)
			case '\n':
				buf.WriteString(`\n`)
			case '\r':
				buf.WriteString(`\r`)
			case '\t':
				buf.WriteString(`\t`)
			default:
				if c < 0x20 {
					buf.WriteString(`\u00`)
					const hex = "0123456789abcdef"
					buf.WriteByte(hex[c>>4])
					buf.WriteByte(hex[c&0xf])
				} else {
					buf.WriteByte(c)
				}
			}
			i++
			continue
		}
		// Multi-byte UTF-8: copy the rune verbatim (canonical JSON keeps UTF-8).
		r, size := utf8.DecodeRuneInString(s[i:])
		if r == utf8.RuneError && size == 1 {
			// Invalid UTF-8 byte; emit replacement character escape.
			buf.WriteString(`�`)
			i++
			continue
		}
		buf.WriteString(s[i : i+size])
		i += size
	}
	buf.WriteByte('"')
}
