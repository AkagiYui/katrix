package canonicaljson

import "testing"

// Test vectors from the Matrix spec appendix "Canonical JSON".
func TestCanonicalSpecVectors(t *testing.T) {
	cases := []struct{ in, want string }{
		{`{}`, `{}`},
		{
			`{"one":1,"two":"Two"}`,
			`{"one":1,"two":"Two"}`,
		},
		{
			`{"b":"2","a":"1"}`,
			`{"a":"1","b":"2"}`,
		},
		{
			`{"auth":{"success":true,"mxid":"@john.doe:example.com","profile":{"display_name":"John Doe","three_pids":[{"medium":"email","address":"john.doe@example.org"},{"medium":"msisdn","address":"123456789"}]}}}`,
			`{"auth":{"mxid":"@john.doe:example.com","profile":{"display_name":"John Doe","three_pids":[{"address":"john.doe@example.org","medium":"email"},{"address":"123456789","medium":"msisdn"}]},"success":true}}`,
		},
		{
			`{"a":"日本語"}`,
			`{"a":"日本語"}`,
		},
		{
			`{"本":2,"日":1}`,
			`{"日":1,"本":2}`,
		},
		{
			`{"a":"日"}`,
			`{"a":"日"}`,
		},
		{
			`{"a":null}`,
			`{"a":null}`,
		},
	}
	for _, c := range cases {
		got, err := Canonical([]byte(c.in))
		if err != nil {
			t.Fatalf("Canonical(%s): %v", c.in, err)
		}
		if string(got) != c.want {
			t.Errorf("Canonical(%s) = %s, want %s", c.in, got, c.want)
		}
	}
}

func TestRejectNonIntegerFloat(t *testing.T) {
	if _, err := Canonical([]byte(`{"a":1.5}`)); err == nil {
		t.Fatal("expected error for fractional number")
	}
}

func TestIntegerRange(t *testing.T) {
	if _, err := Canonical([]byte(`{"a":9007199254740992}`)); err == nil {
		t.Fatal("expected error for out-of-range integer")
	}
	if _, err := Canonical([]byte(`{"a":9007199254740991}`)); err != nil {
		t.Fatalf("max safe integer should be allowed: %v", err)
	}
}
