package main

import (
	"bytes"
	"os"
	"strings"
	"testing"
)

func TestParseGoJSONCounts(t *testing.T) {
	data := []byte(`{"Action":"pass","Package":"github.com/matrix-org/complement/tests","Test":"TestFoo"}
{"Action":"fail","Package":"github.com/matrix-org/complement/tests","Test":"TestBar"}
{"Action":"fail","Package":"github.com/matrix-org/complement/tests","Test":"TestBar/sub1"}
{"Action":"skip","Package":"github.com/matrix-org/complement/tests","Test":"TestBaz"}
{"Action":"fail","Package":"github.com/matrix-org/complement/tests"}
`)
	s := parseGoSuite(data)
	if s.Pass != 1 || s.Fail != 2 || s.Skip != 1 {
		t.Fatalf("got pass=%d fail=%d skip=%d, want 1/2/1", s.Pass, s.Fail, s.Skip)
	}
	// The package-level fail event (no Test field) must not create a result.
	if len(s.Results) != 4 {
		t.Fatalf("got %d results, want 4 (package-level event excluded)", len(s.Results))
	}
}

func TestParseGoJSONIgnoresNoisePrefix(t *testing.T) {
	// go test -json output can be prefixed by `go: downloading` noise lines.
	data := []byte("go: downloading github.com/tidwall/gjson v1.14.4\n" +
		`{"Action":"pass","Package":"p/tests","Test":"TestA"}` + "\n")
	if !looksLikeGoJSON(data) {
		t.Fatal("looksLikeGoJSON should accept noise-prefixed JSON logs")
	}
	s := parseGoSuite(data)
	if s.Pass != 1 {
		t.Fatalf("got pass=%d want 1", s.Pass)
	}
}

func TestParseGoVerbose(t *testing.T) {
	data := []byte(`=== RUN   TestFoo
    foo_test.go:10: got status 500, want 200
--- PASS: TestFoo (0.00s)
=== RUN   TestBar
    bar_test.go:5: timed out after 5s
--- FAIL: TestBar (1.00s)
=== RUN   TestBaz
--- SKIP: TestBaz (0.00s)
`)
	s := parseGoSuite(data)
	if s.Pass != 1 || s.Fail != 1 || s.Skip != 1 {
		t.Fatalf("got pass=%d fail=%d skip=%d, want 1/1/1", s.Pass, s.Fail, s.Skip)
	}
	if len(s.Results) != 3 {
		t.Fatalf("got %d results, want 3", len(s.Results))
	}
}

func TestReportTAPSplitsExpectedFail(t *testing.T) {
	tap := `1..5
ok 1 POST /register works
not ok 2 Broken feature # TODO expected fail
not ok 3 (expected fail) Another broken # TODO expected fail
not ok 4 Real regression
not ok 5 (expected fail) Third broken
`
	out := runTAP(t, tap)
	for _, want := range []string{
		"| ok | not ok | expected fail | unexpected fail | Pass rate |",
		"| 1 | 4 | 3 | 1 |",
		"`Real regression`",
	} {
		if !strings.Contains(out, want) {
			t.Fatalf("output missing %q:\n%s", want, out)
		}
	}
}

func TestReportTAPDoesNotCountSkipAsFail(t *testing.T) {
	tap := "1..2\nok 1 Foo\nok 2 Bar # skip lack of can_post_room_receipts\n"
	out := runTAP(t, tap)
	if !strings.Contains(out, "| 2 | 0 | 0 | 0 |") {
		t.Fatalf("expected ok=2 notOk=0, got:\n%s", out)
	}
}

func TestTopLevel(t *testing.T) {
	cases := map[string]string{
		"TestACLs":               "TestACLs",
		"TestFoo/bar":            "TestFoo",
		"TestFoo/bar/baz":        "TestFoo",
		"TestAlice/TestOne/subA": "TestAlice",
	}
	for in, want := range cases {
		if got := topLevel(in); got != want {
			t.Errorf("topLevel(%q) = %q, want %q", in, got, want)
		}
	}
}

func TestPickDetailPrefersAssertion(t *testing.T) {
	lines := []string{
		"deploy times: 1.2s",
		"foo_test.go:10: got status 500, want 200",
		"some trailing noise",
	}
	if got := pickDetail(lines); !strings.Contains(got, "got status 500, want 200") {
		t.Fatalf("pickDetail should prefer the assertion line, got %q", got)
	}
	// Fallback: no assertion-like line -> first non-noise line.
	if got := pickDetail([]string{"deploy times: 1.2s", "room created"}); got == "" {
		t.Fatal("pickDetail should return a fallback when no assertion exists")
	}
}

func TestShortPkg(t *testing.T) {
	cases := map[string]string{
		"github.com/matrix-org/complement/tests":         "federation",
		"github.com/matrix-org/complement/tests/csapi":   "csapi",
		"github.com/matrix-org/complement/tests/msc2836": "msc2836",
	}
	for in, want := range cases {
		if got := shortPkg(in); got != want {
			t.Errorf("shortPkg(%q) = %q, want %q", in, got, want)
		}
	}
}

// runTAP runs reportTAP against the given TAP input and returns the printed
// output, restoring stdout afterwards.
func runTAP(t *testing.T, tap string) string {
	t.Helper()
	old := os.Stdout
	r, w, err := os.Pipe()
	if err != nil {
		t.Fatal(err)
	}
	os.Stdout = w
	reportTAP([]byte(tap))
	w.Close()
	os.Stdout = old
	var buf bytes.Buffer
	if _, err := buf.ReadFrom(r); err != nil {
		t.Fatal(err)
	}
	return buf.String()
}
