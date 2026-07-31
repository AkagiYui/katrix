// Command testreport converts a test-suite log into a markdown report for the
// CI job summary (GITHUB_STEP_SUMMARY), so each black-box suite answers "which
// tests failed and what was expected vs what actually happened" without
// downloading artifacts.
//
// Usage:
//
//	testreport complement <logfile>   # go test -v or -json output
//	testreport sytest <results.tap>   # SyTest TAP output
//	testreport crypto <logfile>       # go test -json output
//
// Every report carries aggregate pass/fail/skip counts plus a per-test
// breakdown of failures with the first "expected vs actual" assertion
// difference, collapsed into a <details> block to keep the summary compact.
// Works on both `go test -json` (CI) and plain `-v` text (fallback), and
// tolerates non-JSON noise (e.g. `go: downloading` lines).
package main

import (
	"bufio"
	"encoding/json"
	"fmt"
	"os"
	"regexp"
	"sort"
	"strings"
)

// result is one test case (a parent test or a subtest).
type result struct {
	Pkg    string   // short package name (csapi / federation / mscXXXX)
	Name   string   // full test name, parents and subtests alike
	Result string   // PASS / FAIL / SKIP
	Lines  []string // assertion output captured for this test
	Detail string   // first "expected vs actual" line
}

// suite is a parsed go-test log.
type suite struct {
	Results []result
	Pass    int
	Fail    int
	Skip    int
}

func main() {
	if len(os.Args) < 3 {
		fmt.Fprintln(os.Stderr, "usage: testreport complement|sytest|crypto <logfile>")
		os.Exit(2)
	}
	mode, path := os.Args[1], os.Args[2]
	data, err := os.ReadFile(path)
	if err != nil {
		fmt.Printf("## %s report\n\nlog not found: %s\n", modeTitle(mode), path)
		return
	}
	switch mode {
	case "complement":
		reportGoSuite("Complement", data, mode)
	case "crypto":
		reportGoSuite("Complement Crypto", data, mode)
	case "sytest":
		reportTAP(data)
	default:
		fmt.Fprintln(os.Stderr, "unknown mode:", mode)
		os.Exit(2)
	}
}

func modeTitle(mode string) string {
	switch mode {
	case "complement":
		return "Complement"
	case "crypto":
		return "Complement Crypto"
	case "sytest":
		return "SyTest"
	}
	return mode
}

// ---- go test logs (complement / crypto) ----

func parseGoSuite(data []byte) suite {
	s := strings.TrimSpace(string(data))
	if s == "" {
		return suite{}
	}
	if looksLikeGoJSON(data) {
		return parseGoJSON(data)
	}
	return parseGoVerbose(data)
}

// looksLikeGoJSON reports whether the log contains go test -json events. The
// crypto log starts with `go: downloading` noise lines, so scan ahead instead
// of only checking the first line.
func looksLikeGoJSON(data []byte) bool {
	sc := newScanner(data)
	scanned := 0
	for sc.Scan() && scanned < 100 {
		line := strings.TrimSpace(sc.Text())
		scanned++
		if !strings.HasPrefix(line, "{") {
			continue
		}
		var ev goEvent
		if json.Unmarshal([]byte(line), &ev) == nil && ev.Action != "" {
			return true
		}
	}
	return false
}

type goEvent struct {
	Action  string `json:"Action"`
	Package string `json:"Package"`
	Test    string `json:"Test"`
	Output  string `json:"Output"`
}

func parseGoJSON(data []byte) suite {
	var out suite
	byKey := map[string]*result{}
	var order []string
	sc := newScanner(data)
	for sc.Scan() {
		line := sc.Text()
		if !strings.HasPrefix(strings.TrimSpace(line), "{") {
			continue // non-JSON noise (go: downloading, ...)
		}
		var ev goEvent
		if err := json.Unmarshal([]byte(line), &ev); err != nil {
			continue
		}
		if ev.Test == "" {
			continue // package-level event, no per-test accounting
		}
		key := ev.Package + "\x00" + ev.Test
		r, ok := byKey[key]
		if !ok {
			r = &result{Pkg: shortPkg(ev.Package), Name: ev.Test}
			byKey[key] = r
			order = append(order, key)
		}
		switch ev.Action {
		case "pass":
			r.Result = "PASS"
			out.Pass++
		case "fail":
			r.Result = "FAIL"
			out.Fail++
		case "skip":
			r.Result = "SKIP"
			out.Skip++
		case "output":
			if ev.Output != "" {
				r.Lines = append(r.Lines, strings.Split(ev.Output, "\n")...)
			}
		}
	}
	for _, k := range order {
		r := byKey[k]
		r.Detail = pickDetail(r.Lines)
		out.Results = append(out.Results, *r)
	}
	return out
}

// parseGoVerbose parses plain `go test -v` output. Parents and subtests are
// tracked with a RUN/FINISH stack; `=== NAME` markers (parallel subtests) are
// deliberately ignored so each `=== RUN` has exactly one matching result line.
func parseGoVerbose(data []byte) suite {
	var out suite
	var stack []string
	byName := map[string]*result{}
	var order []string
	sc := newScanner(data)
	for sc.Scan() {
		line := sc.Text()
		if m := runRe.FindStringSubmatch(line); m != nil {
			name := m[1]
			stack = append(stack, name)
			if _, ok := byName[name]; !ok {
				byName[name] = &result{Name: name}
				order = append(order, name)
			}
			continue
		}
		if m := doneRe.FindStringSubmatch(line); m != nil {
			switch m[1] {
			case "PASS":
				out.Pass++
			case "FAIL":
				out.Fail++
			case "SKIP":
				out.Skip++
			}
			if r, ok := byName[m[2]]; ok {
				r.Result = m[1]
			}
			if len(stack) > 0 {
				stack = stack[:len(stack)-1]
			}
			// Give the parent the child's package if it never emitted a
			// test-file line of its own (parents rarely do).
			if len(stack) > 0 {
				if parent, ok := byName[stack[len(stack)-1]]; ok && parent.Pkg == "" {
					if child, ok := byName[m[2]]; ok {
						parent.Pkg = child.Pkg
					}
				}
			}
			continue
		}
		if len(stack) > 0 {
			if m := errRe.FindStringSubmatch(line); m != nil {
				cur := stack[len(stack)-1]
				if r, ok := byName[cur]; ok {
					if r.Pkg == "" {
						r.Pkg = pkgFromFile(fileName(m[1]))
					}
					r.Lines = append(r.Lines, m[1])
				}
			}
		}
	}
	for _, n := range order {
		r := byName[n]
		r.Detail = pickDetail(r.Lines)
		out.Results = append(out.Results, *r)
	}
	return out
}

var (
	runRe  = regexp.MustCompile(`^\s*=== RUN\s+(\S+)\s*$`)
	doneRe = regexp.MustCompile(`^\s*--- (PASS|FAIL|SKIP): (\S+)\s*(\(\d+(\.\d+)?s\))?\s*$`)
	errRe  = regexp.MustCompile(`^\s{4}([a-z0-9_]+_test\.go:\d+:.*)$`)
)

// shortPkg maps a complement package path to a short label.
func shortPkg(p string) string {
	parts := strings.Split(p, "/")
	last := parts[len(parts)-1]
	switch last {
	case "tests":
		// The complement root package holds the federation + misc suites.
		return "federation"
	case "csapi":
		return "csapi"
	}
	if strings.HasPrefix(last, "msc") {
		return last
	}
	return last
}

// pkgFromFile is the -v fallback: the test file name reveals the suite.
func pkgFromFile(file string) string {
	switch {
	case strings.HasPrefix(file, "federation_"):
		return "federation"
	case strings.HasPrefix(file, "delayed_event"):
		return "msc4140"
	case strings.HasPrefix(file, "thread_subscriptions"):
		return "msc4306"
	case strings.HasPrefix(file, "owned_state"):
		return "msc3757"
	case strings.HasPrefix(file, "invalid_test"):
		return "csapi"
	default:
		return "csapi"
	}
}

// fileName extracts the test file from a "file.go:NN: msg" line.
func fileName(line string) string {
	if m := fileLineRe.FindStringSubmatch(line); m != nil {
		return m[1]
	}
	return ""
}

// pickDetail returns the first assertion-like line (expected vs actual) from a
// test's output, falling back to the first non-noise line.
func pickDetail(lines []string) string {
	var fallback string
	for _, raw := range lines {
		l := stripANSI(raw)
		l = strings.TrimSpace(l)
		if l == "" {
			continue
		}
		// Ignore "file.go:NN:" prefixes when classifying the line.
		stripped := l
		if m := fileLineRe.FindStringSubmatch(l); m != nil {
			stripped = strings.TrimSpace(m[3])
		}
		if noiseRe.MatchString(stripped) {
			continue
		}
		if fallback == "" {
			fallback = l
		}
		if isAssertion(stripped) {
			return truncate(l)
		}
	}
	return truncate(fallback)
}

var (
	fileLineRe = regexp.MustCompile(`^([a-z0-9_]+_test\.go):(\d+):(.*)$`)
	noiseRe    = regexp.MustCompile(`(?i)^(deploy times:|.*waiting for event id|.*retryuntil|.*\[csapi\]|.*sharing \[server_name|.*: end logs|.*: server logs|katrix: |=== (run|name)|--- (pass|fail|skip):|\d{4}/\d{2}/\d{2} \d{2}:\d{2}:\d{2} =+|^=+$|^-+$)`)
)

func isAssertion(l string) bool {
	return strings.Contains(l, "got status") ||
		strings.Contains(l, "want ") ||
		strings.Contains(l, "MustSyncUntil") ||
		strings.Contains(l, "MatchResponse") ||
		strings.Contains(l, "expected") ||
		strings.Contains(l, "does not exist") ||
		strings.Contains(l, "returned non-") ||
		strings.Contains(l, "timed out") ||
		strings.Contains(l, "not found in ")
}

var ansiRe = regexp.MustCompile(`\x1b\[[0-9;]*m`)

func stripANSI(s string) string { return ansiRe.ReplaceAllString(s, "") }

func truncate(s string) string {
	s = strings.ReplaceAll(s, "\r", "")
	if len(s) > 240 {
		s = s[:237] + "..."
	}
	return s
}

// ---- SyTest TAP ----

type tapFail struct {
	Name   string
	Detail string
}

func reportTAP(data []byte) {
	sc := newScanner(data)
	var ok, notOk int
	var fails []*tapFail
	var inFail *tapFail
	for sc.Scan() {
		line := sc.Text()
		if m := tapStatusRe.FindStringSubmatch(line); m != nil {
			status, rest := m[1], m[2]
			if status == "ok" {
				ok++
				inFail = nil
			} else {
				notOk++
				name := strings.TrimSpace(strings.Replace(rest, "(expected fail) ", "", 1))
				if mm := tapNoteRe.FindStringSubmatch(name); mm != nil {
					name = strings.TrimSpace(mm[1])
				}
				inFail = &tapFail{Name: name}
				fails = append(fails, inFail)
			}
			continue
		}
		// Diagnostic lines following a failure: prefer the assertion diff
		// ("Got X, expected Y"), skip Started/Ended timestamps.
		if inFail != nil && strings.HasPrefix(line, "#") {
			d := strings.TrimSpace(strings.TrimPrefix(line, "#"))
			if d == "" || strings.HasPrefix(d, "Started:") || strings.HasPrefix(d, "Ended:") {
				continue
			}
			if inFail.Detail == "" {
				inFail.Detail = d
			} else if strings.Contains(d, "expected") && !strings.Contains(inFail.Detail, "expected") {
				inFail.Detail = d
			}
		}
	}
	total := ok + notOk
	rate := 0.0
	if total > 0 {
		rate = float64(ok) * 100 / float64(total)
	}
	fmt.Println("## SyTest results")
	fmt.Println()
	fmt.Println("| ok | not ok | total | Pass rate |")
	fmt.Println("|---|---|---|---|")
	fmt.Printf("| %d | %d | %d | %.1f%% |\n", ok, notOk, total, rate)
	fmt.Println()
	printFailTable("Failed tests", failsToDetails(fails))
}

var (
	tapStatusRe = regexp.MustCompile(`^(ok|not ok)\s+\d+\s+(.*)$`)
	tapNoteRe   = regexp.MustCompile(`^(.*?)\s*#\s*.*$`)
)

// ---- shared markdown ----

func reportGoSuite(title string, data []byte, mode string) {
	s := parseGoSuite(data)
	total := s.Pass + s.Fail + s.Skip
	rate := 0.0
	if total > 0 {
		rate = float64(s.Pass) * 100 / float64(total)
	}
	fmt.Printf("## %s results\n", title)
	fmt.Println()
	if mode == "crypto" {
		fmt.Println("Note: baseline run, crypto implementation is incomplete; failures expected.")
		fmt.Println()
	}
	fmt.Println("| PASS | FAIL | SKIP | Pass rate |")
	fmt.Println("|---|---|---|---|")
	fmt.Printf("| %d | %d | %d | %.1f%% (%d / %d) |\n", s.Pass, s.Fail, s.Skip, rate, s.Pass, total)
	fmt.Println()

	if mode == "complement" {
		byPkg := map[string][3]int{}
		for _, r := range s.Results {
			c := byPkg[r.Pkg]
			switch r.Result {
			case "PASS":
				c[0]++
			case "FAIL":
				c[1]++
			case "SKIP":
				c[2]++
			}
			byPkg[r.Pkg] = c
		}
		pkgs := make([]string, 0, len(byPkg))
		for p := range byPkg {
			pkgs = append(pkgs, p)
		}
		sort.Strings(pkgs)
		fmt.Println("| package | PASS | FAIL | SKIP |")
		fmt.Println("|---|---|---|---|")
		for _, p := range pkgs {
			display := p
			if display == "" {
				display = "other"
			}
			c := byPkg[p]
			fmt.Printf("| %s | %d | %d | %d |\n", display, c[0], c[1], c[2])
		}
		fmt.Println()
	}

	// Collapse to top-level tests for the failure table. Prefer a real
	// assertion line ("expected vs actual") over a generic fallback: a
	// top-level test often has no assertion of its own while its subtests do.
	top := map[string]string{}
	topFallback := map[string]string{}
	for i := range s.Results {
		r := &s.Results[i]
		if r.Result != "FAIL" || r.Detail == "" {
			continue
		}
		tl := topLevel(r.Name)
		if isAssertion(r.Detail) {
			if top[tl] == "" {
				top[tl] = r.Detail
			}
		} else if topFallback[tl] == "" {
			topFallback[tl] = r.Detail
		}
	}
	for n, d := range topFallback {
		if top[n] == "" {
			top[n] = d
		}
	}
	names := make([]string, 0, len(top))
	for n := range top {
		names = append(names, n)
	}
	sort.Strings(names)
	printFailTable("Failed tests", namesToDetails(names, top))
}

func topLevel(name string) string {
	if i := strings.Index(name, "/"); i >= 0 {
		return name[:i]
	}
	return name
}

// printFailTable renders the collapsed failure table.
func printFailTable(summary string, rows []detailsRow) {
	total := len(rows)
	shown := total
	const maxRows = 150
	if shown > maxRows {
		shown = maxRows
	}
	fmt.Printf("<details><summary>%s (%d, showing first %d)</summary>\n\n", summary, total, shown)
	fmt.Println("| Test | Expected vs actual |")
	fmt.Println("|---|---|")
	if total == 0 {
		fmt.Println("| _none_ | |")
	}
	for _, r := range rows[:shown] {
		fmt.Printf("| %s | %s |\n", cell(r.name), cell(r.detail))
	}
	if shown < total {
		fmt.Printf("| _… and %d more (full log in artifacts)_ | |\n", total-shown)
	}
	fmt.Println("\n</details>")
}

type detailsRow struct {
	name   string
	detail string
}

func namesToDetails(names []string, top map[string]string) []detailsRow {
	rows := make([]detailsRow, 0, len(names))
	for _, n := range names {
		rows = append(rows, detailsRow{n, top[n]})
	}
	return rows
}

func failsToDetails(fails []*tapFail) []detailsRow {
	rows := make([]detailsRow, 0, len(fails))
	for _, f := range fails {
		rows = append(rows, detailsRow{f.Name, f.Detail})
	}
	return rows
}

// cell escapes a value for a markdown table cell.
func cell(s string) string {
	s = strings.ReplaceAll(s, "|", `\|`)
	if strings.Contains(s, "`") {
		return s
	}
	return "`" + s + "`"
}

func newScanner(data []byte) *bufio.Scanner {
	sc := bufio.NewScanner(strings.NewReader(string(data)))
	// Output events can embed huge assertion dumps; allow multi-MB lines.
	sc.Buffer(make([]byte, 64*1024), 16*1024*1024)
	return sc
}
