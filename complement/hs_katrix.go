// hs_katrix.go - registers katrix as a Complement-known homeserver.
//
// Complement's runtime.SkipIf() detects which homeserver is under test via a
// `*_blacklist` build tag. Without one, every test that calls SkipIf prints:
//
//	WARNING: ... called runtime.SkipIf([...]) but Complement doesn't know
//	which HS is running as it was run without a *_blacklist tag
//
// and then runs the test anyway (often failing). Running with
// `-tags katrix_blacklist` and this file lets those tests skip cleanly on
// katrix where appropriate.
//
// This is vendored into Complement's checkout at CI time (see
// .github/workflows/test.yml) because Complement does not ship a katrix entry
// upstream. Modelled on complement/runtime/hs_dendrite.go.

//go:build katrix_blacklist
// +build katrix_blacklist

package runtime

const Katrix = "katrix"

func init() {
	Homeserver = Katrix
}
