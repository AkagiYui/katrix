package roomver

import "testing"

func TestMSC3389RoomVersion(t *testing.T) {
	rules, ok := Get("org.matrix.msc3389.10")
	if !ok {
		t.Fatal("MSC3389 room version is not registered")
	}
	if !rules.RedactionKeepsRelations {
		t.Error("MSC3389 relation redaction is disabled")
	}
	if rules.UpdatedRedaction || rules.CreateOmitsCreator {
		t.Error("MSC3389 alias inherited unrelated room-version-11 changes")
	}
	if !rules.StrictPowerLevels || !rules.KnockRestrictedAllowed {
		t.Error("MSC3389 alias did not inherit its room-version-10 baseline")
	}
	if got := CapabilityMap()["org.matrix.msc3389.10"]; got != "unstable" {
		t.Errorf("MSC3389 capability = %q, want unstable", got)
	}
	if !containsVersion(Supported(), "org.matrix.msc3389.10") {
		t.Error("MSC3389 missing from federation-supported versions")
	}
}

func TestSupportedIncludesUnstableRoomVersions(t *testing.T) {
	for _, version := range []Version{"org.matrix.msc3389.10", "org.matrix.msc3757.10"} {
		if !containsVersion(Supported(), version) {
			t.Errorf("Supported() does not contain %q", version)
		}
	}
}

func containsVersion(versions []Version, target Version) bool {
	for _, version := range versions {
		if version == target {
			return true
		}
	}
	return false
}
