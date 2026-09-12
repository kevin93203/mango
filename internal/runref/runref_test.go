package runref

import "testing"

const (
	runIDA = "7f31a2c4-d9e0-4b11-9c8a-1234567890ab"
	runIDB = "7f31a2c4-d9e1-4b11-9c8a-1234567890ab"
)

func TestDisplayUsesCompactUUIDPrefixAndPreservesOpaqueIDs(t *testing.T) {
	if got := Display(runIDA, false); got != "7f31a2c4d9e0" {
		t.Fatalf("short UUID = %q", got)
	}
	if got := Display(runIDA, true); got != runIDA {
		t.Fatalf("full UUID = %q", got)
	}
	if got := Display("legacy-run-1", false); got != "legacy-run-1" {
		t.Fatalf("legacy ID = %q", got)
	}
	if got := Display("", false); got != "-" {
		t.Fatalf("empty ID = %q, want -", got)
	}
}

func TestUUIDReferencesAcceptCompactDashedAndUppercaseForms(t *testing.T) {
	cases := []string{
		"7f31a2c4",
		"7f31a2c4d9e0",
		"7f31a2c4-d9e0",
		"7F31A2C4-D9E0-4B11-9C8A-1234567890AB",
	}
	for _, reference := range cases {
		if !Matches(runIDA, reference) {
			t.Errorf("Matches(%q, %q) = false", runIDA, reference)
		}
	}
	if got := StoragePrefix("7f31a2c4d9e0"); got != "7f31a2c4-d9e0" {
		t.Fatalf("storage prefix = %q", got)
	}
	if got := SignificantLength("7f31a2c4-d9e0"); got != 12 {
		t.Fatalf("significant length = %d, want 12", got)
	}
}

func TestOpaqueReferencesRemainLiteralAndCaseSensitive(t *testing.T) {
	if !Matches("legacy-run-1234", "legacy-run") {
		t.Fatal("opaque prefix did not match")
	}
	if Matches("legacy-run-1234", "LEGACY-RUN") {
		t.Fatal("opaque prefix matched with different case")
	}
	if !Matches("deadbeef-legacy", "deadbeef") {
		t.Fatal("hex-looking legacy prefix did not match")
	}
}

func TestUniqueCandidatesAreDirectlyUsable(t *testing.T) {
	candidates := UniqueCandidates([]string{runIDB, "legacy-run-1234", runIDA})
	if len(candidates) != 3 {
		t.Fatalf("candidate count = %d", len(candidates))
	}
	for _, candidate := range candidates {
		if !Matches(candidate.RunID, candidate.Ref) {
			t.Errorf("candidate %q does not match %q", candidate.Ref, candidate.RunID)
		}
		for _, other := range candidates {
			if candidate.RunID != other.RunID && Matches(other.RunID, candidate.Ref) {
				t.Errorf("candidate %q also matches %q", candidate.Ref, other.RunID)
			}
		}
	}
	if candidates[0].Ref != "7f31a2c4d9e0" || candidates[1].Ref != "7f31a2c4d9e1" {
		t.Fatalf("UUID candidates = %+v", candidates[:2])
	}
}

func TestValidateRequiresMinimumOnlyForNonExactReferences(t *testing.T) {
	if err := Validate("1234567"); err == nil {
		t.Fatal("short reference unexpectedly validated")
	}
	if err := Validate("12345678"); err != nil {
		t.Fatalf("minimum reference rejected: %v", err)
	}
}
