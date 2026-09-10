package resources

import (
	"math"
	"testing"

	"github.com/kevin93203/mango/internal/config"
)

func TestNormalizePolicyConvertsLimitsToWireUnits(t *testing.T) {
	policy, err := NormalizePolicy(&config.ResourcePolicy{ProcessLimit: 7, Memory: "512MiB", CPUPercent: 80})
	if err != nil {
		t.Fatal(err)
	}
	if policy.ProcessLimit != 7 || policy.MemoryBytes != 512*(1<<20) || policy.CPUPercent != 80 {
		t.Fatalf("normalized policy = %+v", policy)
	}
}

func TestNormalizePolicyRejectsWireOverflow(t *testing.T) {
	if uint64(math.MaxInt) <= math.MaxUint32 {
		t.Skip("int cannot represent a uint32 overflow on this architecture")
	}
	if _, err := NormalizePolicy(&config.ResourcePolicy{ProcessLimit: math.MaxInt}); err == nil {
		t.Fatal("expected process limit overflow to be rejected")
	}
}
