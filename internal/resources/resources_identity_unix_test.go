//go:build !windows

package resources

import (
	"testing"

	"github.com/kevin93203/mango/internal/config"
)

func TestNormalizeIdentityRejectsUnknownNames(t *testing.T) {
	if _, err := NormalizeIdentity(&config.RunAs{User: "mango-user-that-does-not-exist"}); err == nil {
		t.Fatal("expected unknown run_as user to be rejected")
	}
	if _, err := NormalizeIdentity(&config.RunAs{Group: "mango-group-that-does-not-exist"}); err == nil {
		t.Fatal("expected unknown run_as group to be rejected")
	}
}
