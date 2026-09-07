package daemon

import (
	"os"
	"path/filepath"
	"reflect"
	"runtime"
	"testing"
)

func TestScheduleStateRoundTripsAtomically(t *testing.T) {
	path := filepath.Join(t.TempDir(), "state", "schedules.json")
	want := map[string]bool{"demo/nightly": true, "other/hourly": true}
	if err := saveScheduleState(path, want); err != nil {
		t.Fatal(err)
	}
	got, err := loadScheduleState(path)
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("state = %#v, want %#v", got, want)
	}
	info, err := os.Stat(path)
	if err != nil {
		t.Fatal(err)
	}
	if runtime.GOOS != "windows" && info.Mode().Perm() != 0o600 {
		t.Fatalf("state permissions = %o, want 600", info.Mode().Perm())
	}
}

func TestScheduleStateRejectsInvalidVersionAndJSON(t *testing.T) {
	path := filepath.Join(t.TempDir(), "schedules.json")
	for name, content := range map[string]string{
		"invalid json":        "not json",
		"unsupported version": `{"version":2,"disabled":["demo/job"]}`,
	} {
		t.Run(name, func(t *testing.T) {
			if err := os.WriteFile(path, []byte(content), 0o600); err != nil {
				t.Fatal(err)
			}
			if _, err := loadScheduleState(path); err == nil {
				t.Fatal("loadScheduleState unexpectedly succeeded")
			}
		})
	}
}
