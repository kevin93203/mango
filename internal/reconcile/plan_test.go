package reconcile

import (
	"testing"

	"github.com/kevin93203/mango/internal/config"
)

func TestBuildProducesDeterministicServiceActions(t *testing.T) {
	current := []config.EffectiveService{
		{Name: "web", Command: "old"},
		{Name: "removed", Command: "gone"},
	}
	desired := []config.EffectiveService{
		{Name: "added", Command: "new"},
		{Name: "web", Command: "new"},
	}
	plan := Build("demo", current, desired, map[string]bool{"web": true}, 4, 5)
	if len(plan.Changes) != 3 {
		t.Fatalf("changes = %+v", plan.Changes)
	}
	want := []string{"added", "removed", "restarted"}
	for index, action := range want {
		if plan.Changes[index].Action != action {
			t.Fatalf("change %d = %+v, want action %q", index, plan.Changes[index], action)
		}
	}
	if !plan.HasChanges() || !plan.Changes[2].Restart {
		t.Fatalf("plan = %+v", plan)
	}
}

func TestBuildMarksStoppedDefinitionChangesWithoutRestart(t *testing.T) {
	plan := Build("demo", []config.EffectiveService{{Name: "api", Command: "old"}}, []config.EffectiveService{{Name: "api", Command: "new"}}, map[string]bool{}, 1, 2)
	if len(plan.Changes) != 1 || plan.Changes[0].Action != "changed" || plan.Changes[0].Restart {
		t.Fatalf("plan = %+v", plan)
	}
}
