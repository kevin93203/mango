package target

import (
	"strings"
	"testing"
)

func TestParseStableTargets(t *testing.T) {
	tests := []struct {
		name string
		got  func(string) (Reference, error)
		want Reference
	}{
		{"service", ParseService, Reference{Raw: "demo/api", Kind: Service, Project: "demo", Name: "api"}},
		{"service id", ParseService, Reference{Raw: "7", Kind: Service, Name: "7"}},
		{"project", ParseProject, Reference{Raw: "demo", Kind: Project, Project: "demo", Name: "demo"}},
		{"task", ParseTask, Reference{Raw: "demo/backup", Kind: Task, Project: "demo", Name: "backup"}},
		{"workflow", ParseWorkflow, Reference{Raw: "demo/release", Kind: Workflow, Project: "demo", Name: "release"}},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			got, err := test.got(test.want.Raw)
			if err != nil {
				t.Fatal(err)
			}
			if got != test.want {
				t.Fatalf("reference = %#v, want %#v", got, test.want)
			}
		})
	}
}

func TestParseLogTargets(t *testing.T) {
	for _, value := range []string{"demo/api", "demo/task/backup", "demo/workflow/release/build"} {
		if _, err := ParseLog(value); err != nil {
			t.Fatalf("ParseLog(%q) = %v", value, err)
		}
	}
}

func TestParseScheduleAndServiceOperationTargets(t *testing.T) {
	schedule, err := ParseSchedule("demo")
	if err != nil || schedule.Kind != Schedule || schedule.Project != "demo" {
		t.Fatalf("ParseSchedule(project) = %#v, %v", schedule, err)
	}
	service, err := ParseServiceOperation("demo")
	if err != nil || service.Kind != Service || service.Project != "demo" {
		t.Fatalf("ParseServiceOperation(project) = %#v, %v", service, err)
	}
	for _, value := range []string{"demo/nightly"} {
		if _, err := ParseSchedule(value); err != nil {
			t.Fatalf("ParseSchedule(%q) = %v", value, err)
		}
	}
}

func TestParseRunAcceptsUniqueReferencePrefixes(t *testing.T) {
	for _, value := range []string{"run-2026", "a1b2c3"} {
		got, err := ParseRun(value)
		if err != nil || got != value {
			t.Fatalf("ParseRun(%q) = %q, %v", value, got, err)
		}
	}
}

func TestParseRejectsAmbiguousForms(t *testing.T) {
	for _, test := range []struct {
		name  string
		got   func(string) (Reference, error)
		value string
	}{
		{"service", ParseService, "api"},
		{"task", ParseTask, "demo/backup/node"},
		{"workflow log", ParseLog, "demo/workflow/release"},
	} {
		t.Run(test.name, func(t *testing.T) {
			_, err := test.got(test.value)
			if err == nil || !strings.Contains(err.Error(), "expected") {
				t.Fatalf("error = %v, want canonical-form guidance", err)
			}
		})
	}
}

func TestParseRunRejectsPathLikeValue(t *testing.T) {
	if _, err := ParseRun("demo/task"); err == nil {
		t.Fatal("path-like run reference unexpectedly accepted")
	}
}
