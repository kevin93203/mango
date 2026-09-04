package api

import (
	"encoding/json"
	"testing"
)

func TestFlattenServiceListPreservesHierarchy(t *testing.T) {
	items := []ServiceInfo{{
		ID: 0, Project: "demo", Name: "api", ProcessName: "api.exe", PID: 100,
		Children: []ChildProcessInfo{{
			PID: 200, Depth: 1, Name: "worker",
			Children: []ChildProcessInfo{{PID: 300, Depth: 2, Name: "helper"}},
		}},
	}}

	rows := FlattenServiceList(items)
	if len(rows) != 3 {
		t.Fatalf("rows = %d, want 3", len(rows))
	}
	if !rows[0].Managed || rows[0].ParentIndex != 0 || rows[0].Depth != 0 || rows[0].Service != "demo/api" || rows[0].Process != "api.exe" {
		t.Fatalf("parent row = %+v", rows[0])
	}
	if rows[1].Managed || rows[1].PID != 200 || rows[1].ParentIndex != 0 || rows[1].Depth != 1 || rows[1].Service != "" || rows[1].Process != "worker" {
		t.Fatalf("child row = %+v", rows[1])
	}
	if rows[2].Managed || rows[2].PID != 300 || rows[2].ParentIndex != 0 || rows[2].Depth != 2 || rows[2].Service != "" || rows[2].Process != "helper" {
		t.Fatalf("grandchild row = %+v", rows[2])
	}
}

func TestServiceInfoJSONKeepsHealthAndChildren(t *testing.T) {
	items := []ServiceInfo{{
		Project: "demo", Name: "api", ProcessName: "api.exe", Health: &HealthInfo{Status: "healthy"},
		Children: []ChildProcessInfo{{PID: 200, OSState: "sleep"}},
	}}
	encoded, err := json.Marshal(items)
	if err != nil {
		t.Fatal(err)
	}
	var decoded []ServiceInfo
	if err := json.Unmarshal(encoded, &decoded); err != nil {
		t.Fatal(err)
	}
	if decoded[0].Health == nil || decoded[0].Health.Status != "healthy" {
		t.Fatalf("health = %+v", decoded[0].Health)
	}
	if decoded[0].ProcessName != "api.exe" {
		t.Fatalf("process name = %q, want api.exe", decoded[0].ProcessName)
	}
	if len(decoded[0].Children) != 1 || decoded[0].Children[0].PID != 200 {
		t.Fatalf("children = %+v", decoded[0].Children)
	}
}
