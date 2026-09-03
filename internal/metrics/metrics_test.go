package metrics

import (
	"os"
	"syscall"
	"testing"

	"github.com/shirou/gopsutil/v3/net"
)

func TestSnapshotReportsCurrentProcess(t *testing.T) {
	snapshot, ok := NewCollector().Snapshot(os.Getpid(), 0)
	if !ok {
		t.Fatal("Snapshot() returned false for the current process")
	}
	if snapshot.PID != os.Getpid() {
		t.Fatalf("snapshot pid = %d, want %d", snapshot.PID, os.Getpid())
	}
	if snapshot.Depth != 0 || snapshot.ParentPID != 0 {
		t.Fatalf("root relationship = parent %d, depth %d; want parent 0, depth 0", snapshot.ParentPID, snapshot.Depth)
	}
	if snapshot.Name == "" && snapshot.CommandLine == "" {
		t.Fatal("snapshot has neither process name nor command line")
	}
}

func TestBuildSnapshotTreeSortsNestedChildrenAndSkipsMissingProcesses(t *testing.T) {
	nodes := []treeNode{
		{pid: 100},
		{pid: 300, parentPID: 100, depth: 1},
		{pid: 500, parentPID: 300, depth: 2},
		{pid: 200, parentPID: 100, depth: 1},
		{pid: 400, parentPID: 100, depth: 1},
	}
	snapshots := map[int32]ProcessSnapshot{
		100: {PID: 100},
		300: {PID: 300, ParentPID: 100, Depth: 1},
		500: {PID: 500, ParentPID: 300, Depth: 2},
		200: {PID: 200, ParentPID: 100, Depth: 1},
		// PID 400 is intentionally missing to simulate a process that exited
		// before its snapshot could be collected.
	}

	got, ok := buildSnapshotTree(100, nodes, snapshots)
	if !ok {
		t.Fatal("buildSnapshotTree() returned false")
	}
	if len(got.Children) != 2 || got.Children[0].PID != 200 || got.Children[1].PID != 300 {
		t.Fatalf("root children = %+v, want pids 200 and 300", got.Children)
	}
	if len(got.Children[1].Children) != 1 || got.Children[1].Children[0].PID != 500 {
		t.Fatalf("nested children = %+v, want pid 500", got.Children[1].Children)
	}
}

func TestBuildSnapshotTreeRequiresRootSnapshot(t *testing.T) {
	got, ok := buildSnapshotTree(100, []treeNode{{pid: 100}}, map[int32]ProcessSnapshot{})
	if ok || got.PID != 0 {
		t.Fatalf("buildSnapshotTree() = %+v, %t; want empty snapshot and false", got, ok)
	}
}

func TestListeningPortsFiltersAndSortsConnections(t *testing.T) {
	connections := []net.ConnectionStat{
		{Type: syscall.SOCK_STREAM, Status: "LISTEN", Laddr: net.Addr{Port: 8080}},
		{Type: syscall.SOCK_STREAM, Status: "LISTEN", Laddr: net.Addr{Port: 80}},
		{Type: syscall.SOCK_STREAM, Status: "LISTEN", Laddr: net.Addr{Port: 8080}},
		{Type: syscall.SOCK_STREAM, Status: "ESTABLISHED", Laddr: net.Addr{Port: 3000}},
		{Type: syscall.SOCK_DGRAM, Status: "NONE", Laddr: net.Addr{Port: 5353}},
		{Type: syscall.SOCK_DGRAM, Status: "NONE", Laddr: net.Addr{Port: 0}},
	}

	want := []string{"tcp:80", "udp:5353", "tcp:8080"}
	got := listeningPorts(connections)
	if len(got) != len(want) {
		t.Fatalf("ports = %v, want %v", got, want)
	}
	for i := range want {
		if got[i] != want[i] {
			t.Fatalf("ports = %v, want %v", got, want)
		}
	}
}
