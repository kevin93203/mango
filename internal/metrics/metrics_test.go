package metrics

import (
	"syscall"
	"testing"

	"github.com/shirou/gopsutil/v3/net"
)

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
