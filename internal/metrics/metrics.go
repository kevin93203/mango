package metrics

import (
	"sort"
	"strconv"
	"sync"
	"syscall"
	"time"

	"github.com/shirou/gopsutil/v3/mem"
	"github.com/shirou/gopsutil/v3/net"
	"github.com/shirou/gopsutil/v3/process"
)

type Sample struct {
	CPUPercent float64
	RSSBytes   uint64
	MemoryPct  float64
}

type Collector struct {
	mu        sync.Mutex
	processes map[int32]*process.Process
}

func NewCollector() *Collector {
	return &Collector{processes: map[int32]*process.Process{}}
}

func (c *Collector) Sample(pid int, interval time.Duration) Sample {
	if pid <= 0 {
		return Sample{}
	}
	p := c.getProcess(pid)
	if p == nil {
		return Sample{}
	}
	cpu, _ := p.Percent(0)
	info, _ := p.MemoryInfo()
	var rss uint64
	if info != nil {
		rss = info.RSS
	}
	total := uint64(0)
	if vm, err := mem.VirtualMemory(); err == nil && vm != nil {
		total = vm.Total
	}
	var memoryPct float64
	if total > 0 {
		memoryPct = float64(rss) / float64(total) * 100
	}
	return Sample{CPUPercent: cpu, RSSBytes: rss, MemoryPct: memoryPct}
}

// Ports returns the listening TCP and bound UDP ports for a managed process
// and its descendants. This also covers commands such as "go run" where the
// managed PID is a wrapper around the actual server process.
// Network inspection is best-effort because operating-system permissions may
// prevent reading another process's connections.
func (c *Collector) Ports(pid int) []string {
	if pid <= 0 {
		return nil
	}
	root := c.getProcess(pid)
	if root == nil {
		return nil
	}
	connections := make([]net.ConnectionStat, 0)
	for _, p := range processTree(root) {
		processConnections, err := p.Connections()
		if err == nil {
			connections = append(connections, processConnections...)
		}
	}
	return listeningPorts(connections)
}

func processTree(root *process.Process) []*process.Process {
	result := make([]*process.Process, 0, 1)
	queue := []*process.Process{root}
	seen := map[int32]struct{}{root.Pid: {}}
	for len(queue) > 0 {
		current := queue[0]
		queue = queue[1:]
		result = append(result, current)

		children, err := current.Children()
		if err != nil {
			continue
		}
		for _, child := range children {
			if child == nil {
				continue
			}
			if _, ok := seen[child.Pid]; ok {
				continue
			}
			seen[child.Pid] = struct{}{}
			queue = append(queue, child)
		}
	}
	return result
}

func (c *Collector) getProcess(pid int) *process.Process {
	c.mu.Lock()
	defer c.mu.Unlock()
	if p := c.processes[int32(pid)]; p != nil {
		return p
	}
	p, err := process.NewProcess(int32(pid))
	if err != nil {
		return nil
	}
	c.processes[int32(pid)] = p
	return p
}

func listeningPorts(connections []net.ConnectionStat) []string {
	type portKey struct {
		protocol string
		port     uint32
	}
	ports := make(map[portKey]struct{})
	for _, connection := range connections {
		protocol := ""
		switch connection.Type {
		case syscall.SOCK_STREAM:
			if connection.Status != "LISTEN" {
				continue
			}
			protocol = "tcp"
		case syscall.SOCK_DGRAM:
			protocol = "udp"
		default:
			continue
		}
		if connection.Laddr.Port == 0 {
			continue
		}
		ports[portKey{protocol: protocol, port: connection.Laddr.Port}] = struct{}{}
	}
	keys := make([]portKey, 0, len(ports))
	for port := range ports {
		keys = append(keys, port)
	}
	sort.Slice(keys, func(i, j int) bool {
		if keys[i].port == keys[j].port {
			return keys[i].protocol < keys[j].protocol
		}
		return keys[i].port < keys[j].port
	})
	result := make([]string, 0, len(keys))
	for _, port := range keys {
		result = append(result, port.protocol+":"+strconv.FormatUint(uint64(port.port), 10))
	}
	return result
}

func (c *Collector) Forget(pid int) {
	c.mu.Lock()
	delete(c.processes, int32(pid))
	c.mu.Unlock()
}
