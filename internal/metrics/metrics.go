package metrics

import (
	"context"
	"sort"
	"strconv"
	"strings"
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

type ProcessSnapshot struct {
	PID           int
	ParentPID     int
	Depth         int
	Name          string
	CommandLine   string
	State         string // Deprecated alias for OSState.
	OSState       string
	CPUPercent    float64
	RSSBytes      uint64
	MemoryPercent float64
	Ports         []string
	Children      []ProcessSnapshot
}

type Collector struct {
	mu        sync.Mutex
	processes map[int32]*process.Process

	portsMu         sync.Mutex
	ports           map[int32]portCacheEntry
	portsInflight   map[int32]struct{}
	portsGeneration map[int32]uint64
}

const (
	portCacheTTL      = 5 * time.Second
	portLookupTimeout = 2 * time.Second
)

type portCacheEntry struct {
	ports     []string
	updatedAt time.Time
}

func NewCollector() *Collector {
	return &Collector{
		processes:       map[int32]*process.Process{},
		ports:           map[int32]portCacheEntry{},
		portsInflight:   map[int32]struct{}{},
		portsGeneration: map[int32]uint64{},
	}
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

// Snapshot returns a best-effort snapshot of a process and all its descendants.
// This also covers commands such as "go run" where the managed PID is a
// wrapper around the actual server process.
func (c *Collector) Snapshot(pid int, interval time.Duration) (ProcessSnapshot, bool) {
	if pid <= 0 {
		return ProcessSnapshot{}, false
	}
	root := c.getProcess(pid)
	if root == nil {
		return ProcessSnapshot{}, false
	}
	totalMemory := uint64(0)
	if vm, err := mem.VirtualMemory(); err == nil && vm != nil {
		totalMemory = vm.Total
	}
	nodes := c.processTree(root)
	snapshots := make(map[int32]ProcessSnapshot, len(nodes))
	for _, node := range nodes {
		snapshot, ok := c.snapshotProcess(node.process, node.parentPID, node.depth, totalMemory)
		if !ok {
			if node.pid != root.Pid {
				continue
			}
			snapshot = ProcessSnapshot{PID: int(root.Pid)}
		}
		snapshots[node.pid] = snapshot
	}
	rootSnapshot, ok := buildSnapshotTree(root.Pid, nodes, snapshots)
	if !ok {
		return ProcessSnapshot{PID: int(root.Pid)}, true
	}
	return rootSnapshot, true
}

// Ports returns the listening TCP and bound UDP ports for a single process.
// Network inspection is best-effort because operating-system permissions may
// prevent reading another process's connections.
func (c *Collector) Ports(pid int) []string {
	if pid <= 0 {
		return nil
	}
	p := c.getProcess(pid)
	if p == nil {
		return nil
	}
	return c.portsForProcess(p)
}

type treeNode struct {
	process   *process.Process
	pid       int32
	parentPID int
	depth     int
}

func (c *Collector) processTree(root *process.Process) []treeNode {
	result := make([]treeNode, 0, 1)
	queue := []treeNode{{process: root, pid: root.Pid}}
	seen := map[int32]struct{}{root.Pid: {}}
	for len(queue) > 0 {
		current := queue[0]
		queue = queue[1:]
		result = append(result, current)

		children, err := current.process.Children()
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
			cached := c.getProcess(int(child.Pid))
			if cached == nil {
				continue
			}
			queue = append(queue, treeNode{
				process: cached, pid: cached.Pid, parentPID: int(current.process.Pid), depth: current.depth + 1,
			})
		}
	}
	return result
}

func buildSnapshotTree(rootPID int32, nodes []treeNode, snapshots map[int32]ProcessSnapshot) (ProcessSnapshot, bool) {
	root, ok := snapshots[rootPID]
	if !ok {
		return ProcessSnapshot{}, false
	}
	for index := len(nodes) - 1; index >= 0; index-- {
		node := nodes[index]
		if node.pid == rootPID {
			continue
		}
		child, childOK := snapshots[node.pid]
		parent, parentOK := snapshots[int32(node.parentPID)]
		if !childOK || !parentOK {
			continue
		}
		parent.Children = append(parent.Children, child)
		snapshots[int32(node.parentPID)] = parent
	}
	root = snapshots[rootPID]
	sortSnapshotChildren(&root)
	return root, true
}

func (c *Collector) snapshotProcess(p *process.Process, parentPID, depth int, totalMemory uint64) (ProcessSnapshot, bool) {
	name, nameErr := p.Name()
	commandLine, commandLineErr := p.Cmdline()
	status, statusErr := p.Status()
	if nameErr != nil && commandLineErr != nil && statusErr != nil {
		return ProcessSnapshot{}, false
	}
	cpu, _ := p.Percent(0)
	info, _ := p.MemoryInfo()
	var rss uint64
	if info != nil {
		rss = info.RSS
	}
	var memoryPct float64
	if totalMemory > 0 {
		memoryPct = float64(rss) / float64(totalMemory) * 100
	}
	state := "unknown"
	if len(status) > 0 && strings.TrimSpace(status[0]) != "" {
		state = status[0]
	}
	return ProcessSnapshot{
		PID:           int(p.Pid),
		ParentPID:     parentPID,
		Depth:         depth,
		Name:          name,
		CommandLine:   commandLine,
		State:         state,
		OSState:       state,
		CPUPercent:    cpu,
		RSSBytes:      rss,
		MemoryPercent: memoryPct,
		Ports:         c.portsForProcess(p),
	}, true
}

func (c *Collector) portsForProcess(p *process.Process) []string {
	if p == nil || p.Pid <= 0 {
		return nil
	}

	pid := p.Pid
	now := time.Now()
	c.portsMu.Lock()
	entry, cached := c.ports[pid]
	if cached && now.Sub(entry.updatedAt) < portCacheTTL {
		ports := append([]string(nil), entry.ports...)
		c.portsMu.Unlock()
		return ports
	}

	// Process connection inspection can be surprisingly slow on macOS and
	// Windows (and may block when permissions are restricted). Never make a
	// service status request wait for it. Each PID is queried independently so
	// a wrapper PID cannot starve descendants that own ports.
	if _, running := c.portsInflight[pid]; !running {
		generation := c.portsGeneration[pid]
		c.portsInflight[pid] = struct{}{}
		go c.refreshPorts(pid, p, generation)
	}
	ports := append([]string(nil), entry.ports...)
	c.portsMu.Unlock()
	return ports
}

func (c *Collector) refreshPorts(pid int32, p *process.Process, generation uint64) {
	ctx, cancel := context.WithTimeout(context.Background(), portLookupTimeout)
	defer cancel()
	connections, err := p.ConnectionsWithContext(ctx)
	ports := []string(nil)
	if err == nil {
		ports = listeningPorts(connections)
	}

	c.portsMu.Lock()
	delete(c.portsInflight, pid)
	if c.portsGeneration[pid] == generation {
		c.ports[pid] = portCacheEntry{ports: ports, updatedAt: time.Now()}
	}
	c.portsMu.Unlock()
}

func sortSnapshotChildren(snapshot *ProcessSnapshot) {
	for index := range snapshot.Children {
		sortSnapshotChildren(&snapshot.Children[index])
	}
	sort.Slice(snapshot.Children, func(i, j int) bool {
		return snapshot.Children[i].PID < snapshot.Children[j].PID
	})
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
	c.portsMu.Lock()
	c.portsGeneration[int32(pid)]++
	delete(c.ports, int32(pid))
	c.portsMu.Unlock()
}
