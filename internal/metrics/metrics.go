package metrics

import (
	"sync"
	"time"

	"github.com/shirou/gopsutil/v3/mem"
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
	c.mu.Lock()
	p := c.processes[int32(pid)]
	if p == nil {
		p, _ = process.NewProcess(int32(pid))
		c.processes[int32(pid)] = p
	}
	c.mu.Unlock()
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

func (c *Collector) Forget(pid int) {
	c.mu.Lock()
	delete(c.processes, int32(pid))
	c.mu.Unlock()
}
