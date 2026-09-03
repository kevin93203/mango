package scheduler

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/robfig/cron/v3"
	"github.com/kevin93203/mango/internal/config"
)

type Record struct {
	Project  string
	Name     string
	Started  time.Time
	Finished time.Time
	ExitCode int
	Error    string
}

type Runner func(context.Context, config.EffectiveSchedule) (int, error)

type Scheduler struct {
	mu        sync.Mutex
	cron      *cron.Cron
	entries   map[string]cron.EntryID
	schedules map[string]config.EffectiveSchedule
	running   map[string]int
	history   []Record
	runner    Runner
	ctx       context.Context
}

func New(runner Runner) *Scheduler {
	parser := cron.NewParser(cron.Minute | cron.Hour | cron.Dom | cron.Month | cron.Dow | cron.Descriptor)
	return &Scheduler{
		cron:      cron.New(cron.WithParser(parser)),
		entries:   map[string]cron.EntryID{},
		schedules: map[string]config.EffectiveSchedule{},
		running:   map[string]int{},
		runner:    runner,
		ctx:       context.Background(),
	}
}

func (s *Scheduler) SetContext(ctx context.Context) {
	s.mu.Lock()
	s.ctx = ctx
	s.mu.Unlock()
}

func (s *Scheduler) Start() {
	s.cron.Start()
}

func (s *Scheduler) Stop() context.Context {
	return s.cron.Stop()
}

func (s *Scheduler) Apply(schedules []config.EffectiveSchedule) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	for _, id := range s.entries {
		s.cron.Remove(id)
	}
	s.entries = map[string]cron.EntryID{}
	s.schedules = map[string]config.EffectiveSchedule{}
	for _, schedule := range schedules {
		key := schedule.Project + "/" + schedule.Name
		loc := schedule.Timezone
		if loc == nil {
			loc = time.Local
		}
		spec := "CRON_TZ=" + loc.String() + " " + schedule.Cron
		entry, err := s.cron.AddFunc(spec, func() {
			s.run(schedule)
		})
		if err != nil {
			return fmt.Errorf("schedule %s: %w", key, err)
		}
		s.entries[key] = entry
		s.schedules[key] = schedule
	}
	return nil
}

func (s *Scheduler) RunNow(ctx context.Context, key string) error {
	s.mu.Lock()
	schedule, ok := s.schedules[key]
	s.mu.Unlock()
	if !ok {
		return fmt.Errorf("schedule %s not found", key)
	}
	go s.execute(ctx, schedule)
	return nil
}

func (s *Scheduler) run(schedule config.EffectiveSchedule) {
	s.mu.Lock()
	ctx := s.ctx
	s.mu.Unlock()
	s.execute(ctx, schedule)
}

func (s *Scheduler) execute(ctx context.Context, schedule config.EffectiveSchedule) {
	key := schedule.Project + "/" + schedule.Name
	s.mu.Lock()
	if schedule.Concurrency == "forbid" && s.running[key] > 0 {
		s.mu.Unlock()
		return
	}
	s.running[key]++
	s.mu.Unlock()
	defer func() {
		s.mu.Lock()
		s.running[key]--
		s.mu.Unlock()
	}()
	started := time.Now()
	exitCode, err := s.runner(ctx, schedule)
	record := Record{Project: schedule.Project, Name: schedule.Name, Started: started, Finished: time.Now(), ExitCode: exitCode}
	if err != nil {
		record.Error = err.Error()
	}
	s.mu.Lock()
	s.history = append(s.history, record)
	if len(s.history) > 100 {
		s.history = s.history[len(s.history)-100:]
	}
	s.mu.Unlock()
}

func (s *Scheduler) History() []Record {
	s.mu.Lock()
	defer s.mu.Unlock()
	result := make([]Record, len(s.history))
	copy(result, s.history)
	return result
}

func (s *Scheduler) List() []config.EffectiveSchedule {
	s.mu.Lock()
	defer s.mu.Unlock()
	result := make([]config.EffectiveSchedule, 0, len(s.schedules))
	for _, schedule := range s.schedules {
		result = append(result, schedule)
	}
	return result
}

func (s *Scheduler) Has(key string) bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	_, ok := s.schedules[key]
	return ok
}
