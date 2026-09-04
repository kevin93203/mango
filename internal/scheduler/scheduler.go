package scheduler

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/kevin93203/mango/internal/config"
	"github.com/robfig/cron/v3"
)

type Record struct {
	Project  string
	Name     string
	Started  time.Time
	Finished time.Time
	ExitCode int
	Error    string
	Stderr   string
}

type ExecutionResult struct {
	ExitCode int
	Err      error
	Stderr   string
}

type Runner func(context.Context, config.EffectiveSchedule) ExecutionResult

type Scheduler struct {
	mu           sync.Mutex
	cron         *cron.Cron
	entries      map[string]cron.EntryID
	schedules    map[string]config.EffectiveSchedule
	running      map[string]int
	history      []Record
	historyLimit int
	historyPath  string
	persistMu    sync.Mutex
	executionWG  sync.WaitGroup
	stopping     bool
	runner       Runner
	ctx          context.Context
}

func New(runner Runner) *Scheduler {
	parser := cron.NewParser(cron.Minute | cron.Hour | cron.Dom | cron.Month | cron.Dow | cron.Descriptor)
	return &Scheduler{
		cron:         cron.New(cron.WithParser(parser)),
		entries:      map[string]cron.EntryID{},
		schedules:    map[string]config.EffectiveSchedule{},
		running:      map[string]int{},
		historyLimit: defaultHistoryLimit,
		runner:       runner,
		ctx:          context.Background(),
	}
}

func (s *Scheduler) SetContext(ctx context.Context) {
	s.mu.Lock()
	s.ctx = ctx
	s.mu.Unlock()
}

func (s *Scheduler) Start() {
	s.mu.Lock()
	s.stopping = false
	s.mu.Unlock()
	s.cron.Start()
}

func (s *Scheduler) Stop() context.Context {
	s.mu.Lock()
	s.stopping = true
	s.mu.Unlock()
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
	stopping := s.stopping
	if stopping {
		s.mu.Unlock()
		return errors.New("scheduler is stopping")
	}
	if !ok {
		s.mu.Unlock()
		return fmt.Errorf("schedule %s not found", key)
	}
	s.executionWG.Add(1)
	s.mu.Unlock()
	go func() {
		defer s.executionWG.Done()
		s.execute(ctx, schedule)
	}()
	return nil
}

func (s *Scheduler) Wait() {
	s.executionWG.Wait()
}

func (s *Scheduler) run(schedule config.EffectiveSchedule) {
	s.mu.Lock()
	if s.stopping {
		s.mu.Unlock()
		return
	}
	ctx := s.ctx
	s.executionWG.Add(1)
	s.mu.Unlock()
	defer s.executionWG.Done()
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
	execution := s.runner(ctx, schedule)
	record := Record{
		Project: schedule.Project, Name: schedule.Name, Started: started, Finished: time.Now(),
		ExitCode: execution.ExitCode, Stderr: execution.Stderr,
	}
	if execution.Err != nil {
		record.Error = execution.Err.Error()
	}
	s.record(record)
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
