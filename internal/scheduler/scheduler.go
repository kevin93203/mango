package scheduler

import (
	"context"
	"errors"
	"fmt"
	"sort"
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
	Attempts []Attempt
}

type Attempt struct {
	Number          int
	Started         time.Time
	Finished        time.Time
	DurationSeconds float64
	ExitCode        int
	Error           string
	Stderr          string
}

type ExecutionResult struct {
	ExitCode int
	Err      error
	Stderr   string
}

const (
	StatusIdle    = "idle"
	StatusRunning = "running"
	StatusSuccess = "success"
	StatusFailed  = "failed"
)

// ScheduleSnapshot combines a configured schedule with its current execution
// state and the next cron activation time.
type ScheduleSnapshot struct {
	Schedule        config.EffectiveSchedule
	Status          string
	LastRun         *time.Time
	NextRun         *time.Time
	DurationSeconds *float64
}

type Runner func(context.Context, config.EffectiveSchedule) ExecutionResult

type Scheduler struct {
	mu              sync.Mutex
	cron            *cron.Cron
	entries         map[string]cron.EntryID
	schedules       map[string]config.EffectiveSchedule
	running         map[string]int
	active          map[string]map[uint64]time.Time
	nextExecutionID uint64
	history         []Record
	historyLimit    int
	historyPath     string
	persistMu       sync.Mutex
	executionWG     sync.WaitGroup
	stopping        bool
	started         bool
	runner          Runner
	ctx             context.Context
}

func New(runner Runner) *Scheduler {
	parser := cron.NewParser(cron.Minute | cron.Hour | cron.Dom | cron.Month | cron.Dow | cron.Descriptor)
	return &Scheduler{
		cron:         cron.New(cron.WithParser(parser)),
		entries:      map[string]cron.EntryID{},
		schedules:    map[string]config.EffectiveSchedule{},
		running:      map[string]int{},
		active:       map[string]map[uint64]time.Time{},
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
	s.started = true
	s.mu.Unlock()
	s.cron.Start()
}

func (s *Scheduler) Stop() context.Context {
	s.mu.Lock()
	s.stopping = true
	s.started = false
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
	s.nextExecutionID++
	executionID := s.nextExecutionID
	started := time.Now()
	if s.active[key] == nil {
		s.active[key] = map[uint64]time.Time{}
	}
	s.active[key][executionID] = started
	s.mu.Unlock()
	defer func() {
		s.mu.Lock()
		s.running[key]--
		delete(s.active[key], executionID)
		if len(s.active[key]) == 0 {
			delete(s.active, key)
		}
		s.mu.Unlock()
	}()
	execution, attempts := s.executeWithRetry(ctx, schedule)
	recordStarted := started
	recordFinished := time.Now()
	if len(attempts) > 0 {
		recordStarted = attempts[0].Started
		recordFinished = attempts[len(attempts)-1].Finished
	}
	record := Record{
		Project: schedule.Project, Name: schedule.Name, Started: recordStarted, Finished: recordFinished,
		ExitCode: execution.ExitCode, Stderr: execution.Stderr, Attempts: attempts,
	}
	if execution.Err != nil {
		record.Error = execution.Err.Error()
	}
	s.record(record)
}

func (s *Scheduler) executeWithRetry(ctx context.Context, schedule config.EffectiveSchedule) (ExecutionResult, []Attempt) {
	retries := schedule.RetryCount
	if retries < 0 {
		retries = 0
	}
	var result ExecutionResult
	attempts := make([]Attempt, 0, 1)
	for attemptNumber := 1; ; attemptNumber++ {
		if attemptNumber > 1 && !waitForRetry(ctx, schedule.RetryDelay) {
			return result, attempts
		}
		attemptStarted := time.Now()
		result = s.runner(ctx, schedule)
		attemptFinished := time.Now()
		attempt := Attempt{
			Number:          attemptNumber,
			Started:         attemptStarted,
			Finished:        attemptFinished,
			DurationSeconds: attemptFinished.Sub(attemptStarted).Seconds(),
			ExitCode:        result.ExitCode,
			Stderr:          result.Stderr,
		}
		if attempt.DurationSeconds < 0 {
			attempt.DurationSeconds = 0
		}
		if result.Err != nil {
			attempt.Error = result.Err.Error()
		}
		attempts = append(attempts, attempt)
		if result.Err == nil && result.ExitCode == 0 {
			return result, attempts
		}
		if ctx.Err() != nil || attemptNumber > retries {
			return result, attempts
		}
	}
}

func waitForRetry(ctx context.Context, delay time.Duration) bool {
	if delay <= 0 {
		select {
		case <-ctx.Done():
			return false
		default:
			return true
		}
	}
	timer := time.NewTimer(delay)
	defer timer.Stop()
	select {
	case <-timer.C:
		return true
	case <-ctx.Done():
		return false
	}
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

// ListSnapshots returns configured schedules enriched with execution and cron
// timing information. Runtime execution state is intentionally kept in memory;
// completed execution details come from the persisted history.
func (s *Scheduler) ListSnapshots() []ScheduleSnapshot {
	s.mu.Lock()
	schedules := make(map[string]config.EffectiveSchedule, len(s.schedules))
	entryIDs := make(map[string]cron.EntryID, len(s.entries))
	active := make(map[string][]time.Time, len(s.active))
	started := s.started
	for key, schedule := range s.schedules {
		schedules[key] = schedule
	}
	for key, id := range s.entries {
		entryIDs[key] = id
	}
	for key, executions := range s.active {
		for _, started := range executions {
			active[key] = append(active[key], started)
		}
	}
	history := append([]Record(nil), s.history...)
	s.mu.Unlock()

	latest := make(map[string]Record)
	for _, record := range history {
		key := record.Project + "/" + record.Name
		previous, ok := latest[key]
		if !ok || record.Started.After(previous.Started) {
			latest[key] = record
		}
	}

	keys := make([]string, 0, len(schedules))
	for key := range schedules {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	result := make([]ScheduleSnapshot, 0, len(keys))
	for _, key := range keys {
		schedule := schedules[key]
		snapshot := ScheduleSnapshot{Schedule: schedule, Status: StatusIdle}

		entry := s.cron.Entry(entryIDs[key])
		next := entry.Next
		if next.IsZero() && started && entry.Schedule != nil {
			next = entry.Schedule.Next(time.Now())
		}
		if !next.IsZero() {
			snapshot.NextRun = timePtr(next)
		}

		if starts := active[key]; len(starts) > 0 {
			started := latestStart(starts)
			snapshot.Status = StatusRunning
			snapshot.LastRun = timePtr(started)
			duration := time.Since(started).Seconds()
			if duration < 0 {
				duration = 0
			}
			snapshot.DurationSeconds = floatPtr(duration)
		} else if record, ok := latest[key]; ok {
			snapshot.LastRun = timePtr(record.Started)
			snapshot.DurationSeconds = recordDuration(record)
			if record.Error != "" || record.ExitCode != 0 {
				snapshot.Status = StatusFailed
			} else {
				snapshot.Status = StatusSuccess
			}
		}
		result = append(result, snapshot)
	}
	return result
}

func latestStart(starts []time.Time) time.Time {
	latest := starts[0]
	for _, started := range starts[1:] {
		if started.After(latest) {
			latest = started
		}
	}
	return latest
}

func recordDuration(record Record) *float64 {
	if record.Started.IsZero() || record.Finished.IsZero() {
		return nil
	}
	duration := record.Finished.Sub(record.Started).Seconds()
	if duration < 0 {
		duration = 0
	}
	return floatPtr(duration)
}

func timePtr(value time.Time) *time.Time {
	return &value
}

func floatPtr(value float64) *float64 {
	return &value
}

func (s *Scheduler) Has(key string) bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	_, ok := s.schedules[key]
	return ok
}
