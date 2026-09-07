package scheduler

import (
	"context"
	"errors"
	"fmt"
	"os"
	"sort"
	"sync"
	"time"

	"github.com/kevin93203/mango/internal/config"
	"github.com/robfig/cron/v3"
)

const defaultHistoryLimit = 0

type Record struct {
	RunID                   string
	Project                 string
	Name                    string
	TargetType              string
	Target                  string
	Trigger                 TriggerRef
	Status                  string
	Started                 time.Time
	Finished                time.Time
	ExitCode                int
	Error                   string
	Stderr                  string
	StdoutPath              string
	StderrPath              string
	IdempotencyKey          string
	ConfigurationGeneration uint64
	Attempts                []Attempt
	Tasks                   []TaskRecord
}

// TaskRecord is the execution record for one direct task invocation or one
// workflow node. Node is the workflow node name; Task is the task definition
// referenced by that node.
type TaskRecord struct {
	RunID       string
	ParentRunID string
	Node        string
	Task        string
	// Command, Args, and WorkingDir describe the resolved task invocation.
	Command    string
	Args       []string
	WorkingDir string
	// EnvKeys contains explicitly configured environment names; values are
	// intentionally never persisted in execution history.
	EnvKeys []string
	// ArgsRedacted indicates that command metadata contains redacted values.
	ArgsRedacted    bool
	Status          string
	Started         time.Time
	Finished        time.Time
	DurationSeconds float64
	ExitCode        int
	Error           string
	Stderr          string
	StdoutPath      string
	StderrPath      string
	Attempts        []Attempt
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
	ExitCode   int
	Err        error
	Stderr     string
	StdoutPath string
	StderrPath string
	// Record is set by the workflow executor for unified execution history.
	// When nil, Scheduler builds the single-process record from the result.
	Record *Record
}

// Execution is the durable control-plane representation of a logical run.
// Record contains the execution payload; the remaining fields describe its
// lifecycle and request identity.
type Execution struct {
	Record                  Record
	IdempotencyKey          string
	ConfigurationGeneration uint64
	CreatedAt               time.Time
	UpdatedAt               time.Time
}

type ExecutionEvent struct {
	ID        int64
	RunID     string
	Type      string
	Status    string
	Details   string
	CreatedAt time.Time
}

// ExecutionQuery describes filters for listing logical executions. An empty
// field means that the corresponding dimension is not filtered. Limit zero
// means that the store should return all matching executions.
type ExecutionQuery struct {
	Status      string
	TriggerType string
	Trigger     string
	Project     string
	TargetType  string
	Target      string
	Limit       int
}

type ExecutionOperation struct {
	ID          int64
	RunID       string
	Type        string
	Status      string
	Error       string
	RequestedAt time.Time
	CompletedAt time.Time
}

var ErrExecutionNotFound = errors.New("execution not found")

// ExecutionStore is the metadata persistence boundary for active and
// completed logical runs. HistoryRepository implementations may also
// implement this interface; the scheduler uses it opportunistically so old
// embedders remain source-compatible.
type ExecutionStore interface {
	BeginExecution(context.Context, Record, string, uint64) (Execution, bool, error)
	GetExecution(context.Context, string) (Execution, error)
	ListActiveExecutions(context.Context) ([]Execution, error)
	UpdateExecution(context.Context, Record) error
	RecordExecutionEvent(context.Context, ExecutionEvent) error
	RecordExecutionOperation(context.Context, ExecutionOperation) (ExecutionOperation, error)
}

// ExecutionLister is an optional extension for stores that support querying
// all persisted executions. It is separate from ExecutionStore so existing
// store implementations remain source-compatible.
type ExecutionLister interface {
	ListExecutions(context.Context, ExecutionQuery) ([]Execution, error)
}

const (
	StatusIdle        = "idle"
	StatusRunning     = "running"
	StatusSuccess     = "success"
	StatusFailed      = "failed"
	StatusSkipped     = "skipped"
	StatusCancelled   = "cancelled"
	StatusQueued      = "queued"
	StatusInterrupted = "interrupted"
	StatusDisabled    = "disabled"
)

func IsTerminalStatus(status string) bool {
	switch status {
	case StatusSuccess, StatusFailed, StatusSkipped, StatusCancelled, StatusInterrupted:
		return true
	default:
		return false
	}
}

// ScheduleSnapshot combines a configured schedule with its current execution
// state and the next cron activation time.
type ScheduleSnapshot struct {
	Schedule        config.EffectiveSchedule
	Runs            uint64
	Status          string
	Disabled        bool
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
	disabled        map[string]bool
	running         map[string]int
	active          map[string]map[uint64]time.Time
	activeCancels   map[string]context.CancelFunc
	nextExecutionID uint64
	historyRepo     HistoryRepository
	historyLimit    int
	executionWG     sync.WaitGroup
	stopping        bool
	started         bool
	runner          Runner
	ctx             context.Context
}

func New(runner Runner) *Scheduler {
	parser := cron.NewParser(cron.Minute | cron.Hour | cron.Dom | cron.Month | cron.Dow | cron.Descriptor)
	return &Scheduler{
		cron:          cron.New(cron.WithParser(parser)),
		entries:       map[string]cron.EntryID{},
		schedules:     map[string]config.EffectiveSchedule{},
		disabled:      map[string]bool{},
		running:       map[string]int{},
		active:        map[string]map[uint64]time.Time{},
		activeCancels: map[string]context.CancelFunc{},
		historyLimit:  defaultHistoryLimit,
		historyRepo:   newMemoryHistoryRepository(),
		runner:        runner,
		ctx:           context.Background(),
	}
}

// SetHistoryRepository replaces the in-memory fallback with the daemon's
// durable history store.
func (s *Scheduler) SetHistoryRepository(repo HistoryRepository) error {
	if repo == nil {
		repo = newMemoryHistoryRepository()
	}
	s.mu.Lock()
	limit := s.historyLimit
	s.mu.Unlock()
	if pruner, ok := repo.(historyPruner); ok {
		if err := pruner.Prune(context.Background(), limit); err != nil {
			return err
		}
	}
	s.mu.Lock()
	s.historyRepo = repo
	s.mu.Unlock()
	return nil
}

func (s *Scheduler) SetHistoryLimit(limit int) error {
	if limit < 0 {
		return fmt.Errorf("execution history limit must be non-negative")
	}
	s.mu.Lock()
	s.historyLimit = limit
	repo := s.historyRepo
	s.mu.Unlock()
	if pruner, ok := repo.(historyPruner); ok {
		return pruner.Prune(context.Background(), limit)
	}
	return nil
}

// RefreshHistorySummary validates that the history repository can read the
// configured schedule summaries. History summaries are read on demand and
// are not copied into scheduler memory.
func (s *Scheduler) RefreshHistorySummary(ctx context.Context) error {
	s.mu.Lock()
	repo := s.historyRepo
	refs := make([]ScheduleRef, 0, len(s.schedules))
	for _, schedule := range s.schedules {
		refs = append(refs, ScheduleRef{Project: schedule.Project, Name: schedule.Name})
	}
	s.mu.Unlock()
	_, err := repo.LatestSchedules(ctx, refs)
	return err
}

func (s *Scheduler) QueryHistory(ctx context.Context, query HistoryQuery) ([]Record, error) {
	if query.Tail < 0 {
		return nil, fmt.Errorf("history tail must be non-negative")
	}
	s.mu.Lock()
	repo := s.historyRepo
	s.mu.Unlock()
	return repo.Query(ctx, query)
}

// RunCount returns the lifetime number of logical executions for a target.
func (s *Scheduler) RunCount(targetType, project, target string) uint64 {
	counters, err := s.HistoryCounters(context.Background())
	if err != nil {
		return 0
	}
	return RunCountFromCounters(counters, targetType, project, target)
}

// TriggerRunCount returns the lifetime number of logical executions caused by
// a named trigger.
func (s *Scheduler) TriggerRunCount(project string, trigger TriggerRef) uint64 {
	counters, err := s.HistoryCounters(context.Background())
	if err != nil {
		return 0
	}
	return counters[counterKey("trigger:"+trigger.Type, project, trigger.Name)]
}

func (s *Scheduler) RunCounts() map[string]uint64 {
	counters, err := s.HistoryCounters(context.Background())
	if err != nil {
		return map[string]uint64{}
	}
	return counters
}

// HistoryCounters reads persisted lifetime counters without changing
// scheduler state.
func (s *Scheduler) HistoryCounters(ctx context.Context) (map[string]uint64, error) {
	s.mu.Lock()
	repo := s.historyRepo
	s.mu.Unlock()
	return repo.Counters(ctx)
}

// RunCountFromCounters returns a target count from a batch loaded by
// HistoryCounters.
func RunCountFromCounters(counters map[string]uint64, targetType, project, target string) uint64 {
	return counters[counterKey(targetType, project, target)]
}

// ClearHistory removes persisted execution history without changing the
// scheduler's runtime state.
func (s *Scheduler) ClearHistory(ctx context.Context) error {
	s.mu.Lock()
	repo := s.historyRepo
	s.mu.Unlock()
	clearer, ok := repo.(historyClearer)
	if !ok {
		return errors.New("history repository does not support clearing")
	}
	return clearer.Clear(ctx)
}

func (s *Scheduler) record(record Record) {
	if record.RunID == "" {
		record.RunID = NewRunID()
	}
	s.mu.Lock()
	repo := s.historyRepo
	limit := s.historyLimit
	s.mu.Unlock()
	if err := repo.Record(context.Background(), record, limit); err != nil {
		_, _ = fmt.Fprintf(os.Stderr, "warning: write execution history: %v\n", err)
	}
}

// RecordExecution persists a completed workflow or task execution in the
// unified history store. Manual executions use the same retention and
// persistence as cron-triggered executions.
func (s *Scheduler) RecordExecution(record Record) {
	if record.Status == "" {
		record.Status = statusForResult(record.ExitCode, record.Error)
	}
	s.record(record)
}

func (s *Scheduler) executionStore() ExecutionStore {
	s.mu.Lock()
	defer s.mu.Unlock()
	store, _ := s.historyRepo.(ExecutionStore)
	return store
}

func (s *Scheduler) BeginExecution(ctx context.Context, record Record, idempotencyKey string, configurationGeneration uint64) (Execution, bool, error) {
	store := s.executionStore()
	if store == nil {
		if record.RunID == "" {
			record.RunID = NewRunID()
		}
		record.IdempotencyKey = idempotencyKey
		record.ConfigurationGeneration = configurationGeneration
		now := time.Now()
		return Execution{Record: record, IdempotencyKey: idempotencyKey, ConfigurationGeneration: configurationGeneration, CreatedAt: now, UpdatedAt: now}, true, nil
	}
	return store.BeginExecution(ctx, record, idempotencyKey, configurationGeneration)
}

func (s *Scheduler) GetExecution(ctx context.Context, runID string) (Execution, error) {
	store := s.executionStore()
	if store == nil {
		return Execution{}, ErrExecutionNotFound
	}
	return store.GetExecution(ctx, runID)
}

func (s *Scheduler) ListExecutions(ctx context.Context, query ExecutionQuery) ([]Execution, error) {
	store := s.executionStore()
	if store == nil {
		return []Execution{}, nil
	}
	lister, ok := store.(ExecutionLister)
	if !ok {
		return []Execution{}, nil
	}
	return lister.ListExecutions(ctx, query)
}

func (s *Scheduler) ListActiveExecutions(ctx context.Context) ([]Execution, error) {
	store := s.executionStore()
	if store == nil {
		return []Execution{}, nil
	}
	return store.ListActiveExecutions(ctx)
}

func (s *Scheduler) UpdateExecution(ctx context.Context, record Record) error {
	store := s.executionStore()
	if store == nil {
		return nil
	}
	return store.UpdateExecution(ctx, record)
}

func (s *Scheduler) RecordExecutionEvent(ctx context.Context, event ExecutionEvent) error {
	store := s.executionStore()
	if store == nil {
		return nil
	}
	return store.RecordExecutionEvent(ctx, event)
}

func (s *Scheduler) RecordExecutionOperation(ctx context.Context, operation ExecutionOperation) (ExecutionOperation, error) {
	store := s.executionStore()
	if store == nil {
		return operation, nil
	}
	return store.RecordExecutionOperation(ctx, operation)
}

// CancelExecution cancels a scheduler-owned execution. Manual executions are
// cancelled by the daemon because their context is owned by the workflow
// executor rather than the cron scheduler.
func (s *Scheduler) CancelExecution(runID string) bool {
	s.mu.Lock()
	cancel := s.activeCancels[runID]
	s.mu.Unlock()
	if cancel == nil {
		return false
	}
	cancel()
	return true
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
	configured := make(map[string]config.EffectiveSchedule, len(schedules))
	disabled := make(map[string]bool)
	for _, schedule := range schedules {
		key := schedule.Project + "/" + schedule.Name
		configured[key] = schedule
		if s.disabled[key] {
			disabled[key] = true
			continue
		}
		if _, err := s.addEntryLocked(key, schedule); err != nil {
			return err
		}
	}
	s.schedules = configured
	s.disabled = disabled
	return nil
}

// SetDisabled replaces the scheduler's disabled schedule set. It is used at
// daemon startup to apply persisted schedule state before configuration is
// loaded.
func (s *Scheduler) SetDisabled(keys []string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.disabled = make(map[string]bool, len(keys))
	for _, key := range keys {
		if key != "" {
			s.disabled[key] = true
		}
	}
	for key, id := range s.entries {
		if s.disabled[key] {
			s.cron.Remove(id)
			delete(s.entries, key)
		}
	}
}

// Disable prevents future cron activations for key. Any execution already in
// progress is left untouched.
func (s *Scheduler) Disable(key string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if _, ok := s.schedules[key]; !ok {
		return fmt.Errorf("schedule %s not found", key)
	}
	s.disabled[key] = true
	s.removeEntryLocked(key)
	return nil
}

// Enable restores future cron activations for key.
func (s *Scheduler) Enable(key string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	schedule, ok := s.schedules[key]
	if !ok {
		return fmt.Errorf("schedule %s not found", key)
	}
	if !s.disabled[key] {
		return nil
	}
	if _, err := s.addEntryLocked(key, schedule); err != nil {
		return err
	}
	delete(s.disabled, key)
	return nil
}

func (s *Scheduler) addEntryLocked(key string, schedule config.EffectiveSchedule) (cron.EntryID, error) {
	loc := schedule.Timezone
	if loc == nil {
		loc = time.Local
	}
	spec := "CRON_TZ=" + loc.String() + " " + schedule.Cron
	entry, err := s.cron.AddFunc(spec, func() {
		s.run(schedule)
	})
	if err != nil {
		return 0, fmt.Errorf("schedule %s: %w", key, err)
	}
	s.entries[key] = entry
	return entry, nil
}

func (s *Scheduler) removeEntryLocked(key string) {
	if id, ok := s.entries[key]; ok {
		s.cron.Remove(id)
		delete(s.entries, key)
	}
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
	// v3 concurrency is owned by the target workflow/task. Keep the old
	// schedule-level check for package-level compatibility with pre-v3 callers.
	if schedule.TargetType == "" && schedule.Concurrency == "forbid" && s.running[key] > 0 {
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
	runID := NewRunID()
	occurrenceID := fmt.Sprintf("%s:%d", key, started.UnixNano())
	trigger := ScheduleTrigger(schedule.Name)
	trigger.EventID = occurrenceID
	queuedRecord := Record{
		RunID: runID, Project: schedule.Project, Name: schedule.Name,
		TargetType: schedule.TargetType, Target: schedule.Target, Trigger: trigger,
		Status: StatusQueued, Started: started, ConfigurationGeneration: schedule.ConfigurationGeneration,
	}
	if _, _, err := s.BeginExecution(ctx, queuedRecord, "", schedule.ConfigurationGeneration); err != nil {
		_, _ = fmt.Fprintf(os.Stderr, "warning: create execution metadata: %v\n", err)
		s.mu.Lock()
		s.running[key]--
		delete(s.active[key], executionID)
		if len(s.active[key]) == 0 {
			delete(s.active, key)
		}
		s.mu.Unlock()
		return
	}
	ctx, cancel := context.WithCancel(ctx)
	s.mu.Lock()
	s.activeCancels[runID] = cancel
	s.mu.Unlock()
	defer func() {
		cancel()
		s.mu.Lock()
		delete(s.activeCancels, runID)
		s.running[key]--
		delete(s.active[key], executionID)
		if len(s.active[key]) == 0 {
			delete(s.active, key)
		}
		s.mu.Unlock()
	}()
	schedule.RunID = runID
	schedule.OccurrenceID = occurrenceID
	runningRecord := queuedRecord
	runningRecord.Status = StatusRunning
	if err := s.UpdateExecution(ctx, runningRecord); err != nil {
		_, _ = fmt.Fprintf(os.Stderr, "warning: update execution metadata: %v\n", err)
		return
	}
	execution, attempts := s.executeWithRetry(ctx, schedule)
	if execution.Record != nil {
		record := *execution.Record
		record.RunID = runID
		if record.Project == "" {
			record.Project = schedule.Project
		}
		if record.Name == "" {
			record.Name = schedule.Name
		}
		if record.TargetType == "" {
			record.TargetType = schedule.TargetType
		}
		if record.Target == "" {
			record.Target = schedule.Target
		}
		// A scheduler-owned invocation is always a schedule trigger. The
		// executor receives the same value, but normalizing here keeps records
		// correct for custom runners as well.
		record.Trigger = trigger
		if record.Started.IsZero() {
			record.Started = started
		}
		if record.Finished.IsZero() {
			record.Finished = time.Now()
		}
		if record.Status == "" {
			record.Status = statusForResult(record.ExitCode, record.Error)
		}
		// Scheduler-level retries are logical execution attempts, even when
		// the workflow executor supplies the richer task/node record.
		record.Attempts = attempts
		s.record(record)
		return
	}
	recordStarted := started
	recordFinished := time.Now()
	if len(attempts) > 0 {
		recordStarted = attempts[0].Started
		recordFinished = attempts[len(attempts)-1].Finished
	}
	record := Record{
		RunID: runID, Project: schedule.Project, Name: schedule.Name, TargetType: schedule.TargetType,
		Target: schedule.Target, Trigger: trigger, Started: recordStarted, Finished: recordFinished,
		ConfigurationGeneration: schedule.ConfigurationGeneration,
		ExitCode:                execution.ExitCode, Stderr: execution.Stderr, StdoutPath: execution.StdoutPath, StderrPath: execution.StderrPath,
		Attempts: attempts,
	}
	if execution.Err != nil {
		record.Error = execution.Err.Error()
	}
	record.Status = statusForResult(record.ExitCode, record.Error)
	s.record(record)
}

func statusForResult(exitCode int, errorText string) string {
	if errorText != "" || exitCode != 0 {
		return StatusFailed
	}
	return StatusSuccess
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
	result, err := s.QueryHistory(context.Background(), HistoryQuery{})
	if err != nil {
		return []Record{}
	}
	return result
}

func (s *Scheduler) HistoryTail(limit int) []Record {
	result, err := s.QueryHistory(context.Background(), HistoryQuery{Tail: limit})
	if err != nil {
		return []Record{}
	}
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

// ListSnapshots returns configured schedules enriched with runtime state and
// completed execution details read from the history repository.
func (s *Scheduler) ListSnapshots(ctx context.Context) ([]ScheduleSnapshot, error) {
	s.mu.Lock()
	schedules := make(map[string]config.EffectiveSchedule, len(s.schedules))
	entryIDs := make(map[string]cron.EntryID, len(s.entries))
	disabled := make(map[string]bool, len(s.disabled))
	active := make(map[string][]time.Time, len(s.active))
	started := s.started
	repo := s.historyRepo
	refs := make([]ScheduleRef, 0, len(s.schedules))
	for key, schedule := range s.schedules {
		schedules[key] = schedule
		refs = append(refs, ScheduleRef{Project: schedule.Project, Name: schedule.Name})
	}
	for key, id := range s.entries {
		entryIDs[key] = id
	}
	for key, value := range s.disabled {
		disabled[key] = value
	}
	for key, executions := range s.active {
		for _, started := range executions {
			active[key] = append(active[key], started)
		}
	}
	s.mu.Unlock()

	counters, err := repo.Counters(ctx)
	if err != nil {
		return nil, err
	}
	latest, err := repo.LatestSchedules(ctx, refs)
	if err != nil {
		return nil, err
	}

	keys := make([]string, 0, len(schedules))
	for key := range schedules {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	result := make([]ScheduleSnapshot, 0, len(keys))
	for _, key := range keys {
		schedule := schedules[key]
		snapshot := ScheduleSnapshot{Schedule: schedule, Runs: counters[counterKey("trigger:"+TriggerSchedule, schedule.Project, schedule.Name)], Status: StatusIdle, Disabled: disabled[key]}

		if !snapshot.Disabled {
			entry := s.cron.Entry(entryIDs[key])
			next := entry.Next
			if next.IsZero() && started && entry.Schedule != nil {
				next = entry.Schedule.Next(time.Now())
			}
			if !next.IsZero() {
				snapshot.NextRun = timePtr(next)
			}
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
			if record.Status != "" {
				snapshot.Status = record.Status
			} else if record.Error != "" || record.ExitCode != 0 {
				snapshot.Status = StatusFailed
			} else {
				snapshot.Status = StatusSuccess
			}
		}
		if snapshot.Disabled {
			snapshot.Status = StatusDisabled
		}
		result = append(result, snapshot)
	}
	return result, nil
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
