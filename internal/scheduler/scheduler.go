package scheduler

import (
	"context"
	"encoding/json"
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

const (
	OccurrencePending    = "pending"
	OccurrenceDispatched = "dispatched"
)

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
	// RetriedFromRunID links a manually-created execution to the terminal
	// execution it was retried from. Automatic retries remain attempts on the
	// same execution and do not set this field.
	RetriedFromRunID string
	Attempts         []Attempt
	Tasks            []TaskRecord
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
	ArgsRedacted bool
	Status       string
	// SkipReason preserves the internal reason for a public skipped status.
	// In particular, upstream_failed remains distinguishable without changing
	// the existing status vocabulary exposed by older clients.
	SkipReason      string
	Timeout         time.Duration
	RetryCount      int
	RetryDelay      time.Duration
	AllowFailure    bool
	PolicyResolved  bool
	Started         time.Time
	Finished        time.Time
	DurationSeconds float64
	ExitCode        int
	Error           string
	Stderr          string
	StdoutPath      string
	StderrPath      string
	Attempts        []Attempt
	Artifacts       []Artifact
}

// Artifact is metadata for a declared task output. Mango never stores the
// file contents in execution history.
type Artifact struct {
	Path   string
	Exists bool
	Size   int64
	SHA256 string
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
	Artifacts  []Artifact
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
	// All disables the active-only default used by execution ls.
	All bool
}

var ErrExecutionNotFound = errors.New("execution not found")
var ErrScheduleOccurrenceNotFound = errors.New("schedule occurrence not found")

// ErrTerminalExecutionImmutable is returned when a terminal execution is
// written with a different terminal payload. Terminal records are canonical
// and may only receive an identical, idempotent write.
var ErrTerminalExecutionImmutable = errors.New("terminal execution is immutable")

// ErrExecutionTransition is returned when a write attempts an invalid
// lifecycle transition, such as terminal -> queued/running.
var ErrExecutionTransition = errors.New("invalid execution state transition")

// ExecutionStore is the metadata persistence boundary for active and
// completed logical runs. It owns lifecycle writes, execution reads, and
// logical-run listing; terminal history queries remain on HistoryReader.
type ExecutionStore interface {
	Record(context.Context, Record, int) error
	BeginExecution(context.Context, Record, string, uint64) (Execution, bool, error)
	GetExecution(context.Context, string) (Execution, error)
	ListActiveExecutions(context.Context) ([]Execution, error)
	ListExecutions(context.Context, ExecutionQuery) ([]Execution, error)
	UpdateExecution(context.Context, Record) error
	RecordExecutionEvent(context.Context, ExecutionEvent) error
}

// ExecutionEventReader is an optional detail-read boundary for history.get.
// Keeping it separate preserves compatibility with older embedders that
// implement only the execution lifecycle store.
type ExecutionEventReader interface {
	ListExecutionEvents(context.Context, string) ([]ExecutionEvent, error)
}

// HistoryPurger is the narrow destructive history boundary. It removes only
// terminal execution data; active execution metadata and lifetime counters are
// outside its scope.
type HistoryPurger interface {
	Purge(context.Context, *time.Time, bool) (int, error)
}

type ScheduleOccurrence struct {
	ID          string
	Project     string
	Schedule    string
	ScheduledAt time.Time
	Status      string
	RunID       string
	Misfire     string
	CreatedAt   time.Time
	UpdatedAt   time.Time
}

type ScheduleOccurrenceStore interface {
	ClaimScheduleOccurrence(context.Context, ScheduleOccurrence) (ScheduleOccurrence, bool, error)
	GetScheduleOccurrence(context.Context, string) (ScheduleOccurrence, error)
	LatestScheduleOccurrence(context.Context, string, string) (ScheduleOccurrence, error)
	UpdateScheduleOccurrence(context.Context, ScheduleOccurrence) error
}

type WebhookDelivery struct {
	WebhookKey     string
	IdempotencyKey string
	BodySHA256     string
	BodySize       int64
	RunID          string
	CreatedAt      time.Time
}

type WebhookDeliveryStore interface {
	ClaimWebhookDelivery(context.Context, WebhookDelivery) (WebhookDelivery, bool, error)
}

var ErrWebhookDeliveryConflict = errors.New("webhook idempotency key was reused with a different body")

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
	mu                   sync.Mutex
	cron                 *cron.Cron
	entries              map[string]cron.EntryID
	schedules            map[string]config.EffectiveSchedule
	disabled             map[string]bool
	running              map[string]int
	active               map[string]map[uint64]time.Time
	activeCancels        map[string]context.CancelFunc
	nextExecutionID      uint64
	executionStoreRepo   ExecutionStore
	historyReader        HistoryReader
	historyPurger        HistoryPurger
	historyPruner        historyPruner
	historyClearer       historyClearer
	occurrenceStore      ScheduleOccurrenceStore
	webhookDeliveryStore WebhookDeliveryStore
	historyLimit         int
	executionWG          sync.WaitGroup
	stopping             bool
	started              bool
	runner               Runner
	ctx                  context.Context
	clock                func() time.Time
	parser               cron.Parser
}

func New(runner Runner) *Scheduler {
	parser := cron.NewParser(cron.Minute | cron.Hour | cron.Dom | cron.Month | cron.Dow | cron.Descriptor)
	repo := newMemoryHistoryRepository()
	return &Scheduler{
		cron:                 cron.New(cron.WithParser(parser)),
		parser:               parser,
		entries:              map[string]cron.EntryID{},
		schedules:            map[string]config.EffectiveSchedule{},
		disabled:             map[string]bool{},
		running:              map[string]int{},
		active:               map[string]map[uint64]time.Time{},
		activeCancels:        map[string]context.CancelFunc{},
		historyLimit:         defaultHistoryLimit,
		executionStoreRepo:   repo,
		historyReader:        repo,
		historyPurger:        repo.(HistoryPurger),
		historyPruner:        repo.(historyPruner),
		historyClearer:       repo.(historyClearer),
		occurrenceStore:      repo.(ScheduleOccurrenceStore),
		webhookDeliveryStore: repo.(WebhookDeliveryStore),
		runner:               runner,
		ctx:                  context.Background(),
		clock:                time.Now,
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
	s.executionStoreRepo = repo
	s.historyReader = repo
	s.historyPurger, _ = repo.(HistoryPurger)
	s.historyPruner, _ = repo.(historyPruner)
	s.historyClearer, _ = repo.(historyClearer)
	s.occurrenceStore, _ = repo.(ScheduleOccurrenceStore)
	s.webhookDeliveryStore, _ = repo.(WebhookDeliveryStore)
	s.mu.Unlock()
	return nil
}

func (s *Scheduler) SetHistoryLimit(limit int) error {
	if limit < 0 {
		return fmt.Errorf("execution history limit must be non-negative")
	}
	s.mu.Lock()
	s.historyLimit = limit
	pruner := s.historyPruner
	s.mu.Unlock()
	if pruner != nil {
		return pruner.Prune(context.Background(), limit)
	}
	return nil
}

// RefreshHistorySummary validates that the history repository can read the
// configured schedule summaries. History summaries are read on demand and
// are not copied into scheduler memory.
func (s *Scheduler) RefreshHistorySummary(ctx context.Context) error {
	s.mu.Lock()
	reader := s.historyReader
	refs := make([]ScheduleRef, 0, len(s.schedules))
	for _, schedule := range s.schedules {
		refs = append(refs, ScheduleRef{Project: schedule.Project, Name: schedule.Name})
	}
	s.mu.Unlock()
	_, err := reader.LatestSchedules(ctx, refs)
	return err
}

func (s *Scheduler) QueryHistory(ctx context.Context, query HistoryQuery) ([]Record, error) {
	if query.Limit < 0 || query.Tail < 0 {
		return nil, fmt.Errorf("history limit must be non-negative")
	}
	s.mu.Lock()
	reader := s.historyReader
	s.mu.Unlock()
	return reader.Query(ctx, query)
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
	reader := s.historyReader
	s.mu.Unlock()
	return reader.Counters(ctx)
}

// PurgeHistory removes terminal execution metadata through the dedicated
// history purger. Active execution rows, counters, and logs are unaffected.
func (s *Scheduler) PurgeHistory(ctx context.Context, before *time.Time, all bool) (int, error) {
	s.mu.Lock()
	purger := s.historyPurger
	s.mu.Unlock()
	if purger == nil {
		return 0, errors.New("history repository does not support purging")
	}
	return purger.Purge(ctx, before, all)
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
	clearer := s.historyClearer
	s.mu.Unlock()
	if clearer == nil {
		return errors.New("history repository does not support clearing")
	}
	return clearer.Clear(ctx)
}

func (s *Scheduler) record(record Record) {
	if record.RunID == "" {
		record.RunID = NewRunID()
	}
	s.mu.Lock()
	store := s.executionStoreRepo
	limit := s.historyLimit
	s.mu.Unlock()
	if err := store.Record(context.Background(), record, limit); err != nil {
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
	return s.executionStoreRepo
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

func (s *Scheduler) ClaimWebhookDelivery(ctx context.Context, delivery WebhookDelivery) (WebhookDelivery, bool, error) {
	s.mu.Lock()
	store := s.webhookDeliveryStore
	s.mu.Unlock()
	if store == nil {
		return delivery, true, nil
	}
	return store.ClaimWebhookDelivery(ctx, delivery)
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
	return store.ListExecutions(ctx, query)
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

func (s *Scheduler) ListExecutionEvents(ctx context.Context, runID string) ([]ExecutionEvent, error) {
	store := s.executionStore()
	reader, ok := store.(ExecutionEventReader)
	if !ok {
		return []ExecutionEvent{}, nil
	}
	return reader.ListExecutionEvents(ctx, runID)
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

// ReconcileOccurrences scans each enabled schedule once after startup. It is
// deliberately asynchronous so daemon startup is not blocked by a long-running
// catch-up execution.
func (s *Scheduler) ReconcileOccurrences(ctx context.Context) {
	s.mu.Lock()
	if s.stopping {
		s.mu.Unlock()
		return
	}
	schedules := make([]config.EffectiveSchedule, 0, len(s.schedules))
	for key, schedule := range s.schedules {
		if s.disabled[key] {
			continue
		}
		schedules = append(schedules, schedule)
	}
	s.mu.Unlock()
	sort.Slice(schedules, func(i, j int) bool {
		left := schedules[i].Project + "/" + schedules[i].Name
		right := schedules[j].Project + "/" + schedules[j].Name
		return left < right
	})
	for _, schedule := range schedules {
		s.executionWG.Add(1)
		go func(schedule config.EffectiveSchedule) {
			defer s.executionWG.Done()
			s.executeDueMode(ctx, schedule, s.now(), true)
		}(schedule)
	}
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
	// Build all new cron entries before touching the active set. A malformed
	// replacement must leave the currently working scheduler intact.
	configured := make(map[string]config.EffectiveSchedule, len(schedules))
	disabled := make(map[string]bool)
	ordered := append([]config.EffectiveSchedule(nil), schedules...)
	sort.SliceStable(ordered, func(i, j int) bool {
		left := ordered[i].Project + "/" + ordered[i].Name
		right := ordered[j].Project + "/" + ordered[j].Name
		return left < right
	})
	staged := make(map[string]cron.EntryID, len(ordered))
	for _, schedule := range ordered {
		key := schedule.Project + "/" + schedule.Name
		configured[key] = schedule
		if s.disabled[key] {
			disabled[key] = true
			continue
		}
		entry, err := s.addCronEntryLocked(key, schedule)
		if err != nil {
			for _, id := range staged {
				s.cron.Remove(id)
			}
			return err
		}
		staged[key] = entry
	}
	for _, id := range s.entries {
		s.cron.Remove(id)
	}
	s.entries = staged
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
	entry, err := s.addCronEntryLocked(key, schedule)
	if err != nil {
		return 0, err
	}
	s.entries[key] = entry
	return entry, nil
}

func (s *Scheduler) addCronEntryLocked(key string, schedule config.EffectiveSchedule) (cron.EntryID, error) {
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
	s.executeDue(ctx, schedule, s.now())
}

func (s *Scheduler) executeOccurrence(ctx context.Context, schedule config.EffectiveSchedule, scheduledAt time.Time) {
	store := s.occurrenceStoreSnapshot()
	occurrenceID := ScheduleOccurrenceID(schedule.Project, schedule.Name, scheduledAt)
	if store != nil {
		occurrence, created, err := store.ClaimScheduleOccurrence(ctx, ScheduleOccurrence{
			ID: occurrenceID, Project: schedule.Project, Schedule: schedule.Name,
			ScheduledAt: scheduledAt.UTC(), Status: OccurrencePending, Misfire: schedule.Misfire,
		})
		if err != nil {
			_, _ = fmt.Fprintf(os.Stderr, "warning: claim schedule occurrence %s: %v\n", occurrenceID, err)
			return
		}
		if !created && occurrence.Status != OccurrencePending {
			return
		}
	}
	schedule.OccurrenceID = occurrenceID
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
	automaticOccurrence := schedule.OccurrenceID != ""
	occurrenceID := schedule.OccurrenceID
	if occurrenceID == "" {
		occurrenceID = fmt.Sprintf("%s:%d", key, started.UnixNano())
	}
	trigger := ScheduleTrigger(schedule.Name)
	trigger.EventID = occurrenceID
	queuedRecord := Record{
		RunID: runID, Project: schedule.Project, Name: schedule.Name,
		TargetType: schedule.TargetType, Target: schedule.Target, Trigger: trigger,
		Status: StatusQueued, Started: started, IdempotencyKey: func() string {
			if automaticOccurrence {
				return ScheduleOccurrenceIdempotencyKey(occurrenceID)
			}
			return ""
		}(), ConfigurationGeneration: schedule.ConfigurationGeneration,
	}
	idempotencyKey := queuedRecord.IdempotencyKey
	queuedExecution, created, err := s.BeginExecution(ctx, queuedRecord, idempotencyKey, schedule.ConfigurationGeneration)
	if err != nil {
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
	if !created {
		runID = queuedExecution.Record.RunID
		if automaticOccurrence {
			s.updateOccurrence(ctx, occurrenceID, runID, queuedExecution.Record.Status)
		}
		return
	}
	if automaticOccurrence {
		details, _ := json.Marshal(map[string]string{
			"occurrence_id": occurrenceID, "misfire": schedule.Misfire,
		})
		_ = s.RecordExecutionEvent(ctx, ExecutionEvent{RunID: runID, Type: "schedule_occurrence", Status: OccurrenceDispatched, Details: string(details)})
		s.updateOccurrence(ctx, occurrenceID, runID, OccurrenceDispatched)
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
		if automaticOccurrence {
			s.updateOccurrence(context.Background(), occurrenceID, runID, record.Status)
		}
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
	if automaticOccurrence {
		s.updateOccurrence(context.Background(), occurrenceID, runID, record.Status)
	}
}

func (s *Scheduler) updateOccurrence(ctx context.Context, occurrenceID, runID, status string) {
	store := s.occurrenceStoreSnapshot()
	if store == nil {
		return
	}
	occurrence, err := store.GetScheduleOccurrence(ctx, occurrenceID)
	if err != nil {
		return
	}
	occurrence.RunID = runID
	occurrence.Status = status
	_ = store.UpdateScheduleOccurrence(ctx, occurrence)
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
		result = s.runner(WithAttemptNumber(ctx, attemptNumber), schedule)
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
	reader := s.historyReader
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

	counters, err := reader.Counters(ctx)
	if err != nil {
		return nil, err
	}
	latest, err := reader.LatestSchedules(ctx, refs)
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
