package scheduler

import (
	"context"
	"errors"
	"reflect"
	"sort"
	"sync"
	"time"
)

// HistoryQuery describes the filters shared by the history IPC methods and
// the interactive history browser. Name is the target name, or the task name
// when TargetType is task.
type HistoryQuery struct {
	// Limit is the maximum number of terminal records to return. Results are
	// always newest-first. Tail is retained only for source compatibility with
	// pre-major embedders and is treated as Limit when Limit is zero.
	Limit       int
	Tail        int
	Status      string
	TriggerType string
	Trigger     string
	TargetType  string
	Project     string
	Target      string
	Name        string // legacy alias for Target
	Attempts    bool
}

type ScheduleRef struct {
	Project string
	Name    string
}

// HistoryReader is the narrow read boundary for terminal history and lifetime
// summaries. It never owns active execution transitions.
type HistoryReader interface {
	Query(context.Context, HistoryQuery) ([]Record, error)
	Counters(context.Context) (map[string]uint64, error)
	LatestSchedules(context.Context, []ScheduleRef) (map[string]Record, error)
}

// HistoryRepository is the compatibility composite accepted by Scheduler.
// New code should depend on ExecutionStore, HistoryReader, and HistoryPurger
// separately.
type HistoryRepository interface {
	ExecutionStore
	HistoryReader
	Close() error
}

type historyPruner interface {
	Prune(context.Context, int) error
}

type historyClearer interface {
	Clear(context.Context) error
}

type memoryHistoryRepository struct {
	mu                sync.Mutex
	records           []Record
	counters          map[string]uint64
	executions        map[string]Execution
	events            map[string][]ExecutionEvent
	occurrences       map[string]ScheduleOccurrence
	webhookDeliveries map[string]WebhookDelivery
}

func newMemoryHistoryRepository() HistoryRepository {
	return &memoryHistoryRepository{
		counters:          make(map[string]uint64),
		executions:        make(map[string]Execution),
		events:            make(map[string][]ExecutionEvent),
		occurrences:       make(map[string]ScheduleOccurrence),
		webhookDeliveries: make(map[string]WebhookDelivery),
	}
}

func (r *memoryHistoryRepository) ClaimWebhookDelivery(_ context.Context, delivery WebhookDelivery) (WebhookDelivery, bool, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if delivery.WebhookKey == "" || delivery.IdempotencyKey == "" {
		return WebhookDelivery{}, false, errors.New("webhook delivery identity is required")
	}
	key := delivery.WebhookKey + "\x00" + delivery.IdempotencyKey
	if existing, ok := r.webhookDeliveries[key]; ok {
		if existing.BodySHA256 != delivery.BodySHA256 || existing.BodySize != delivery.BodySize {
			return WebhookDelivery{}, false, ErrWebhookDeliveryConflict
		}
		return existing, false, nil
	}
	if delivery.CreatedAt.IsZero() {
		delivery.CreatedAt = time.Now().UTC()
	}
	r.webhookDeliveries[key] = delivery
	return delivery, true, nil
}

func (r *memoryHistoryRepository) ClaimScheduleOccurrence(_ context.Context, occurrence ScheduleOccurrence) (ScheduleOccurrence, bool, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if occurrence.ID == "" {
		return ScheduleOccurrence{}, false, errors.New("schedule occurrence id is required")
	}
	if existing, ok := r.occurrences[occurrence.ID]; ok {
		return existing, false, nil
	}
	if occurrence.Status == "" {
		occurrence.Status = OccurrencePending
	}
	now := time.Now().UTC()
	if occurrence.CreatedAt.IsZero() {
		occurrence.CreatedAt = now
	}
	occurrence.UpdatedAt = now
	r.occurrences[occurrence.ID] = occurrence
	return occurrence, true, nil
}

func (r *memoryHistoryRepository) GetScheduleOccurrence(_ context.Context, id string) (ScheduleOccurrence, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	occurrence, ok := r.occurrences[id]
	if !ok {
		return ScheduleOccurrence{}, ErrScheduleOccurrenceNotFound
	}
	return occurrence, nil
}

func (r *memoryHistoryRepository) LatestScheduleOccurrence(_ context.Context, project, schedule string) (ScheduleOccurrence, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	var latest ScheduleOccurrence
	found := false
	for _, occurrence := range r.occurrences {
		if occurrence.Project != project || occurrence.Schedule != schedule {
			continue
		}
		if !found || occurrence.ScheduledAt.After(latest.ScheduledAt) {
			latest = occurrence
			found = true
		}
	}
	if !found {
		return ScheduleOccurrence{}, ErrScheduleOccurrenceNotFound
	}
	return latest, nil
}

func (r *memoryHistoryRepository) UpdateScheduleOccurrence(_ context.Context, occurrence ScheduleOccurrence) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	previous, ok := r.occurrences[occurrence.ID]
	if !ok {
		return ErrScheduleOccurrenceNotFound
	}
	if occurrence.CreatedAt.IsZero() {
		occurrence.CreatedAt = previous.CreatedAt
	}
	occurrence.UpdatedAt = time.Now().UTC()
	r.occurrences[occurrence.ID] = occurrence
	return nil
}

func (r *memoryHistoryRepository) Record(_ context.Context, record Record, limit int) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	if record.Status == "" {
		record.Status = statusForCompletedRecord(record)
	}
	if record.RunID == "" {
		record.RunID = NewRunID()
	}
	created := true
	previousStatus := ""
	var previousRecord Record
	for index := range r.records {
		if record.RunID != "" && r.records[index].RunID == record.RunID {
			created = false
			previousRecord = cloneRecord(r.records[index])
			previousStatus = previousRecord.Status
			if IsTerminalStatus(previousStatus) {
				if record.IdempotencyKey == "" {
					record.IdempotencyKey = previousRecord.IdempotencyKey
				}
				if record.ConfigurationGeneration == 0 {
					record.ConfigurationGeneration = previousRecord.ConfigurationGeneration
				}
				if record.RetriedFromRunID == "" {
					record.RetriedFromRunID = previousRecord.RetriedFromRunID
				}
				if !SameTerminalRecord(previousRecord, record) {
					if IsActiveStatus(record.Status) {
						return ErrExecutionTransition
					}
					return ErrTerminalExecutionImmutable
				}
				return nil
			}
			updated := cloneRecord(record)
			if len(updated.Attempts) == 0 {
				updated.Attempts = append([]Attempt(nil), previousRecord.Attempts...)
			}
			if len(updated.Tasks) == 0 {
				updated.Tasks = append([]TaskRecord(nil), previousRecord.Tasks...)
			}
			r.records[index] = updated
			record = updated
			break
		}
	}
	if created {
		r.records = append(r.records, cloneRecord(record))
	}
	if created || (!IsTerminalStatus(previousStatus) && IsTerminalStatus(record.Status)) {
		for _, key := range counterKeys(record) {
			r.counters[key]++
		}
	}
	now := time.Now()
	previous := r.executions[record.RunID]
	if record.IdempotencyKey == "" {
		record.IdempotencyKey = previous.IdempotencyKey
	}
	if record.ConfigurationGeneration == 0 {
		record.ConfigurationGeneration = previous.ConfigurationGeneration
	}
	r.executions[record.RunID] = Execution{Record: cloneRecord(record), IdempotencyKey: record.IdempotencyKey, ConfigurationGeneration: record.ConfigurationGeneration, CreatedAt: previous.CreatedAt, UpdatedAt: now}
	if r.executions[record.RunID].CreatedAt.IsZero() {
		r.executions[record.RunID] = Execution{Record: cloneRecord(record), IdempotencyKey: record.IdempotencyKey, ConfigurationGeneration: record.ConfigurationGeneration, CreatedAt: now, UpdatedAt: now}
	}
	if created || previousStatus != record.Status {
		r.appendEventLocked(ExecutionEvent{RunID: record.RunID, Type: "state_transition", Status: record.Status})
	}
	r.pruneLocked(limit)
	return nil
}

func statusForCompletedRecord(record Record) string {
	if record.Error != "" || record.ExitCode != 0 {
		return StatusFailed
	}
	return StatusSuccess
}

func (r *memoryHistoryRepository) Clear(_ context.Context) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.records = nil
	r.counters = make(map[string]uint64)
	r.executions = make(map[string]Execution)
	r.events = make(map[string][]ExecutionEvent)
	r.webhookDeliveries = make(map[string]WebhookDelivery)
	return nil
}

func (r *memoryHistoryRepository) Prune(_ context.Context, limit int) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.pruneLocked(limit)
	return nil
}

func (r *memoryHistoryRepository) pruneLocked(limit int) {
	if limit <= 0 {
		return
	}
	terminal := make([]int, 0)
	for index, record := range r.records {
		if IsTerminalStatus(record.Status) {
			terminal = append(terminal, index)
		}
	}
	if len(terminal) <= limit {
		return
	}
	remove := make(map[int]bool, len(terminal)-limit)
	for _, index := range terminal[:len(terminal)-limit] {
		remove[index] = true
		runID := r.records[index].RunID
		delete(r.executions, runID)
		for key, delivery := range r.webhookDeliveries {
			if delivery.RunID == runID {
				delete(r.webhookDeliveries, key)
			}
		}
	}
	kept := make([]Record, 0, len(r.records)-len(remove))
	for index, record := range r.records {
		if !remove[index] {
			kept = append(kept, record)
		}
	}
	r.records = kept
}

func (r *memoryHistoryRepository) Query(_ context.Context, query HistoryQuery) ([]Record, error) {
	limit := query.Limit
	if limit == 0 {
		limit = query.Tail
	}
	if limit < 0 {
		return nil, errors.New("history limit must be non-negative")
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	result := make([]Record, 0, len(r.records))
	position := make(map[string]int, len(r.records))
	for index, record := range r.records {
		position[record.RunID] = index
	}
	for _, record := range r.records {
		if !IsTerminalStatus(record.Status) {
			continue
		}
		if !matchesHistoryQuery(record, query) {
			continue
		}
		name := query.Target
		if name == "" {
			name = query.Name
		}
		if query.TargetType == "task" && name != "" && record.TargetType == "workflow" {
			record.Tasks = matchingTasks(record.Tasks, name)
		}
		result = append(result, cloneRecord(record))
	}
	sort.SliceStable(result, func(i, j int) bool {
		if result[i].Started.Equal(result[j].Started) {
			return position[result[i].RunID] > position[result[j].RunID]
		}
		return result[i].Started.After(result[j].Started)
	})
	if limit > 0 && limit < len(result) {
		result = result[:limit]
	}
	return result, nil
}

func (r *memoryHistoryRepository) Counters(_ context.Context) (map[string]uint64, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	return copyCounts(r.counters), nil
}

func (r *memoryHistoryRepository) LatestSchedules(_ context.Context, schedules []ScheduleRef) (map[string]Record, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	result := make(map[string]Record, len(schedules))
	for _, schedule := range schedules {
		key := schedule.Project + "/" + schedule.Name
		for _, record := range r.records {
			matches := record.Project == schedule.Project &&
				((record.Trigger.Type == TriggerSchedule && record.Trigger.Name == schedule.Name) ||
					(record.Trigger.IsZero() && record.Name == schedule.Name))
			if !matches {
				continue
			}
			previous, ok := result[key]
			if !ok || !record.Started.Before(previous.Started) {
				result[key] = cloneRecord(record)
			}
		}
	}
	return result, nil
}

func (*memoryHistoryRepository) Close() error { return nil }

func (r *memoryHistoryRepository) BeginExecution(_ context.Context, record Record, idempotencyKey string, configurationGeneration uint64) (Execution, bool, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	for _, execution := range r.executions {
		if idempotencyKey != "" && execution.IdempotencyKey == idempotencyKey {
			return cloneExecution(execution), false, nil
		}
	}
	if record.RunID == "" {
		record.RunID = NewRunID()
	}
	now := time.Now()
	if record.Status == "" {
		record.Status = StatusQueued
	}
	record.IdempotencyKey = idempotencyKey
	record.ConfigurationGeneration = configurationGeneration
	execution := Execution{Record: cloneRecord(record), IdempotencyKey: idempotencyKey, ConfigurationGeneration: configurationGeneration, CreatedAt: now, UpdatedAt: now}
	r.executions[record.RunID] = execution
	r.records = append(r.records, cloneRecord(record))
	r.appendEventLocked(ExecutionEvent{RunID: record.RunID, Type: "created", Status: record.Status})
	return cloneExecution(execution), true, nil
}

func (r *memoryHistoryRepository) GetExecution(_ context.Context, runID string) (Execution, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	execution, ok := r.executions[runID]
	if !ok {
		for _, record := range r.records {
			if record.RunID == runID {
				return Execution{Record: cloneRecord(record)}, nil
			}
		}
		return Execution{}, ErrExecutionNotFound
	}
	return cloneExecution(execution), nil
}

func (r *memoryHistoryRepository) ListActiveExecutions(_ context.Context) ([]Execution, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	result := make([]Execution, 0)
	for _, execution := range r.executions {
		if execution.Record.Status == StatusQueued || execution.Record.Status == StatusRunning {
			result = append(result, cloneExecution(execution))
		}
	}
	return result, nil
}

func (r *memoryHistoryRepository) ListExecutions(_ context.Context, query ExecutionQuery) ([]Execution, error) {
	if query.Limit < 0 {
		return nil, errors.New("execution list limit must be non-negative")
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	result := make([]Execution, 0, len(r.executions))
	for _, execution := range r.executions {
		if !matchesExecutionQuery(execution, query) {
			continue
		}
		result = append(result, cloneExecution(execution))
	}
	sort.SliceStable(result, func(i, j int) bool {
		if result[i].Record.Started.Equal(result[j].Record.Started) {
			return result[i].Record.RunID > result[j].Record.RunID
		}
		return result[i].Record.Started.After(result[j].Record.Started)
	})
	if query.Limit > 0 && query.Limit < len(result) {
		result = result[:query.Limit]
	}
	return result, nil
}

func (r *memoryHistoryRepository) UpdateExecution(ctx context.Context, record Record) error {
	return r.Record(ctx, record, 0)
}

func (r *memoryHistoryRepository) Purge(_ context.Context, before *time.Time, all bool) (int, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if !all && before == nil {
		return 0, errors.New("history purge requires before or all")
	}
	removed := 0
	kept := make([]Record, 0, len(r.records))
	for _, record := range r.records {
		purge := IsTerminalStatus(record.Status) && (all || record.Finished.Before(*before))
		if purge {
			delete(r.executions, record.RunID)
			for key, delivery := range r.webhookDeliveries {
				if delivery.RunID == record.RunID {
					delete(r.webhookDeliveries, key)
				}
			}
			delete(r.events, record.RunID)
			removed++
			continue
		}
		kept = append(kept, record)
	}
	r.records = kept
	return removed, nil
}

func (r *memoryHistoryRepository) RecordExecutionEvent(_ context.Context, event ExecutionEvent) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.appendEventLocked(event)
	return nil
}

func (r *memoryHistoryRepository) ListExecutionEvents(_ context.Context, runID string) ([]ExecutionEvent, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	return append([]ExecutionEvent(nil), r.events[runID]...), nil
}

func (r *memoryHistoryRepository) appendEventLocked(event ExecutionEvent) {
	if event.CreatedAt.IsZero() {
		event.CreatedAt = time.Now().UTC()
	}
	event.ID = int64(len(r.events[event.RunID]) + 1)
	r.events[event.RunID] = append(r.events[event.RunID], event)
}

func cloneExecution(execution Execution) Execution {
	execution.Record = cloneRecord(execution.Record)
	return execution
}

func matchesHistoryQuery(record Record, query HistoryQuery) bool {
	if query.Status != "" && record.Status != query.Status {
		return false
	}
	if query.TriggerType != "" && record.Trigger.Type != query.TriggerType {
		return false
	}
	if query.Trigger != "" && record.Trigger.Name != query.Trigger {
		return false
	}
	if query.Project != "" && record.Project != query.Project {
		return false
	}
	switch query.TargetType {
	case "workflow":
		name := query.Target
		if name == "" {
			name = query.Name
		}
		return record.TargetType == "workflow" && (name == "" || record.Target == name)
	case "task":
		name := query.Target
		if name == "" {
			name = query.Name
		}
		if record.TargetType == "task" {
			return name == "" || record.Target == name
		}
		if record.TargetType != "workflow" {
			return false
		}
		for _, task := range record.Tasks {
			if name == "" || task.Task == name {
				return true
			}
		}
		return false
	default:
		name := query.Target
		if name == "" {
			name = query.Name
		}
		if name != "" && record.Target != name && !recordContainsTask(record, name) {
			return false
		}
		return true
	}
}

func matchesExecutionQuery(execution Execution, query ExecutionQuery) bool {
	record := execution.Record
	if !query.All && query.Status == "" && !IsActiveStatus(record.Status) {
		return false
	}
	if query.Status != "" && record.Status != query.Status {
		return false
	}
	if query.TriggerType != "" && record.Trigger.Type != query.TriggerType {
		return false
	}
	if query.Trigger != "" && record.Trigger.Name != query.Trigger {
		return false
	}
	if query.Project != "" && record.Project != query.Project {
		return false
	}
	if query.TargetType != "" && record.TargetType != query.TargetType {
		return false
	}
	return query.Target == "" || record.Target == query.Target
}

func IsActiveStatus(status string) bool {
	return status == StatusQueued || status == StatusRunning
}

// SameTerminalRecord reports whether two terminal writes describe the same
// canonical execution payload. Callers may omit child rows when repeating an
// otherwise identical terminal write; supplied child rows must match.
func SameTerminalRecord(left, right Record) bool {
	if !IsTerminalStatus(left.Status) || !IsTerminalStatus(right.Status) || left.Status != right.Status {
		return false
	}
	if len(right.Attempts) > 0 && !reflect.DeepEqual(left.Attempts, right.Attempts) {
		return false
	}
	if len(right.Tasks) > 0 && !reflect.DeepEqual(left.Tasks, right.Tasks) {
		return false
	}
	left.Attempts = nil
	left.Tasks = nil
	right.Attempts = nil
	right.Tasks = nil
	return left.RunID == right.RunID && left.Project == right.Project && left.Name == right.Name &&
		left.TargetType == right.TargetType && left.Target == right.Target && left.Trigger == right.Trigger &&
		left.Started.Equal(right.Started) && left.Finished.Equal(right.Finished) && left.ExitCode == right.ExitCode &&
		left.Error == right.Error && left.Stderr == right.Stderr && left.StdoutPath == right.StdoutPath &&
		left.StderrPath == right.StderrPath && left.IdempotencyKey == right.IdempotencyKey &&
		left.ConfigurationGeneration == right.ConfigurationGeneration && left.RetriedFromRunID == right.RetriedFromRunID
}

func recordContainsTask(record Record, name string) bool {
	if record.TargetType == "task" {
		return name == "" || record.Target == name
	}
	if record.TargetType != "workflow" {
		return false
	}
	for _, task := range record.Tasks {
		if name == "" || task.Task == name {
			return true
		}
	}
	return false
}

func matchingTasks(tasks []TaskRecord, name string) []TaskRecord {
	result := make([]TaskRecord, 0, len(tasks))
	for _, task := range tasks {
		if task.Task == name {
			result = append(result, task)
		}
	}
	sort.SliceStable(result, func(i, j int) bool { return result[i].Started.Before(result[j].Started) })
	return result
}

func cloneRecord(record Record) Record {
	result := record
	result.Attempts = append([]Attempt(nil), record.Attempts...)
	result.Tasks = make([]TaskRecord, len(record.Tasks))
	for index, task := range record.Tasks {
		result.Tasks[index] = task
		result.Tasks[index].Args = append([]string(nil), task.Args...)
		result.Tasks[index].EnvKeys = append([]string(nil), task.EnvKeys...)
		result.Tasks[index].Attempts = append([]Attempt(nil), task.Attempts...)
		result.Tasks[index].Artifacts = append([]Artifact(nil), task.Artifacts...)
	}
	return result
}
