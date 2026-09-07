package scheduler

import (
	"context"
	"errors"
	"sort"
	"sync"
	"time"
)

// HistoryQuery describes the filters shared by the history IPC methods and
// the interactive history browser. Name is the target name, or the task name
// when TargetType is task.
type HistoryQuery struct {
	Tail        int
	TriggerType string
	Trigger     string
	TargetType  string
	Project     string
	Name        string
}

type ScheduleRef struct {
	Project string
	Name    string
}

// HistoryRepository is the persistence boundary for completed executions.
// Implementations must keep Record atomic, including retention and counters.
type HistoryRepository interface {
	Record(context.Context, Record, int) error
	Query(context.Context, HistoryQuery) ([]Record, error)
	Counters(context.Context) (map[string]uint64, error)
	LatestSchedules(context.Context, []ScheduleRef) (map[string]Record, error)
	Close() error
}

type historyPruner interface {
	Prune(context.Context, int) error
}

type historyClearer interface {
	Clear(context.Context) error
}

type memoryHistoryRepository struct {
	mu         sync.Mutex
	records    []Record
	counters   map[string]uint64
	executions map[string]Execution
}

func newMemoryHistoryRepository() HistoryRepository {
	return &memoryHistoryRepository{counters: make(map[string]uint64), executions: make(map[string]Execution)}
}

func (r *memoryHistoryRepository) Record(_ context.Context, record Record, limit int) error {
	r.mu.Lock()
	defer r.mu.Unlock()
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
			updated := cloneRecord(record)
			if !IsTerminalStatus(previousStatus) || !IsTerminalStatus(record.Status) {
				if len(updated.Attempts) == 0 {
					updated.Attempts = append([]Attempt(nil), previousRecord.Attempts...)
				}
				if len(updated.Tasks) == 0 {
					updated.Tasks = append([]TaskRecord(nil), previousRecord.Tasks...)
				}
			}
			if IsTerminalStatus(record.Status) && !IsTerminalStatus(previousStatus) {
				updated.Attempts = append(append([]Attempt(nil), previousRecord.Attempts...), updated.Attempts...)
				updated.Tasks = append(append([]TaskRecord(nil), previousRecord.Tasks...), updated.Tasks...)
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
	r.pruneLocked(limit)
	return nil
}

func (r *memoryHistoryRepository) Clear(_ context.Context) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.records = nil
	r.counters = make(map[string]uint64)
	r.executions = make(map[string]Execution)
	return nil
}

func (r *memoryHistoryRepository) Prune(_ context.Context, limit int) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.pruneLocked(limit)
	return nil
}

func (r *memoryHistoryRepository) pruneLocked(limit int) {
	if limit > 0 && len(r.records) > limit {
		r.records = append([]Record(nil), r.records[len(r.records)-limit:]...)
	}
}

func (r *memoryHistoryRepository) Query(_ context.Context, query HistoryQuery) ([]Record, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	result := make([]Record, 0, len(r.records))
	for _, record := range r.records {
		if !matchesHistoryQuery(record, query) {
			continue
		}
		if query.TargetType == "task" && query.Name != "" && record.TargetType == "workflow" {
			record.Tasks = matchingTasks(record.Tasks, query.Name)
		}
		result = append(result, cloneRecord(record))
	}
	if query.Tail > 0 && query.Tail < len(result) {
		result = result[len(result)-query.Tail:]
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

func (*memoryHistoryRepository) RecordExecutionEvent(context.Context, ExecutionEvent) error {
	return nil
}

func (*memoryHistoryRepository) RecordExecutionOperation(_ context.Context, operation ExecutionOperation) (ExecutionOperation, error) {
	if operation.RequestedAt.IsZero() {
		operation.RequestedAt = time.Now()
	}
	return operation, nil
}

func cloneExecution(execution Execution) Execution {
	execution.Record = cloneRecord(execution.Record)
	return execution
}

func matchesHistoryQuery(record Record, query HistoryQuery) bool {
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
		return record.TargetType == "workflow" && (query.Name == "" || record.Target == query.Name)
	case "task":
		if record.TargetType == "task" {
			return query.Name == "" || record.Target == query.Name
		}
		if record.TargetType != "workflow" {
			return false
		}
		for _, task := range record.Tasks {
			if query.Name == "" || task.Task == query.Name {
				return true
			}
		}
		return false
	default:
		if query.Name != "" && record.Target != query.Name && !recordContainsTask(record, query.Name) {
			return false
		}
		return true
	}
}

func matchesExecutionQuery(execution Execution, query ExecutionQuery) bool {
	record := execution.Record
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
	}
	return result
}
