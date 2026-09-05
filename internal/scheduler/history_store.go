package scheduler

import (
	"context"
	"sort"
	"sync"
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

type memoryHistoryRepository struct {
	mu       sync.Mutex
	records  []Record
	counters map[string]uint64
}

func newMemoryHistoryRepository() HistoryRepository {
	return &memoryHistoryRepository{counters: make(map[string]uint64)}
}

func (r *memoryHistoryRepository) Record(_ context.Context, record Record, limit int) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.records = append(r.records, cloneRecord(record))
	for _, key := range counterKeys(record) {
		r.counters[key]++
	}
	r.pruneLocked(limit)
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
