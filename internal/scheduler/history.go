package scheduler

import (
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
)

const (
	defaultHistoryLimit = 0
	historyVersion      = 2
)

type HistoryFile struct {
	Version  int               `json:"version"`
	Records  []Record          `json:"records"`
	Counters map[string]uint64 `json:"counters"`
}

func (s *Scheduler) LoadHistory(path string, limit int) error {
	if limit < 0 {
		return fmt.Errorf("execution history limit must be non-negative")
	}

	var (
		records        []Record
		counters       = make(map[string]uint64)
		loadErr        error
		needsMigration bool
	)
	data, err := os.ReadFile(path)
	if errors.Is(err, os.ErrNotExist) {
		// A missing state file is the normal first-run case.
	} else if err != nil {
		loadErr = fmt.Errorf("read execution history: %w", err)
	} else {
		var envelope struct {
			Version  int               `json:"version"`
			Records  []Record          `json:"records"`
			Counters map[string]uint64 `json:"counters"`
		}
		if err := json.Unmarshal(data, &envelope); err != nil {
			loadErr = fmt.Errorf("decode execution history: %w", err)
		} else if envelope.Version != 1 && envelope.Version != historyVersion {
			loadErr = fmt.Errorf("unsupported execution history version %d", envelope.Version)
		} else {
			records = envelope.Records
			for index := range records {
				if records[index].RunID == "" {
					records[index].RunID = NewRunID()
					needsMigration = true
				}
			}
			if envelope.Version == 1 {
				// Record's custom decoder normalizes legacy string triggers.
				counters = countersForRecords(records)
				needsMigration = true
			} else if envelope.Counters != nil {
				counters = copyCounts(envelope.Counters)
			} else {
				counters = countersForRecords(records)
			}
		}
	}
	loadedCount := len(records)
	records = trimHistory(records, limit)

	s.persistMu.Lock()
	defer s.persistMu.Unlock()
	s.mu.Lock()
	s.historyPath = path
	s.historyLimit = limit
	s.history = trimHistory(records, limit)
	s.counters = counters
	s.mu.Unlock()
	if loadErr == nil && (needsMigration || len(records) != loadedCount || !historyFileIsCurrent(data)) {
		if err := saveHistory(path, records, s.RunCounts()); err != nil {
			return fmt.Errorf("compact execution history: %w", err)
		}
	}
	return loadErr
}

func (s *Scheduler) record(record Record) {
	s.persistMu.Lock()
	defer s.persistMu.Unlock()

	s.mu.Lock()
	if record.RunID == "" {
		record.RunID = NewRunID()
	}
	s.history = append(s.history, record)
	s.history = trimHistory(s.history, s.historyLimit)
	if s.counters == nil {
		s.counters = make(map[string]uint64)
	}
	for _, key := range counterKeys(record) {
		s.counters[key]++
	}
	records := append([]Record(nil), s.history...)
	counters := copyCounts(s.counters)
	path := s.historyPath
	s.mu.Unlock()

	if path == "" {
		return
	}
	if err := saveHistory(path, records, counters); err != nil {
		_, _ = fmt.Fprintf(os.Stderr, "warning: write execution history: %v\n", err)
	}
}

func (s *Scheduler) HistoryTail(limit int) []Record {
	s.mu.Lock()
	defer s.mu.Unlock()
	if limit <= 0 || limit >= len(s.history) {
		return append([]Record{}, s.history...)
	}
	result := make([]Record, limit)
	copy(result, s.history[len(s.history)-limit:])
	return result
}

// RunCount returns the lifetime number of logical executions for a target.
func (s *Scheduler) RunCount(targetType, project, target string) uint64 {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.counters[counterKey(targetType, project, target)]
}

// TriggerRunCount returns the lifetime number of logical executions caused by
// a named trigger. Only named trigger types, such as schedules and webhooks,
// are counted here.
func (s *Scheduler) TriggerRunCount(project string, trigger TriggerRef) uint64 {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.counters[counterKey("trigger:"+trigger.Type, project, trigger.Name)]
}

func (s *Scheduler) RunCounts() map[string]uint64 {
	s.mu.Lock()
	defer s.mu.Unlock()
	return copyCounts(s.counters)
}

// RecordExecution persists a completed workflow or task execution in the
// scheduler's unified history store. Manual executions use the same retention
// and persistence as cron-triggered executions.
func (s *Scheduler) RecordExecution(record Record) {
	if record.Status == "" {
		record.Status = statusForResult(record.ExitCode, record.Error)
	}
	s.record(record)
}

func trimHistory(records []Record, limit int) []Record {
	if limit > 0 && len(records) > limit {
		records = records[len(records)-limit:]
	}
	return append([]Record(nil), records...)
}

func saveHistory(path string, records []Record, counterValues ...map[string]uint64) error {
	var counters map[string]uint64
	if len(counterValues) > 0 {
		counters = counterValues[0]
	}
	if counters == nil {
		counters = map[string]uint64{}
	}
	data, err := json.MarshalIndent(HistoryFile{Version: historyVersion, Records: records, Counters: counters}, "", "  ")
	if err != nil {
		return err
	}
	directory := filepath.Dir(path)
	if err := os.MkdirAll(directory, 0o700); err != nil {
		return err
	}
	temp, err := os.CreateTemp(directory, ".execution-history-*.tmp")
	if err != nil {
		return err
	}
	tempName := temp.Name()
	defer os.Remove(tempName)
	if err := temp.Chmod(0o600); err != nil {
		_ = temp.Close()
		return err
	}
	if _, err := temp.Write(append(data, '\n')); err != nil {
		_ = temp.Close()
		return err
	}
	if err := temp.Close(); err != nil {
		return err
	}
	return os.Rename(tempName, path)
}

func counterKey(kind, project, target string) string {
	return kind + "|" + project + "|" + target
}

func counterKeys(record Record) []string {
	keys := make([]string, 0, len(record.Tasks)+3)
	if record.TargetType != "" && record.Target != "" {
		keys = append(keys, counterKey(record.TargetType, record.Project, record.Target))
	}
	if record.Trigger.Type != "" && record.Trigger.Name != "" {
		keys = append(keys, counterKey("trigger:"+record.Trigger.Type, record.Project, record.Trigger.Name))
	}
	if record.TargetType == "workflow" {
		for _, task := range record.Tasks {
			if task.Task != "" {
				keys = append(keys, counterKey("task", record.Project, task.Task))
			}
		}
	}
	return keys
}

func countersForRecords(records []Record) map[string]uint64 {
	result := make(map[string]uint64)
	for _, record := range records {
		for _, key := range counterKeys(record) {
			result[key]++
		}
	}
	return result
}

func copyCounts(values map[string]uint64) map[string]uint64 {
	result := make(map[string]uint64, len(values))
	for key, value := range values {
		result[key] = value
	}
	return result
}

func historyFileIsCurrent(data []byte) bool {
	var envelope struct {
		Version  int             `json:"version"`
		Counters json.RawMessage `json:"counters"`
	}
	if json.Unmarshal(data, &envelope) != nil {
		return false
	}
	return envelope.Version == historyVersion && len(envelope.Counters) > 0 && string(envelope.Counters) != "null"
}
