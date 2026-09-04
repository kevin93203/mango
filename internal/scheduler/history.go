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
	historyVersion      = 1
)

type HistoryFile struct {
	Version int      `json:"version"`
	Records []Record `json:"records"`
}

func (s *Scheduler) LoadHistory(path string, limit int) error {
	if limit < 0 {
		return fmt.Errorf("execution history limit must be non-negative")
	}

	var (
		records []Record
		loadErr error
	)
	data, err := os.ReadFile(path)
	if errors.Is(err, os.ErrNotExist) {
		// A missing state file is the normal first-run case.
	} else if err != nil {
		loadErr = fmt.Errorf("read execution history: %w", err)
	} else {
		var file HistoryFile
		if err := json.Unmarshal(data, &file); err != nil {
			loadErr = fmt.Errorf("decode execution history: %w", err)
		} else if file.Version != historyVersion {
			loadErr = fmt.Errorf("unsupported execution history version %d", file.Version)
		} else {
			records = file.Records
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
	s.mu.Unlock()
	if loadErr == nil && len(records) != loadedCount {
		if err := saveHistory(path, records); err != nil {
			return fmt.Errorf("compact execution history: %w", err)
		}
	}
	return loadErr
}

func (s *Scheduler) record(record Record) {
	s.persistMu.Lock()
	defer s.persistMu.Unlock()

	s.mu.Lock()
	s.history = append(s.history, record)
	s.history = trimHistory(s.history, s.historyLimit)
	records := append([]Record(nil), s.history...)
	path := s.historyPath
	s.mu.Unlock()

	if path == "" {
		return
	}
	if err := saveHistory(path, records); err != nil {
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

func saveHistory(path string, records []Record) error {
	data, err := json.MarshalIndent(HistoryFile{Version: historyVersion, Records: records}, "", "  ")
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
