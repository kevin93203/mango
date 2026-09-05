package history

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/glebarez/sqlite"
	"github.com/kevin93203/mango/internal/scheduler"
	"gorm.io/driver/mysql"
	"gorm.io/driver/postgres"
	"gorm.io/gorm"
	"gorm.io/gorm/clause"
)

type Config struct {
	Driver string
	Path   string
	DSN    string
}

type Repository struct {
	db      *gorm.DB
	driver  string
	writeMu sync.Mutex
}

type runModel struct {
	ID          int64     `gorm:"primaryKey;autoIncrement"`
	RunID       string    `gorm:"uniqueIndex;size:255;not null"`
	Project     string    `gorm:"index:idx_history_runs_target,priority:1;size:255"`
	Name        string    `gorm:"size:255"`
	TargetType  string    `gorm:"index:idx_history_runs_target,priority:2;size:32"`
	Target      string    `gorm:"index:idx_history_runs_target,priority:3;size:255"`
	TriggerType string    `gorm:"index:idx_history_runs_trigger,priority:1;size:32"`
	TriggerName string    `gorm:"index:idx_history_runs_trigger,priority:2;size:255"`
	TriggerMode string    `gorm:"size:32"`
	EventID     string    `gorm:"size:255"`
	Status      string    `gorm:"size:32"`
	Started     time.Time `gorm:"index:idx_history_runs_started,priority:1"`
	Finished    time.Time
	ExitCode    int
	Error       string `gorm:"type:text"`
	Stderr      string `gorm:"type:text"`
}

func (runModel) TableName() string { return "history_runs" }

type taskModel struct {
	ID              int64  `gorm:"primaryKey;autoIncrement"`
	RunRowID        int64  `gorm:"index:idx_history_tasks_run_started,priority:1;not null"`
	TaskRunID       string `gorm:"index;size:255"`
	ParentRunID     string `gorm:"size:255"`
	Node            string `gorm:"size:255"`
	Task            string `gorm:"index;size:255"`
	Command         string `gorm:"type:text"`
	ArgsJSON        string `gorm:"type:text"`
	WorkingDir      string `gorm:"type:text"`
	EnvKeysJSON     string `gorm:"type:text"`
	ArgsRedacted    bool
	Status          string    `gorm:"size:32"`
	Started         time.Time `gorm:"index:idx_history_tasks_run_started,priority:2"`
	Finished        time.Time
	DurationSeconds float64
	ExitCode        int
	Error           string `gorm:"type:text"`
	Stderr          string `gorm:"type:text"`
}

func (taskModel) TableName() string { return "history_tasks" }

type attemptModel struct {
	ID              int64 `gorm:"primaryKey;autoIncrement"`
	RunRowID        int64 `gorm:"index;not null"`
	TaskRowID       int64 `gorm:"index;not null"`
	Number          int
	Started         time.Time
	Finished        time.Time
	DurationSeconds float64
	ExitCode        int
	Error           string `gorm:"type:text"`
	Stderr          string `gorm:"type:text"`
}

func (attemptModel) TableName() string { return "history_attempts" }

type counterModel struct {
	Key   string `gorm:"primaryKey;size:512"`
	Value int64
}

func (counterModel) TableName() string { return "history_counters" }

func Open(config Config) (*Repository, error) {
	driver := strings.ToLower(strings.TrimSpace(config.Driver))
	if driver == "" {
		driver = "sqlite"
	}
	var dialector gorm.Dialector
	switch driver {
	case "sqlite":
		if config.Path == "" {
			return nil, errors.New("sqlite history database path is required")
		}
		if directory := filepath.Dir(config.Path); directory != "." {
			if err := os.MkdirAll(directory, 0o700); err != nil {
				return nil, fmt.Errorf("create history database directory: %w", err)
			}
		}
		dialector = sqlite.Open(config.Path)
	case "postgres":
		if config.DSN == "" {
			return nil, errors.New("postgres history database DSN is required")
		}
		dialector = postgres.Open(config.DSN)
	case "mysql":
		if config.DSN == "" {
			return nil, errors.New("mysql history database DSN is required")
		}
		dialector = mysql.Open(config.DSN)
	default:
		return nil, fmt.Errorf("unsupported history database driver %q", driver)
	}

	db, err := gorm.Open(dialector, &gorm.Config{})
	if err != nil {
		return nil, fmt.Errorf("open history database: %w", err)
	}
	sqlDB, err := db.DB()
	if err != nil {
		return nil, fmt.Errorf("get history database handle: %w", err)
	}
	if driver == "sqlite" {
		// A single writer is sufficient for the daemon and avoids SQLite lock
		// contention while still allowing all repository operations to use the
		// database/sql abstraction.
		sqlDB.SetMaxOpenConns(1)
		sqlDB.SetMaxIdleConns(1)
	}
	repository := &Repository{db: db, driver: driver}
	if err := db.AutoMigrate(&runModel{}, &taskModel{}, &attemptModel{}, &counterModel{}); err != nil {
		_ = sqlDB.Close()
		return nil, fmt.Errorf("auto-migrate history database: %w", err)
	}
	return repository, nil
}

func (r *Repository) Record(ctx context.Context, record scheduler.Record, limit int) error {
	r.writeMu.Lock()
	defer r.writeMu.Unlock()

	return r.db.WithContext(ctx).Transaction(func(tx *gorm.DB) error {
		run := runModelFromRecord(record)
		if err := tx.Create(&run).Error; err != nil {
			return err
		}

		for _, task := range record.Tasks {
			model := taskModelFromRecord(run.ID, task)
			if err := tx.Create(&model).Error; err != nil {
				return err
			}
			if err := createAttempts(tx, run.ID, model.ID, task.Attempts); err != nil {
				return err
			}
		}
		if err := createAttempts(tx, run.ID, 0, record.Attempts); err != nil {
			return err
		}
		for _, key := range scheduler.CounterKeys(record) {
			counter := counterModel{Key: key, Value: 1}
			if err := tx.Clauses(clause.OnConflict{
				Columns:   []clause.Column{{Name: "key"}},
				DoUpdates: clause.Assignments(map[string]interface{}{"value": gorm.Expr("value + ?", 1)}),
			}).Create(&counter).Error; err != nil {
				return err
			}
		}
		return r.prune(tx, limit)
	})
}

func (r *Repository) Prune(ctx context.Context, limit int) error {
	r.writeMu.Lock()
	defer r.writeMu.Unlock()
	return r.db.WithContext(ctx).Transaction(func(tx *gorm.DB) error {
		return r.prune(tx, limit)
	})
}

func (r *Repository) Query(ctx context.Context, query scheduler.HistoryQuery) ([]scheduler.Record, error) {
	if query.Tail < 0 {
		return nil, errors.New("history tail must be non-negative")
	}
	db := r.db.WithContext(ctx).Model(&runModel{})
	if query.TriggerType != "" {
		db = db.Where("trigger_type = ?", query.TriggerType)
	}
	if query.Trigger != "" {
		db = db.Where("trigger_name = ?", query.Trigger)
	}
	if query.Project != "" {
		db = db.Where("project = ?", query.Project)
	}

	var taskRunIDs []int64
	if query.TargetType == "task" || (query.TargetType == "" && query.Name != "") {
		taskDB := r.db.WithContext(ctx).Model(&taskModel{}).Select("run_row_id")
		if query.Name != "" {
			taskDB = taskDB.Where("task = ?", query.Name)
		}
		if query.Project != "" {
			taskDB = taskDB.Joins("JOIN history_runs ON history_runs.id = history_tasks.run_row_id").Where("history_runs.project = ?", query.Project)
		}
		if err := taskDB.Distinct("run_row_id").Pluck("run_row_id", &taskRunIDs).Error; err != nil {
			return nil, err
		}
	}

	switch query.TargetType {
	case "workflow":
		db = db.Where("target_type = ?", "workflow")
		if query.Name != "" {
			db = db.Where("target = ?", query.Name)
		}
	case "task":
		if query.Name == "" {
			db = db.Where("target_type = ? OR id IN ?", "task", taskRunIDs)
		} else {
			db = db.Where("(target_type = ? AND target = ?) OR id IN ?", "task", query.Name, taskRunIDs)
		}
	case "":
		if query.Name != "" {
			db = db.Where("target = ? OR id IN ?", query.Name, taskRunIDs)
		}
	}

	order := "started ASC, id ASC"
	if query.Tail > 0 {
		order = "started DESC, id DESC"
		db = db.Limit(query.Tail)
	}
	var runs []runModel
	if err := db.Order(order).Find(&runs).Error; err != nil {
		return nil, err
	}
	if query.Tail > 0 {
		sort.Slice(runs, func(i, j int) bool {
			if runs[i].Started.Equal(runs[j].Started) {
				return runs[i].ID < runs[j].ID
			}
			return runs[i].Started.Before(runs[j].Started)
		})
	}
	return r.loadRecords(ctx, runs, query)
}

func (r *Repository) Counters(ctx context.Context) (map[string]uint64, error) {
	var counters []counterModel
	if err := r.db.WithContext(ctx).Find(&counters).Error; err != nil {
		return nil, err
	}
	result := make(map[string]uint64, len(counters))
	for _, counter := range counters {
		if counter.Value >= 0 {
			result[counter.Key] = uint64(counter.Value)
		}
	}
	return result, nil
}

func (r *Repository) LatestSchedules(ctx context.Context, schedules []scheduler.ScheduleRef) (map[string]scheduler.Record, error) {
	result := make(map[string]scheduler.Record, len(schedules))
	for _, schedule := range schedules {
		db := r.db.WithContext(ctx).Model(&runModel{}).
			Where("project = ? AND trigger_type = ? AND trigger_name = ?", schedule.Project, scheduler.TriggerSchedule, schedule.Name).
			Order("started DESC, id DESC").Limit(1)
		var run runModel
		if err := db.First(&run).Error; err != nil {
			if !errors.Is(err, gorm.ErrRecordNotFound) {
				return nil, err
			}
			// Preserve read-only compatibility for records created by old
			// embedders that did not set a structured trigger.
			fallback := r.db.WithContext(ctx).Model(&runModel{}).
				Where("project = ? AND trigger_type = ? AND trigger_name = ? AND name = ?", schedule.Project, "", "", schedule.Name).
				Order("started DESC, id DESC").Limit(1)
			if err := fallback.First(&run).Error; err != nil {
				if errors.Is(err, gorm.ErrRecordNotFound) {
					continue
				}
				return nil, err
			}
		}
		records, err := r.loadRecords(ctx, []runModel{run}, scheduler.HistoryQuery{})
		if err != nil {
			return nil, err
		}
		if len(records) == 1 {
			result[schedule.Project+"/"+schedule.Name] = records[0]
		}
	}
	return result, nil
}

func (r *Repository) Close() error {
	sqlDB, err := r.db.DB()
	if err != nil {
		return err
	}
	return sqlDB.Close()
}

func (r *Repository) prune(tx *gorm.DB, limit int) error {
	if limit <= 0 {
		return nil
	}
	var boundary runModel
	err := tx.Model(&runModel{}).Order("started DESC, id DESC").Offset(limit).First(&boundary).Error
	if errors.Is(err, gorm.ErrRecordNotFound) {
		return nil
	}
	if err != nil {
		return err
	}
	var ids []int64
	if err := tx.Model(&runModel{}).
		Where("started < ? OR (started = ? AND id <= ?)", boundary.Started, boundary.Started, boundary.ID).
		Pluck("id", &ids).Error; err != nil {
		return err
	}
	var taskIDs []int64
	if err := tx.Model(&taskModel{}).Where("run_row_id IN ?", ids).Pluck("id", &taskIDs).Error; err != nil {
		return err
	}
	if len(taskIDs) > 0 {
		if err := tx.Where("task_row_id IN ?", taskIDs).Delete(&attemptModel{}).Error; err != nil {
			return err
		}
	}
	if err := tx.Where("run_row_id IN ?", ids).Delete(&attemptModel{}).Error; err != nil {
		return err
	}
	if err := tx.Where("run_row_id IN ?", ids).Delete(&taskModel{}).Error; err != nil {
		return err
	}
	return tx.Where("id IN ?", ids).Delete(&runModel{}).Error
}

func (r *Repository) loadRecords(ctx context.Context, runs []runModel, query scheduler.HistoryQuery) ([]scheduler.Record, error) {
	if len(runs) == 0 {
		return []scheduler.Record{}, nil
	}
	runIDs := make([]int64, 0, len(runs))
	for _, run := range runs {
		runIDs = append(runIDs, run.ID)
	}
	var tasks []taskModel
	taskDB := r.db.WithContext(ctx).Where("run_row_id IN ?", runIDs)
	if err := taskDB.Order("started ASC, id ASC").Find(&tasks).Error; err != nil {
		return nil, err
	}

	var attempts []attemptModel
	if err := r.db.WithContext(ctx).Where("run_row_id IN ?", runIDs).Order("started ASC, id ASC").Find(&attempts).Error; err != nil {
		return nil, err
	}
	attemptsByTask := make(map[int64][]scheduler.Attempt)
	for _, attempt := range attempts {
		attemptsByTask[attempt.TaskRowID] = append(attemptsByTask[attempt.TaskRowID], attemptToRecord(attempt))
	}
	tasksByRun := make(map[int64][]taskModel)
	for _, task := range tasks {
		tasksByRun[task.RunRowID] = append(tasksByRun[task.RunRowID], task)
	}

	result := make([]scheduler.Record, 0, len(runs))
	for _, run := range runs {
		record := scheduler.Record{
			RunID: run.RunID, Project: run.Project, Name: run.Name, TargetType: run.TargetType, Target: run.Target,
			Trigger: scheduler.TriggerRef{Type: run.TriggerType, Name: run.TriggerName, Mode: run.TriggerMode, EventID: run.EventID},
			Status:  run.Status, Started: run.Started, Finished: run.Finished, ExitCode: run.ExitCode,
			Error: run.Error, Stderr: run.Stderr, Attempts: cloneAttempts(attemptsByTask[0]),
		}
		for _, task := range tasksByRun[run.ID] {
			if query.TargetType == "task" && query.Name != "" && run.TargetType == "workflow" && task.Task != query.Name {
				continue
			}
			record.Tasks = append(record.Tasks, scheduler.TaskRecord{
				RunID: task.TaskRunID, ParentRunID: task.ParentRunID, Node: task.Node, Task: task.Task,
				Command: task.Command, Args: decodeStrings(task.ArgsJSON), WorkingDir: task.WorkingDir,
				EnvKeys: decodeStrings(task.EnvKeysJSON), ArgsRedacted: task.ArgsRedacted, Status: task.Status,
				Started: task.Started, Finished: task.Finished, DurationSeconds: task.DurationSeconds,
				ExitCode: task.ExitCode, Error: task.Error, Stderr: task.Stderr,
				Attempts: cloneAttempts(attemptsByTask[task.ID]),
			})
		}
		result = append(result, record)
	}
	return result, nil
}

func runModelFromRecord(record scheduler.Record) runModel {
	return runModel{
		RunID: record.RunID, Project: record.Project, Name: record.Name, TargetType: record.TargetType, Target: record.Target,
		TriggerType: record.Trigger.Type, TriggerName: record.Trigger.Name, TriggerMode: record.Trigger.Mode, EventID: record.Trigger.EventID,
		Status: record.Status, Started: record.Started, Finished: record.Finished, ExitCode: record.ExitCode,
		Error: record.Error, Stderr: record.Stderr,
	}
}

func taskModelFromRecord(runID int64, task scheduler.TaskRecord) taskModel {
	return taskModel{
		RunRowID: runID, TaskRunID: task.RunID, ParentRunID: task.ParentRunID, Node: task.Node, Task: task.Task,
		Command: task.Command, ArgsJSON: encodeStrings(task.Args), WorkingDir: task.WorkingDir,
		EnvKeysJSON: encodeStrings(task.EnvKeys), ArgsRedacted: task.ArgsRedacted, Status: task.Status,
		Started: task.Started, Finished: task.Finished, DurationSeconds: task.DurationSeconds,
		ExitCode: task.ExitCode, Error: task.Error, Stderr: task.Stderr,
	}
}

func createAttempts(tx *gorm.DB, runID, taskID int64, attempts []scheduler.Attempt) error {
	for _, attempt := range attempts {
		model := attemptModel{
			RunRowID: runID, TaskRowID: taskID, Number: attempt.Number, Started: attempt.Started,
			Finished: attempt.Finished, DurationSeconds: attempt.DurationSeconds, ExitCode: attempt.ExitCode,
			Error: attempt.Error, Stderr: attempt.Stderr,
		}
		if err := tx.Create(&model).Error; err != nil {
			return err
		}
	}
	return nil
}

func attemptToRecord(attempt attemptModel) scheduler.Attempt {
	return scheduler.Attempt{
		Number: attempt.Number, Started: attempt.Started, Finished: attempt.Finished,
		DurationSeconds: attempt.DurationSeconds, ExitCode: attempt.ExitCode, Error: attempt.Error, Stderr: attempt.Stderr,
	}
}

func cloneAttempts(attempts []scheduler.Attempt) []scheduler.Attempt {
	if attempts == nil {
		return nil
	}
	return append([]scheduler.Attempt(nil), attempts...)
}

func encodeStrings(values []string) string {
	data, _ := json.Marshal(values)
	return string(data)
}

func decodeStrings(value string) []string {
	if value == "" {
		return nil
	}
	var result []string
	if json.Unmarshal([]byte(value), &result) != nil {
		return nil
	}
	return result
}
