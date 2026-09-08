package history

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/glebarez/sqlite"
	"github.com/kevin93203/mango/internal/scheduler"
	"gorm.io/driver/mysql"
	"gorm.io/driver/postgres"
	"gorm.io/gorm"
	"gorm.io/gorm/clause"
	"gorm.io/gorm/logger"
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
	ID                      int64     `gorm:"primaryKey;autoIncrement"`
	RunID                   string    `gorm:"uniqueIndex;size:255;not null"`
	Project                 string    `gorm:"index:idx_history_runs_target,priority:1;size:255"`
	Name                    string    `gorm:"size:255"`
	TargetType              string    `gorm:"index:idx_history_runs_target,priority:2;size:32"`
	Target                  string    `gorm:"index:idx_history_runs_target,priority:3;size:255"`
	TriggerType             string    `gorm:"index:idx_history_runs_trigger,priority:1;size:32"`
	TriggerName             string    `gorm:"index:idx_history_runs_trigger,priority:2;size:255"`
	TriggerMode             string    `gorm:"size:32"`
	EventID                 string    `gorm:"size:255"`
	Status                  string    `gorm:"size:32"`
	Started                 time.Time `gorm:"index:idx_history_runs_started,priority:1"`
	Finished                time.Time
	ExitCode                int
	Error                   string `gorm:"type:text"`
	Stderr                  string `gorm:"type:text"`
	StdoutPath              string `gorm:"type:text"`
	StderrPath              string `gorm:"type:text"`
	IdempotencyKey          string `gorm:"index;size:255"`
	ConfigurationGeneration uint64
	RetriedFromRunID        *string   `gorm:"index:idx_history_runs_retried_from_run_id;size:255"`
	CreatedAt               time.Time `gorm:"index"`
	UpdatedAt               time.Time
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
	StdoutPath      string `gorm:"type:text"`
	StderrPath      string `gorm:"type:text"`
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

type schemaVersionModel struct {
	ID        int `gorm:"primaryKey"`
	Version   int
	AppliedAt time.Time
}

func (schemaVersionModel) TableName() string { return "schema_version" }

type eventModel struct {
	ID        int64     `gorm:"primaryKey;autoIncrement"`
	RunID     string    `gorm:"index;size:255;not null"`
	Type      string    `gorm:"size:64;not null"`
	Status    string    `gorm:"size:32"`
	Details   string    `gorm:"type:text"`
	CreatedAt time.Time `gorm:"index"`
}

func (eventModel) TableName() string { return "execution_events" }

type operationModel struct {
	ID          int64     `gorm:"primaryKey;autoIncrement"`
	RunID       string    `gorm:"index;size:255;not null"`
	Type        string    `gorm:"size:64;not null"`
	Status      string    `gorm:"size:32"`
	Error       string    `gorm:"type:text"`
	RequestedAt time.Time `gorm:"index"`
	CompletedAt time.Time
}

func (operationModel) TableName() string { return "execution_operations" }

const currentSchemaVersion = 3

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

	db, err := gorm.Open(dialector, &gorm.Config{Logger: logger.Default.LogMode(logger.Silent)})
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
	if err := migrate(db, driver, config.Path); err != nil {
		_ = sqlDB.Close()
		return nil, fmt.Errorf("migrate history database: %w", err)
	}
	return repository, nil
}

// migrate applies the metadata schema explicitly. Keeping migration steps in
// code rather than calling AutoMigrate on every open makes upgrades
// reviewable, repeatable, and fail closed when a step cannot be applied.
func migrate(db *gorm.DB, driver, path string) error {
	if driver == "sqlite" {
		if err := backupBeforeMigration(path); err != nil {
			return err
		}
	}
	migrator := db.Migrator()
	if !migrator.HasTable(&schemaVersionModel{}) {
		if err := migrator.CreateTable(&schemaVersionModel{}); err != nil {
			return fmt.Errorf("create schema version table: %w", err)
		}
	}
	versionRow := schemaVersionModel{ID: 1}
	result := db.Where("id = ?", 1).First(&versionRow)
	version := 0
	if result.Error == nil {
		version = versionRow.Version
	} else if !errors.Is(result.Error, gorm.ErrRecordNotFound) {
		return result.Error
	}
	if version > currentSchemaVersion {
		return fmt.Errorf("history schema version %d is newer than supported version %d", version, currentSchemaVersion)
	}
	for next := version + 1; next <= currentSchemaVersion; next++ {
		if err := applyMigration(db, next); err != nil {
			return fmt.Errorf("apply history migration %d: %w", next, err)
		}
		versionRow.ID = 1
		versionRow.Version = next
		versionRow.AppliedAt = time.Now().UTC()
		if err := db.Save(&versionRow).Error; err != nil {
			return fmt.Errorf("record history migration %d: %w", next, err)
		}
	}
	return nil
}

func applyMigration(db *gorm.DB, version int) error {
	migrator := db.Migrator()
	switch version {
	case 1:
		models := []interface{}{&runModel{}, &taskModel{}, &attemptModel{}, &counterModel{}}
		for _, model := range models {
			if !migrator.HasTable(model) {
				if err := migrator.CreateTable(model); err != nil {
					return err
				}
			}
		}
		return nil
	case 2:
		if !migrator.HasTable(&runModel{}) {
			return errors.New("history_runs table is missing")
		}
		columns := []string{"StdoutPath", "StderrPath", "IdempotencyKey", "ConfigurationGeneration", "CreatedAt", "UpdatedAt"}
		for _, column := range columns {
			if !migrator.HasColumn(&runModel{}, column) {
				if err := migrator.AddColumn(&runModel{}, column); err != nil {
					return fmt.Errorf("add history_runs.%s: %w", column, err)
				}
			}
		}
		if migrator.HasTable(&taskModel{}) {
			for _, column := range []string{"StdoutPath", "StderrPath"} {
				if !migrator.HasColumn(&taskModel{}, column) {
					if err := migrator.AddColumn(&taskModel{}, column); err != nil {
						return fmt.Errorf("add history_tasks.%s: %w", column, err)
					}
				}
			}
		}
		for _, model := range []interface{}{&eventModel{}, &operationModel{}} {
			if !migrator.HasTable(model) {
				if err := migrator.CreateTable(model); err != nil {
					return err
				}
			}
		}
		return nil
	case 3:
		if !migrator.HasTable(&runModel{}) {
			return errors.New("history_runs table is missing")
		}
		if !migrator.HasColumn(&runModel{}, "RetriedFromRunID") {
			if err := migrator.AddColumn(&runModel{}, "RetriedFromRunID"); err != nil {
				return fmt.Errorf("add history_runs.retried_from_run_id: %w", err)
			}
		}
		if !migrator.HasIndex(&runModel{}, "idx_history_runs_retried_from_run_id") {
			if err := migrator.CreateIndex(&runModel{}, "idx_history_runs_retried_from_run_id"); err != nil {
				return fmt.Errorf("index history_runs.retried_from_run_id: %w", err)
			}
		}
		return nil
	default:
		return fmt.Errorf("unknown history migration %d", version)
	}
}

func backupBeforeMigration(path string) error {
	if path == "" {
		return nil
	}
	info, err := os.Stat(path)
	if errors.Is(err, os.ErrNotExist) {
		return nil
	}
	if err != nil {
		return err
	}
	if info.Size() == 0 {
		return nil
	}
	backup := path + ".bak"
	if _, err := os.Stat(backup); err == nil {
		return nil
	} else if !errors.Is(err, os.ErrNotExist) {
		return err
	}
	data, err := os.ReadFile(path)
	if err != nil {
		return fmt.Errorf("read SQLite database for backup: %w", err)
	}
	if err := os.WriteFile(backup, data, 0o600); err != nil {
		return fmt.Errorf("write SQLite migration backup: %w", err)
	}
	return nil
}

func (r *Repository) Record(ctx context.Context, record scheduler.Record, limit int) error {
	if record.Status == "" {
		record.Status = statusForCompletedRecord(record)
	}
	r.writeMu.Lock()
	defer r.writeMu.Unlock()

	return r.db.WithContext(ctx).Transaction(func(tx *gorm.DB) error {
		run, created, previousStatus, err := r.saveRecord(tx, record)
		if err != nil {
			return err
		}
		if created || (!isTerminalStatus(previousStatus) && isTerminalStatus(record.Status)) {
			for _, key := range scheduler.CounterKeys(record) {
				counter := counterModel{Key: key, Value: 1}
				if err := tx.Clauses(clause.OnConflict{
					Columns:   []clause.Column{{Name: "key"}},
					DoUpdates: clause.Assignments(map[string]interface{}{"value": counterIncrementExpression(tx)}),
				}).Create(&counter).Error; err != nil {
					return err
				}
			}
		}
		if (created || previousStatus != record.Status) && record.Status != "" {
			if err := createEvent(tx, scheduler.ExecutionEvent{RunID: run.RunID, Type: "state_transition", Status: record.Status, CreatedAt: time.Now().UTC()}); err != nil {
				return err
			}
		}
		return r.prune(tx, limit)
	})
}

func statusForCompletedRecord(record scheduler.Record) string {
	if record.Error != "" || record.ExitCode != 0 {
		return scheduler.StatusFailed
	}
	return scheduler.StatusSuccess
}

func (r *Repository) saveRecord(tx *gorm.DB, record scheduler.Record) (runModel, bool, string, error) {
	if record.RunID == "" {
		record.RunID = scheduler.NewRunID()
	}
	run := runModelFromRecord(record)
	previousStatus := ""
	created := false
	var existing runModel
	err := tx.Where("run_id = ?", record.RunID).First(&existing).Error
	if errors.Is(err, gorm.ErrRecordNotFound) {
		if legacyID, parseErr := legacyRunRowID(record.RunID); parseErr == nil {
			err = tx.Where("id = ?", legacyID).First(&existing).Error
		}
	}
	if errors.Is(err, gorm.ErrRecordNotFound) {
		created = true
		if run.CreatedAt.IsZero() {
			run.CreatedAt = time.Now().UTC()
		}
		run.UpdatedAt = run.CreatedAt
		if err := tx.Create(&run).Error; err != nil {
			return run, false, previousStatus, err
		}
	} else if err != nil {
		return run, false, previousStatus, err
	} else {
		previousStatus = existing.Status
		if isTerminalStatus(existing.Status) {
			previous, err := recordFromModel(existing)
			if err != nil {
				return run, false, previousStatus, err
			}
			if scheduler.IsActiveStatus(record.Status) {
				return run, false, previousStatus, scheduler.ErrExecutionTransition
			}
			if record.IdempotencyKey == "" {
				record.IdempotencyKey = previous.IdempotencyKey
			}
			if record.ConfigurationGeneration == 0 {
				record.ConfigurationGeneration = previous.ConfigurationGeneration
			}
			if record.RetriedFromRunID == "" {
				record.RetriedFromRunID = previous.RetriedFromRunID
			}
			if detailed, loadErr := r.loadRecordsFromDB(context.Background(), tx, []runModel{existing}, scheduler.HistoryQuery{}); loadErr != nil {
				return run, false, previousStatus, loadErr
			} else if len(detailed) == 1 {
				previous = detailed[0]
			}
			if !scheduler.SameTerminalRecord(previous, record) {
				return run, false, previousStatus, scheduler.ErrTerminalExecutionImmutable
			}
			// Identical terminal writes are idempotent. In particular, do not
			// duplicate task/attempt/event rows or touch retention state.
			return existing, false, previousStatus, nil
		}
		run.ID = existing.ID
		if run.IdempotencyKey == "" {
			run.IdempotencyKey = existing.IdempotencyKey
		}
		if run.ConfigurationGeneration == 0 {
			run.ConfigurationGeneration = existing.ConfigurationGeneration
		}
		if run.RetriedFromRunID == nil {
			run.RetriedFromRunID = existing.RetriedFromRunID
		}
		if run.CreatedAt.IsZero() {
			run.CreatedAt = existing.CreatedAt
		}
		run.UpdatedAt = time.Now().UTC()
		if err := tx.Model(&existing).Updates(map[string]interface{}{
			"project": run.Project, "name": run.Name, "target_type": run.TargetType, "target": run.Target,
			"trigger_type": run.TriggerType, "trigger_name": run.TriggerName, "trigger_mode": run.TriggerMode,
			"event_id": run.EventID, "status": run.Status, "started": run.Started, "finished": run.Finished,
			"exit_code": run.ExitCode, "error": run.Error, "stderr": run.Stderr,
			"stdout_path": run.StdoutPath, "stderr_path": run.StderrPath,
			"idempotency_key": run.IdempotencyKey, "configuration_generation": run.ConfigurationGeneration,
			"retried_from_run_id": run.RetriedFromRunID,
			"created_at":          run.CreatedAt, "updated_at": run.UpdatedAt,
		}).Error; err != nil {
			return run, false, previousStatus, err
		}
		// Replacing child rows is allowed only while the execution is active.
		// A terminal snapshot is the first and final child snapshot.
		if len(record.Tasks) > 0 || len(record.Attempts) > 0 {
			var taskIDs []int64
			if err := tx.Model(&taskModel{}).Where("run_row_id = ?", run.ID).Pluck("id", &taskIDs).Error; err != nil {
				return run, false, previousStatus, err
			}
			if len(taskIDs) > 0 {
				if err := tx.Where("task_row_id IN ?", taskIDs).Delete(&attemptModel{}).Error; err != nil {
					return run, false, previousStatus, err
				}
			}
			if err := tx.Where("run_row_id = ?", run.ID).Delete(&attemptModel{}).Error; err != nil {
				return run, false, previousStatus, err
			}
			if err := tx.Where("run_row_id = ?", run.ID).Delete(&taskModel{}).Error; err != nil {
				return run, false, previousStatus, err
			}
		}
	}

	for _, task := range record.Tasks {
		model := taskModelFromRecord(run.ID, task)
		if err := tx.Create(&model).Error; err != nil {
			return run, created, previousStatus, err
		}
		if err := createAttempts(tx, run.ID, model.ID, task.Attempts); err != nil {
			return run, created, previousStatus, err
		}
	}
	if err := createAttempts(tx, run.ID, 0, record.Attempts); err != nil {
		return run, created, previousStatus, err
	}
	return run, created, previousStatus, nil
}

func isTerminalStatus(status string) bool {
	switch status {
	case scheduler.StatusSuccess, scheduler.StatusFailed, scheduler.StatusSkipped, scheduler.StatusCancelled, scheduler.StatusInterrupted:
		return true
	default:
		return false
	}
}

func createEvent(tx *gorm.DB, event scheduler.ExecutionEvent) error {
	if event.CreatedAt.IsZero() {
		event.CreatedAt = time.Now().UTC()
	}
	return tx.Create(&eventModel{RunID: event.RunID, Type: event.Type, Status: event.Status, Details: event.Details, CreatedAt: event.CreatedAt}).Error
}

func (r *Repository) BeginExecution(ctx context.Context, record scheduler.Record, idempotencyKey string, configurationGeneration uint64) (scheduler.Execution, bool, error) {
	r.writeMu.Lock()
	defer r.writeMu.Unlock()
	if record.RunID == "" {
		record.RunID = scheduler.NewRunID()
	}
	if record.Status == "" {
		record.Status = scheduler.StatusQueued
	}
	record.IdempotencyKey = idempotencyKey
	record.ConfigurationGeneration = configurationGeneration
	returnValue := scheduler.Execution{Record: record, IdempotencyKey: idempotencyKey, ConfigurationGeneration: configurationGeneration}
	err := r.db.WithContext(ctx).Transaction(func(tx *gorm.DB) error {
		if idempotencyKey != "" {
			var existing runModel
			if err := tx.Where("idempotency_key = ?", idempotencyKey).Order("id ASC").First(&existing).Error; err == nil {
				returnValue, _ = executionFromModel(existing)
				returnValue.Record, _ = recordFromModel(existing)
				return errExistingExecution
			} else if !errors.Is(err, gorm.ErrRecordNotFound) {
				return err
			}
		}
		run, _, _, err := r.saveRecord(tx, record)
		if err != nil {
			return err
		}
		if err := createEvent(tx, scheduler.ExecutionEvent{RunID: run.RunID, Type: "created", Status: record.Status, CreatedAt: time.Now().UTC()}); err != nil {
			return err
		}
		returnValue, _ = executionFromModel(run)
		returnValue.Record, _ = recordFromModel(run)
		return nil
	})
	if errors.Is(err, errExistingExecution) {
		return returnValue, false, nil
	}
	if err != nil {
		return scheduler.Execution{}, false, err
	}
	return returnValue, true, nil
}

var errExistingExecution = errors.New("existing execution")

func executionFromModel(run runModel) (scheduler.Execution, error) {
	record, err := recordFromModel(run)
	if err != nil {
		return scheduler.Execution{}, err
	}
	return scheduler.Execution{Record: record, IdempotencyKey: run.IdempotencyKey, ConfigurationGeneration: run.ConfigurationGeneration, CreatedAt: run.CreatedAt, UpdatedAt: run.UpdatedAt}, nil
}

func recordFromModel(run runModel) (scheduler.Record, error) {
	return scheduler.Record{
		RunID: compatibleRunID(run), Project: run.Project, Name: run.Name, TargetType: run.TargetType, Target: run.Target,
		Trigger: scheduler.TriggerRef{Type: run.TriggerType, Name: run.TriggerName, Mode: run.TriggerMode, EventID: run.EventID},
		Status:  run.Status, Started: run.Started, Finished: run.Finished, ExitCode: run.ExitCode,
		Error: run.Error, Stderr: run.Stderr, StdoutPath: run.StdoutPath, StderrPath: run.StderrPath,
		IdempotencyKey: run.IdempotencyKey, ConfigurationGeneration: run.ConfigurationGeneration,
		RetriedFromRunID: stringValue(run.RetriedFromRunID),
	}, nil
}

func compatibleRunID(run runModel) string {
	if run.RunID != "" {
		return run.RunID
	}
	if run.ID > 0 {
		return "legacy-" + strconv.FormatInt(run.ID, 10)
	}
	return scheduler.NewRunID()
}

func legacyRunRowID(runID string) (int64, error) {
	if !strings.HasPrefix(runID, "legacy-") {
		return 0, errors.New("not a legacy run id")
	}
	value := strings.TrimPrefix(runID, "legacy-")
	if value == "" {
		return 0, errors.New("legacy run id is missing row id")
	}
	return strconv.ParseInt(value, 10, 64)
}

func (r *Repository) GetExecution(ctx context.Context, runID string) (scheduler.Execution, error) {
	var run runModel
	err := r.db.WithContext(ctx).Where("run_id = ?", runID).First(&run).Error
	if errors.Is(err, gorm.ErrRecordNotFound) {
		if legacyID, parseErr := legacyRunRowID(runID); parseErr == nil {
			err = r.db.WithContext(ctx).Where("id = ?", legacyID).First(&run).Error
		}
	}
	if err != nil {
		if errors.Is(err, gorm.ErrRecordNotFound) {
			return scheduler.Execution{}, scheduler.ErrExecutionNotFound
		}
		return scheduler.Execution{}, err
	}
	records, err := r.loadRecords(ctx, []runModel{run}, scheduler.HistoryQuery{})
	if err != nil {
		return scheduler.Execution{}, err
	}
	execution, err := executionFromModel(run)
	if err != nil {
		return scheduler.Execution{}, err
	}
	if len(records) == 1 {
		execution.Record = records[0]
	}
	return execution, nil
}

func (r *Repository) ListActiveExecutions(ctx context.Context) ([]scheduler.Execution, error) {
	var runs []runModel
	if err := r.db.WithContext(ctx).Where("status IN ?", []string{scheduler.StatusQueued, scheduler.StatusRunning}).Order("started ASC, id ASC").Find(&runs).Error; err != nil {
		return nil, err
	}
	result := make([]scheduler.Execution, 0, len(runs))
	for _, run := range runs {
		execution, err := executionFromModel(run)
		if err != nil {
			return nil, err
		}
		records, err := r.loadRecords(ctx, []runModel{run}, scheduler.HistoryQuery{})
		if err != nil {
			return nil, err
		}
		if len(records) == 1 {
			execution.Record = records[0]
		}
		result = append(result, execution)
	}
	return result, nil
}

func (r *Repository) ListExecutions(ctx context.Context, query scheduler.ExecutionQuery) ([]scheduler.Execution, error) {
	if query.Limit < 0 {
		return nil, errors.New("execution list limit must be non-negative")
	}
	db := r.db.WithContext(ctx).Model(&runModel{})
	if query.Status != "" {
		db = db.Where("status = ?", query.Status)
	} else if !query.All {
		db = db.Where("status IN ?", []string{scheduler.StatusQueued, scheduler.StatusRunning})
	}
	if query.TriggerType != "" {
		db = db.Where("trigger_type = ?", query.TriggerType)
	}
	if query.Trigger != "" {
		db = db.Where("trigger_name = ?", query.Trigger)
	}
	if query.Project != "" {
		db = db.Where("project = ?", query.Project)
	}
	if query.TargetType != "" {
		db = db.Where("target_type = ?", query.TargetType)
	}
	if query.Target != "" {
		db = db.Where("target = ?", query.Target)
	}
	if query.Limit > 0 {
		db = db.Limit(query.Limit)
	}
	var runs []runModel
	if err := db.Order("started DESC, id DESC").Find(&runs).Error; err != nil {
		return nil, err
	}
	records, err := r.loadRecords(ctx, runs, scheduler.HistoryQuery{})
	if err != nil {
		return nil, err
	}
	recordByRunID := make(map[string]scheduler.Record, len(records))
	for _, record := range records {
		recordByRunID[record.RunID] = record
	}
	result := make([]scheduler.Execution, 0, len(runs))
	for _, run := range runs {
		execution, err := executionFromModel(run)
		if err != nil {
			return nil, err
		}
		if record, ok := recordByRunID[compatibleRunID(run)]; ok {
			execution.Record = record
		}
		result = append(result, execution)
	}
	return result, nil
}

func (r *Repository) UpdateExecution(ctx context.Context, record scheduler.Record) error {
	r.writeMu.Lock()
	defer r.writeMu.Unlock()
	return r.db.WithContext(ctx).Transaction(func(tx *gorm.DB) error {
		run, created, previousStatus, err := r.saveRecord(tx, record)
		if err != nil {
			return err
		}
		if created || (!isTerminalStatus(previousStatus) && isTerminalStatus(record.Status)) {
			for _, key := range scheduler.CounterKeys(record) {
				counter := counterModel{Key: key, Value: 1}
				if err := tx.Clauses(clause.OnConflict{
					Columns:   []clause.Column{{Name: "key"}},
					DoUpdates: clause.Assignments(map[string]interface{}{"value": counterIncrementExpression(tx)}),
				}).Create(&counter).Error; err != nil {
					return err
				}
			}
		}
		if (created || previousStatus != record.Status) && record.Status != "" {
			return createEvent(tx, scheduler.ExecutionEvent{RunID: run.RunID, Type: "state_transition", Status: record.Status, CreatedAt: time.Now().UTC()})
		}
		return nil
	})
}

func (r *Repository) RecordExecutionEvent(ctx context.Context, event scheduler.ExecutionEvent) error {
	r.writeMu.Lock()
	defer r.writeMu.Unlock()
	return r.db.WithContext(ctx).Transaction(func(tx *gorm.DB) error { return createEvent(tx, event) })
}

func (r *Repository) ListExecutionEvents(ctx context.Context, runID string) ([]scheduler.ExecutionEvent, error) {
	var rows []eventModel
	if err := r.db.WithContext(ctx).Where("run_id = ?", runID).Order("id ASC").Find(&rows).Error; err != nil {
		return nil, err
	}
	result := make([]scheduler.ExecutionEvent, 0, len(rows))
	for _, row := range rows {
		result = append(result, scheduler.ExecutionEvent{
			ID: row.ID, RunID: row.RunID, Type: row.Type, Status: row.Status,
			Details: row.Details, CreatedAt: row.CreatedAt,
		})
	}
	return result, nil
}

func (r *Repository) RecordExecutionOperation(ctx context.Context, operation scheduler.ExecutionOperation) (scheduler.ExecutionOperation, error) {
	r.writeMu.Lock()
	defer r.writeMu.Unlock()
	if operation.RequestedAt.IsZero() {
		operation.RequestedAt = time.Now().UTC()
	}
	var model operationModel
	err := r.db.WithContext(ctx).Transaction(func(tx *gorm.DB) error {
		model = operationModel{RunID: operation.RunID, Type: operation.Type, Status: operation.Status, Error: operation.Error, RequestedAt: operation.RequestedAt, CompletedAt: operation.CompletedAt}
		if err := tx.Create(&model).Error; err != nil {
			return err
		}
		return createEvent(tx, scheduler.ExecutionEvent{RunID: operation.RunID, Type: "operation:" + operation.Type, Status: operation.Status, Details: operation.Error, CreatedAt: operation.RequestedAt})
	})
	if err != nil {
		return scheduler.ExecutionOperation{}, err
	}
	operation.ID = model.ID
	return operation, nil
}

func (r *Repository) ListExecutionOperations(ctx context.Context, runID string) ([]scheduler.ExecutionOperation, error) {
	var rows []operationModel
	if err := r.db.WithContext(ctx).Where("run_id = ?", runID).Order("id ASC").Find(&rows).Error; err != nil {
		return nil, err
	}
	result := make([]scheduler.ExecutionOperation, 0, len(rows))
	for _, row := range rows {
		result = append(result, scheduler.ExecutionOperation{
			ID: row.ID, RunID: row.RunID, Type: row.Type, Status: row.Status,
			Error: row.Error, RequestedAt: row.RequestedAt, CompletedAt: row.CompletedAt,
		})
	}
	return result, nil
}

// Ping verifies that the database connection used by the repository is
// currently usable without modifying any history data.
func (r *Repository) Ping(ctx context.Context) error {
	sqlDB, err := r.db.DB()
	if err != nil {
		return err
	}
	return sqlDB.PingContext(ctx)
}

// MissingTables reports required history tables that are not present. The
// check is read-only and does not run migrations.
func (r *Repository) MissingTables(ctx context.Context) ([]string, error) {
	if err := r.Ping(ctx); err != nil {
		return nil, err
	}
	tables := []struct {
		name  string
		model interface{}
	}{
		{name: "history_runs", model: &runModel{}},
		{name: "history_tasks", model: &taskModel{}},
		{name: "history_attempts", model: &attemptModel{}},
		{name: "history_counters", model: &counterModel{}},
		{name: "schema_version", model: &schemaVersionModel{}},
		{name: "execution_events", model: &eventModel{}},
		{name: "execution_operations", model: &operationModel{}},
	}
	migrator := r.db.WithContext(ctx).Migrator()
	missing := make([]string, 0)
	for _, table := range tables {
		if !migrator.HasTable(table.model) {
			missing = append(missing, table.name)
		}
	}
	return missing, nil
}

// Clear removes all persisted execution history, including lifetime
// counters, while retaining the database schema for future writes.
func (r *Repository) Clear(ctx context.Context) error {
	r.writeMu.Lock()
	defer r.writeMu.Unlock()

	return r.db.WithContext(ctx).Transaction(func(tx *gorm.DB) error {
		allowGlobalDelete := tx.Session(&gorm.Session{AllowGlobalUpdate: true})
		if err := allowGlobalDelete.Delete(&attemptModel{}).Error; err != nil {
			return err
		}
		if err := allowGlobalDelete.Delete(&taskModel{}).Error; err != nil {
			return err
		}
		if err := allowGlobalDelete.Delete(&runModel{}).Error; err != nil {
			return err
		}
		if err := allowGlobalDelete.Delete(&eventModel{}).Error; err != nil {
			return err
		}
		if err := allowGlobalDelete.Delete(&operationModel{}).Error; err != nil {
			return err
		}
		return allowGlobalDelete.Delete(&counterModel{}).Error
	})
}

// Purge removes terminal executions and their metadata while leaving active
// runs, lifetime counters, and log files untouched. A nil before is valid
// only when all is true.
func (r *Repository) Purge(ctx context.Context, before *time.Time, all bool) (int, error) {
	if !all && before == nil {
		return 0, errors.New("history purge requires before or all")
	}
	r.writeMu.Lock()
	defer r.writeMu.Unlock()
	removed := 0
	err := r.db.WithContext(ctx).Transaction(func(tx *gorm.DB) error {
		statuses := []string{scheduler.StatusSuccess, scheduler.StatusFailed, scheduler.StatusSkipped, scheduler.StatusCancelled, scheduler.StatusInterrupted}
		query := tx.Model(&runModel{}).Where("status IN ?", statuses)
		if !all {
			query = query.Where("finished < ?", *before)
		}
		var runs []runModel
		if err := query.Find(&runs).Error; err != nil {
			return err
		}
		if len(runs) == 0 {
			return nil
		}
		ids := make([]int64, 0, len(runs))
		runIDs := make([]string, 0, len(runs))
		for _, run := range runs {
			ids = append(ids, run.ID)
			runIDs = append(runIDs, run.RunID)
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
		if err := tx.Where("run_id IN ?", runIDs).Delete(&eventModel{}).Error; err != nil {
			return err
		}
		if err := tx.Where("run_id IN ?", runIDs).Delete(&operationModel{}).Error; err != nil {
			return err
		}
		if err := tx.Where("run_row_id IN ?", ids).Delete(&taskModel{}).Error; err != nil {
			return err
		}
		if err := tx.Where("id IN ?", ids).Delete(&runModel{}).Error; err != nil {
			return err
		}
		removed = len(ids)
		return nil
	})
	return removed, err
}

func counterIncrementExpression(db *gorm.DB) clause.Expr {
	table := db.Statement.Quote((counterModel{}).TableName())
	column := db.Statement.Quote("value")
	return gorm.Expr(fmt.Sprintf("%s.%s + ?", table, column), 1)
}

func (r *Repository) Prune(ctx context.Context, limit int) error {
	r.writeMu.Lock()
	defer r.writeMu.Unlock()
	return r.db.WithContext(ctx).Transaction(func(tx *gorm.DB) error {
		return r.prune(tx, limit)
	})
}

func (r *Repository) Query(ctx context.Context, query scheduler.HistoryQuery) ([]scheduler.Record, error) {
	limit := query.Limit
	if limit == 0 {
		limit = query.Tail
	}
	if limit < 0 {
		return nil, errors.New("history limit must be non-negative")
	}
	db := r.db.WithContext(ctx).Model(&runModel{}).Where("status IN ?", []string{
		scheduler.StatusSuccess, scheduler.StatusFailed, scheduler.StatusSkipped, scheduler.StatusCancelled, scheduler.StatusInterrupted,
	})
	if query.Status != "" {
		db = db.Where("status = ?", query.Status)
	}
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
	name := query.Target
	if name == "" {
		name = query.Name
	}
	if query.TargetType == "task" || (query.TargetType == "" && name != "") {
		taskDB := r.db.WithContext(ctx).Model(&taskModel{}).Select("run_row_id")
		if name != "" {
			taskDB = taskDB.Where("task = ?", name)
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
		if name != "" {
			db = db.Where("target = ?", name)
		}
	case "task":
		if name == "" {
			db = db.Where("target_type = ? OR id IN ?", "task", taskRunIDs)
		} else {
			db = db.Where("(target_type = ? AND target = ?) OR id IN ?", "task", name, taskRunIDs)
		}
	case "":
		if name != "" {
			db = db.Where("target = ? OR id IN ?", name, taskRunIDs)
		}
	}

	order := "started DESC, id DESC"
	if limit > 0 {
		db = db.Limit(limit)
	}
	var runs []runModel
	if err := db.Order(order).Find(&runs).Error; err != nil {
		return nil, err
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
	statuses := []string{scheduler.StatusSuccess, scheduler.StatusFailed, scheduler.StatusSkipped, scheduler.StatusCancelled, scheduler.StatusInterrupted}
	var boundary runModel
	err := tx.Model(&runModel{}).Where("status IN ?", statuses).Order("finished DESC, id DESC").Offset(limit).First(&boundary).Error
	if errors.Is(err, gorm.ErrRecordNotFound) {
		return nil
	}
	if err != nil {
		return err
	}
	var ids []int64
	if err := tx.Model(&runModel{}).Where("status IN ?", statuses).
		Where("finished < ? OR (finished = ? AND id <= ?)", boundary.Finished, boundary.Finished, boundary.ID).
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
	var runIDs []string
	if err := tx.Model(&runModel{}).Where("id IN ?", ids).Pluck("run_id", &runIDs).Error; err != nil {
		return err
	}
	if len(runIDs) > 0 {
		if err := tx.Where("run_id IN ?", runIDs).Delete(&eventModel{}).Error; err != nil {
			return err
		}
		if err := tx.Where("run_id IN ?", runIDs).Delete(&operationModel{}).Error; err != nil {
			return err
		}
	}
	return tx.Where("id IN ?", ids).Delete(&runModel{}).Error
}

func (r *Repository) loadRecords(ctx context.Context, runs []runModel, query scheduler.HistoryQuery) ([]scheduler.Record, error) {
	return r.loadRecordsFromDB(ctx, r.db, runs, query)
}

func (r *Repository) loadRecordsFromDB(ctx context.Context, db *gorm.DB, runs []runModel, query scheduler.HistoryQuery) ([]scheduler.Record, error) {
	if len(runs) == 0 {
		return []scheduler.Record{}, nil
	}
	db = db.WithContext(ctx)
	runIDs := make([]int64, 0, len(runs))
	for _, run := range runs {
		runIDs = append(runIDs, run.ID)
	}
	var tasks []taskModel
	taskDB := db.Where("run_row_id IN ?", runIDs)
	if err := taskDB.Order("started ASC, id ASC").Find(&tasks).Error; err != nil {
		return nil, err
	}

	var attempts []attemptModel
	if err := db.Where("run_row_id IN ?", runIDs).Order("started ASC, id ASC").Find(&attempts).Error; err != nil {
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
			RunID: compatibleRunID(run), Project: run.Project, Name: run.Name, TargetType: run.TargetType, Target: run.Target,
			Trigger: scheduler.TriggerRef{Type: run.TriggerType, Name: run.TriggerName, Mode: run.TriggerMode, EventID: run.EventID},
			Status:  run.Status, Started: run.Started, Finished: run.Finished, ExitCode: run.ExitCode,
			Error: run.Error, Stderr: run.Stderr, StdoutPath: run.StdoutPath, StderrPath: run.StderrPath,
			IdempotencyKey: run.IdempotencyKey, ConfigurationGeneration: run.ConfigurationGeneration,
			RetriedFromRunID: stringValue(run.RetriedFromRunID),
			Attempts:         cloneAttempts(attemptsByTask[0]),
		}
		for _, task := range tasksByRun[run.ID] {
			name := query.Target
			if name == "" {
				name = query.Name
			}
			if query.TargetType == "task" && name != "" && run.TargetType == "workflow" && task.Task != name {
				continue
			}
			record.Tasks = append(record.Tasks, scheduler.TaskRecord{
				RunID: task.TaskRunID, ParentRunID: task.ParentRunID, Node: task.Node, Task: task.Task,
				Command: task.Command, Args: decodeStrings(task.ArgsJSON), WorkingDir: task.WorkingDir,
				EnvKeys: decodeStrings(task.EnvKeysJSON), ArgsRedacted: task.ArgsRedacted, Status: task.Status,
				Started: task.Started, Finished: task.Finished, DurationSeconds: task.DurationSeconds,
				ExitCode: task.ExitCode, Error: task.Error, Stderr: task.Stderr,
				StdoutPath: task.StdoutPath, StderrPath: task.StderrPath,
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
		Error: record.Error, Stderr: record.Stderr, StdoutPath: record.StdoutPath, StderrPath: record.StderrPath,
		IdempotencyKey: record.IdempotencyKey, ConfigurationGeneration: record.ConfigurationGeneration,
		RetriedFromRunID: stringPointer(record.RetriedFromRunID),
	}
}

func stringPointer(value string) *string {
	if value == "" {
		return nil
	}
	return &value
}

func stringValue(value *string) string {
	if value == nil {
		return ""
	}
	return *value
}

func taskModelFromRecord(runID int64, task scheduler.TaskRecord) taskModel {
	return taskModel{
		RunRowID: runID, TaskRunID: task.RunID, ParentRunID: task.ParentRunID, Node: task.Node, Task: task.Task,
		Command: task.Command, ArgsJSON: encodeStrings(task.Args), WorkingDir: task.WorkingDir,
		EnvKeysJSON: encodeStrings(task.EnvKeys), ArgsRedacted: task.ArgsRedacted, Status: task.Status,
		Started: task.Started, Finished: task.Finished, DurationSeconds: task.DurationSeconds,
		ExitCode: task.ExitCode, Error: task.Error, Stderr: task.Stderr,
		StdoutPath: task.StdoutPath, StderrPath: task.StderrPath,
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
