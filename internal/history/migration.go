package history

import (
	"context"
	"crypto/rand"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"time"

	"github.com/glebarez/sqlite"
	"github.com/kevin93203/mango/internal/instance"
	"gorm.io/gorm"
	"gorm.io/gorm/clause"
	"gorm.io/gorm/logger"
)

const (
	migrationMarkerFormat = 1
	migrationPending      = "pending"
	migrationRunning      = "running"
	migrationCompleted    = "completed"
	migrationFailed       = "failed"
	migrationBlocked      = "blocked"
)

type migrationDefinition struct {
	Version int
	Name    string
	Input   string
}

var migrationDefinitions = []migrationDefinition{
	{Version: 1, Name: "core-history", Input: "mango-history-v1-core-history"},
	{Version: 2, Name: "execution-metadata", Input: "mango-history-v2-execution-metadata"},
	{Version: 3, Name: "retry-lineage", Input: "mango-history-v3-retry-lineage"},
	{Version: 4, Name: "remove-execution-operations", Input: "mango-history-v4-remove-execution-operations"},
	{Version: 5, Name: "artifacts", Input: "mango-history-v5-artifacts"},
	{Version: 6, Name: "schedule-occurrences", Input: "mango-history-v6-schedule-occurrences"},
	{Version: 7, Name: "webhook-deliveries", Input: "mango-history-v7-webhook-deliveries"},
	{Version: 8, Name: "workflow-policy", Input: "mango-history-v8-workflow-policy"},
	{Version: 9, Name: "schedule-occurrence-backfill", Input: "mango-history-v9-schedule-occurrence-backfill"},
	{Version: 10, Name: "observability", Input: "mango-history-v10-observability"},
	{Version: 11, Name: "migration-ledger", Input: "mango-history-v11-migration-ledger"},
}

// MigrationStatus is a read-only diagnostic view of SQLite migration state.
// It deliberately contains no DSN or credential fields.
type MigrationStatus struct {
	Status           string `json:"status"`
	CurrentVersion   int    `json:"current_version"`
	TargetVersion    int    `json:"target_version"`
	MarkerPath       string `json:"marker_path,omitempty"`
	BackupPath       string `json:"backup_path,omitempty"`
	ChecksumMismatch bool   `json:"checksum_mismatch,omitempty"`
	Error            string `json:"error,omitempty"`
	Recovery         string `json:"recovery,omitempty"`
}

// CurrentSchemaVersion returns the metadata schema understood by this build.
func CurrentSchemaVersion() int { return currentSchemaVersion }

type migrationMarker struct {
	FormatVersion int       `json:"format_version"`
	Status        string    `json:"status"`
	RunID         string    `json:"run_id"`
	DatabasePath  string    `json:"database_path"`
	SourceSchema  int       `json:"source_schema"`
	TargetSchema  int       `json:"target_schema"`
	LastCompleted int       `json:"last_completed_version"`
	SourceSHA256  string    `json:"source_sha256,omitempty"`
	FinalSHA256   string    `json:"final_sha256,omitempty"`
	BackupPath    string    `json:"backup_path,omitempty"`
	BackupSHA256  string    `json:"backup_sha256,omitempty"`
	StartedAt     time.Time `json:"started_at"`
	CompletedAt   time.Time `json:"completed_at,omitempty"`
	Error         string    `json:"error,omitempty"`
}

func migrationDefinitionFor(version int) (migrationDefinition, bool) {
	for _, definition := range migrationDefinitions {
		if definition.Version == version {
			return definition, true
		}
	}
	return migrationDefinition{}, false
}

func migrationChecksum(definition migrationDefinition) string {
	digest := sha256.Sum256([]byte(definition.Input))
	return hex.EncodeToString(digest[:])
}

func runMigrationCoordinator(db *gorm.DB, driver, path string) error {
	return db.WithContext(context.Background()).Connection(func(connection *gorm.DB) error {
		release, err := acquireMigrationLock(connection, driver, path)
		if err != nil {
			return err
		}
		defer func() { _ = release() }()

		if driver == "sqlite" {
			return runSQLiteMigrations(connection, path)
		}
		return runExternalMigrations(connection, driver)
	})
}

func acquireMigrationLock(db *gorm.DB, driver, path string) (func() error, error) {
	switch driver {
	case "sqlite":
		if path == "" {
			return nil, errors.New("SQLite migration path is empty")
		}
		absolute, err := filepath.Abs(path)
		if err != nil {
			return nil, fmt.Errorf("resolve SQLite migration lock path: %w", err)
		}
		lockPath := absolute + ".migration.lock"
		if err := os.MkdirAll(filepath.Dir(lockPath), 0o700); err != nil {
			return nil, fmt.Errorf("create migration lock directory: %w", err)
		}
		lock, err := instance.Acquire(lockPath)
		if err != nil {
			return nil, fmt.Errorf("acquire SQLite migration lock: %w", err)
		}
		return lock.Close, nil
	case "postgres":
		if err := db.Exec("SELECT pg_advisory_lock(hashtext('mango:history:migration'))").Error; err != nil {
			return nil, fmt.Errorf("acquire PostgreSQL migration lock: %w", err)
		}
		return func() error {
			return db.Exec("SELECT pg_advisory_unlock(hashtext('mango:history:migration'))").Error
		}, nil
	case "mysql":
		var acquired int
		if err := db.Raw("SELECT GET_LOCK(?, 0)", "mango:history:migration").Scan(&acquired).Error; err != nil {
			return nil, fmt.Errorf("acquire MySQL migration lock: %w", err)
		}
		if acquired != 1 {
			return nil, errors.New("acquire MySQL migration lock: another migration is running")
		}
		return func() error {
			var released int
			return db.Raw("SELECT RELEASE_LOCK(?)", "mango:history:migration").Scan(&released).Error
		}, nil
	default:
		return nil, fmt.Errorf("unsupported migration driver %q", driver)
	}
}

func runSQLiteMigrations(db *gorm.DB, path string) error {
	path, err := filepath.Abs(path)
	if err != nil {
		return fmt.Errorf("resolve SQLite migration path: %w", err)
	}
	markerPath := path + ".migration.json"
	marker, markerExists, err := readMigrationMarker(markerPath)
	if err != nil {
		return err
	}
	version, err := readSchemaVersion(db)
	if err != nil {
		return err
	}
	if version > currentSchemaVersion {
		return fmt.Errorf("history schema version %d is newer than supported version %d", version, currentSchemaVersion)
	}
	if err := verifyMigrationLedger(db); err != nil {
		return err
	}
	if markerExists {
		if err := validateSQLiteMarker(db, path, marker, version); err != nil {
			return err
		}
	}

	if version == currentSchemaVersion {
		if err := verifyRequiredSchema(db); err != nil {
			return err
		}
		if markerExists && marker.Status != migrationCompleted {
			marker.Status = migrationCompleted
			marker.LastCompleted = version
			marker.TargetSchema = currentSchemaVersion
			marker.CompletedAt = time.Now().UTC()
			marker.Error = ""
			if err := writeMigrationMarker(markerPath, marker); err != nil {
				return err
			}
		}
		return nil
	}

	if err := checkpointSQLite(db, path); err != nil {
		return err
	}
	sourceSHA, err := sha256File(path)
	if err != nil {
		return err
	}

	runID := marker.RunID
	if runID == "" {
		runID, err = newMigrationRunID()
		if err != nil {
			return err
		}
	}
	backupPath := marker.BackupPath
	backupSHA := marker.BackupSHA256
	if backupPath != "" {
		actual, checksumErr := sha256File(backupPath)
		if checksumErr != nil || backupSHA == "" || actual != backupSHA {
			return fmt.Errorf("migration backup checksum mismatch for %q", backupPath)
		}
	} else if sourceSHA != "" {
		empty, emptyErr := sqliteDatabaseEmpty(db)
		if emptyErr != nil {
			return emptyErr
		}
		if version == 0 && empty {
			// A newly-created SQLite file has SQLite header pages but no user
			// data. Do not create an operator backup for a fresh database.
			backupPath = ""
			backupSHA = ""
		} else {
			backupPath = path + ".migration-" + runID + ".bak"
			if err := copyFile(path, backupPath); err != nil {
				return fmt.Errorf("create SQLite migration backup: %w", err)
			}
			backupSHA, err = sha256File(backupPath)
			if err != nil {
				return err
			}
			// Preserve the historical backup name for operators and older runbooks,
			// but never overwrite an existing backup.
			legacyBackup := path + ".bak"
			if _, statErr := os.Stat(legacyBackup); errors.Is(statErr, os.ErrNotExist) {
				if err := copyFile(backupPath, legacyBackup); err != nil {
					return fmt.Errorf("create legacy SQLite migration backup: %w", err)
				}
			} else if statErr != nil {
				return statErr
			}
		}
	}

	sourceSchema := version
	markerSourceSHA := sourceSHA
	if markerExists && marker.SourceSchema > 0 {
		sourceSchema = marker.SourceSchema
		if marker.SourceSHA256 != "" {
			markerSourceSHA = marker.SourceSHA256
		}
	}
	marker = migrationMarker{
		FormatVersion: migrationMarkerFormat, Status: migrationRunning, RunID: runID,
		DatabasePath: path, SourceSchema: sourceSchema, TargetSchema: currentSchemaVersion,
		LastCompleted: version, SourceSHA256: markerSourceSHA, BackupPath: backupPath,
		BackupSHA256: backupSHA, StartedAt: time.Now().UTC(),
	}
	if markerExists && marker.Status == migrationFailed {
		marker.Status = migrationRunning
		marker.StartedAt = time.Now().UTC()
		marker.CompletedAt = time.Time{}
		marker.Error = ""
	}
	if err := ensureSchemaVersionTable(db); err != nil {
		return markMigrationFailed(markerPath, marker, err)
	}
	if err := writeMigrationMarker(markerPath, marker); err != nil {
		return err
	}

	err = runMigrationSteps(db, version, func(completed int) error {
		marker.LastCompleted = completed
		return writeMigrationMarker(markerPath, marker)
	})
	if err != nil {
		return markMigrationFailed(markerPath, marker, err)
	}
	finalVersion, err := readSchemaVersion(db)
	if err != nil {
		return markMigrationFailed(markerPath, marker, err)
	}
	if finalVersion != currentSchemaVersion {
		return markMigrationFailed(markerPath, marker, fmt.Errorf("migration completed at schema version %d, want %d", finalVersion, currentSchemaVersion))
	}
	if err := verifyRequiredSchema(db); err != nil {
		return markMigrationFailed(markerPath, marker, err)
	}
	if err := verifyMigrationLedger(db); err != nil {
		return markMigrationFailed(markerPath, marker, err)
	}
	marker.Status = migrationCompleted
	marker.LastCompleted = finalVersion
	marker.FinalSHA256, err = sha256File(path)
	if err != nil {
		return markMigrationFailed(markerPath, marker, err)
	}
	marker.CompletedAt = time.Now().UTC()
	marker.Error = ""
	if err := writeMigrationMarker(markerPath, marker); err != nil {
		return err
	}
	return nil
}

func runExternalMigrations(db *gorm.DB, driver string) error {
	version, err := readSchemaVersion(db)
	if err != nil {
		return err
	}
	if version > currentSchemaVersion {
		return fmt.Errorf("history schema version %d is newer than supported version %d", version, currentSchemaVersion)
	}
	if version == currentSchemaVersion {
		if err := verifyRequiredSchema(db); err != nil {
			return fmt.Errorf("%s history schema verification failed: %w", driver, err)
		}
		return verifyMigrationLedger(db)
	}
	if err := ensureSchemaVersionTable(db); err != nil {
		return err
	}
	if err := runMigrationSteps(db, version, nil); err != nil {
		return fmt.Errorf("%s history migration failed; restore the operator backup before retrying: %w", driver, err)
	}
	if err := verifyRequiredSchema(db); err != nil {
		return fmt.Errorf("%s history schema verification failed: %w", driver, err)
	}
	return verifyMigrationLedger(db)
}

func runMigrationSteps(db *gorm.DB, version int, progress func(int) error) error {
	for next := version + 1; next <= currentSchemaVersion; next++ {
		definition, ok := migrationDefinitionFor(next)
		if !ok {
			return fmt.Errorf("missing migration definition %d", next)
		}
		err := db.Transaction(func(tx *gorm.DB) error {
			if err := applyMigration(tx, next); err != nil {
				return fmt.Errorf("apply %s: %w", definition.Name, err)
			}
			row := schemaVersionModel{ID: 1, Version: next, AppliedAt: time.Now().UTC()}
			if err := tx.Save(&row).Error; err != nil {
				return fmt.Errorf("record schema version %d: %w", next, err)
			}
			if err := recordMigrationLedger(tx, definition); err != nil {
				return fmt.Errorf("record migration ledger %d: %w", next, err)
			}
			return nil
		})
		if err != nil {
			return err
		}
		if progress != nil {
			if err := progress(next); err != nil {
				return fmt.Errorf("record migration progress %d: %w", next, err)
			}
		}
	}
	return nil
}

func ensureSchemaVersionTable(db *gorm.DB) error {
	if db.Migrator().HasTable(&schemaVersionModel{}) {
		return nil
	}
	if err := db.Migrator().CreateTable(&schemaVersionModel{}); err != nil {
		return fmt.Errorf("create schema version table: %w", err)
	}
	return nil
}

func readSchemaVersion(db *gorm.DB) (int, error) {
	if !db.Migrator().HasTable(&schemaVersionModel{}) {
		return 0, nil
	}
	var row schemaVersionModel
	result := db.Session(&gorm.Session{NewDB: true}).Where("id = ?", 1).First(&row)
	if errors.Is(result.Error, gorm.ErrRecordNotFound) {
		return 0, nil
	}
	if result.Error != nil {
		return 0, result.Error
	}
	return row.Version, nil
}

func recordMigrationLedger(db *gorm.DB, definition migrationDefinition) error {
	if !db.Migrator().HasTable(&schemaMigrationModel{}) {
		return nil
	}
	if definition.Version == currentSchemaVersion {
		for _, previous := range migrationDefinitions {
			if previous.Version >= definition.Version {
				break
			}
			row := schemaMigrationModel{Version: previous.Version, Name: previous.Name, Checksum: migrationChecksum(previous), AppliedAt: time.Now().UTC()}
			if err := db.Clauses(clause.OnConflict{Columns: []clause.Column{{Name: "version"}}, DoNothing: true}).Create(&row).Error; err != nil {
				return err
			}
		}
	}
	row := schemaMigrationModel{Version: definition.Version, Name: definition.Name, Checksum: migrationChecksum(definition), AppliedAt: time.Now().UTC()}
	return db.Clauses(clause.OnConflict{Columns: []clause.Column{{Name: "version"}}, DoUpdates: clause.AssignmentColumns([]string{"name", "checksum", "applied_at"})}).Create(&row).Error
}

func verifyMigrationLedger(db *gorm.DB) error {
	if !db.Migrator().HasTable(&schemaMigrationModel{}) {
		return nil
	}
	var rows []schemaMigrationModel
	query := db.Session(&gorm.Session{NewDB: true}).Model(&schemaMigrationModel{}).Order("version ASC")
	if err := query.Find(&rows).Error; err != nil {
		return err
	}
	for _, row := range rows {
		definition, ok := migrationDefinitionFor(row.Version)
		if !ok || row.Name != definition.Name || row.Checksum != migrationChecksum(definition) {
			wantName, wantChecksum := "<unknown>", "<unknown>"
			if ok {
				wantName, wantChecksum = definition.Name, migrationChecksum(definition)
			}
			return fmt.Errorf("migration ledger checksum mismatch at version %d: got name=%q checksum=%q want name=%q checksum=%q", row.Version, row.Name, row.Checksum, wantName, wantChecksum)
		}
	}
	version, err := readSchemaVersion(db)
	if err != nil {
		return err
	}
	if version == currentSchemaVersion {
		seen := make(map[int]struct{}, len(rows))
		for _, row := range rows {
			seen[row.Version] = struct{}{}
		}
		for expected := 1; expected <= currentSchemaVersion; expected++ {
			if _, ok := seen[expected]; !ok {
				return fmt.Errorf("migration ledger is incomplete: version %d is missing", expected)
			}
		}
	}
	return nil
}

func sqliteDatabaseEmpty(db *gorm.DB) (bool, error) {
	var count int64
	if err := db.Session(&gorm.Session{NewDB: true}).Raw(`
		SELECT COUNT(*)
		FROM sqlite_master
		WHERE name NOT LIKE 'sqlite_%'
		  AND type IN ('table', 'index', 'trigger', 'view')`).Scan(&count).Error; err != nil {
		return false, fmt.Errorf("inspect SQLite database contents: %w", err)
	}
	return count == 0, nil
}

func verifyRequiredSchema(db *gorm.DB) error {
	migrator := db.Migrator()
	tables := []struct {
		name  string
		model interface{}
	}{
		{name: "history_runs", model: &runModel{}}, {name: "history_tasks", model: &taskModel{}},
		{name: "history_attempts", model: &attemptModel{}}, {name: "history_counters", model: &counterModel{}},
		{name: "history_artifacts", model: &artifactModel{}}, {name: "schedule_occurrences", model: &scheduleOccurrenceModel{}},
		{name: "webhook_deliveries", model: &webhookDeliveryModel{}}, {name: "execution_events", model: &eventModel{}},
		{name: "events", model: &observabilityEventModel{}}, {name: "audit_entries", model: &auditModel{}},
		{name: "secret_references", model: &secretReferenceModel{}}, {name: "resource_policies", model: &resourcePolicyModel{}},
		{name: "schema_version", model: &schemaVersionModel{}}, {name: "schema_migrations", model: &schemaMigrationModel{}},
	}
	for _, table := range tables {
		if !migrator.HasTable(table.model) {
			return fmt.Errorf("required history table %q is missing", table.name)
		}
	}
	indexes := []struct {
		table string
		model interface{}
		name  string
	}{
		{table: "history_runs", model: &runModel{}, name: "idx_history_runs_run_id"},
		{table: "history_runs", model: &runModel{}, name: "idx_history_runs_retried_from_run_id"},
		{table: "history_tasks", model: &taskModel{}, name: "idx_history_tasks_task_run_id"},
		{table: "history_attempts", model: &attemptModel{}, name: "idx_history_attempts_task_row_id"},
		{table: "history_artifacts", model: &artifactModel{}, name: "idx_history_artifacts_task_row_id"},
		{table: "schedule_occurrences", model: &scheduleOccurrenceModel{}, name: "idx_schedule_occurrence_slot"},
		{table: "webhook_deliveries", model: &webhookDeliveryModel{}, name: "idx_webhook_delivery_identity"},
		{table: "execution_events", model: &eventModel{}, name: "idx_execution_events_run_id"},
		{table: "events", model: &observabilityEventModel{}, name: "idx_events_timestamp"},
		{table: "audit_entries", model: &auditModel{}, name: "idx_audit_entries_timestamp"},
		{table: "secret_references", model: &secretReferenceModel{}, name: "idx_secret_references_project"},
		{table: "resource_policies", model: &resourcePolicyModel{}, name: "idx_resource_policies_project"},
		{table: "schema_migrations", model: &schemaMigrationModel{}, name: "idx_schema_migrations_applied_at"},
	}
	for _, index := range indexes {
		if !migrator.HasIndex(index.model, index.name) {
			return fmt.Errorf("required history index %q on %s is missing", index.name, index.table)
		}
	}
	return nil
}

func validateSQLiteMarker(db *gorm.DB, path string, marker migrationMarker, current int) error {
	if marker.DatabasePath != "" && marker.DatabasePath != path {
		return fmt.Errorf("SQLite migration marker belongs to %q, not %q", marker.DatabasePath, path)
	}
	if marker.TargetSchema != 0 && marker.TargetSchema != currentSchemaVersion {
		return fmt.Errorf("migration marker target schema %d is not supported by this binary", marker.TargetSchema)
	}
	if marker.Status != migrationRunning && marker.Status != migrationFailed && marker.Status != migrationCompleted && marker.Status != migrationPending {
		return fmt.Errorf("unsupported migration marker status %q", marker.Status)
	}
	if marker.SourceSchema > 0 && marker.BackupPath == "" {
		return errors.New("migration marker is missing the required upgrade backup")
	}
	if marker.BackupPath != "" {
		actual, err := sha256File(marker.BackupPath)
		if err != nil || marker.BackupSHA256 == "" || actual != marker.BackupSHA256 {
			return fmt.Errorf("migration backup checksum mismatch for %q", marker.BackupPath)
		}
	}
	if marker.Status == migrationCompleted {
		if current != currentSchemaVersion {
			return fmt.Errorf("completed migration marker is inconsistent with schema version %d", current)
		}
		return nil
	}
	if current == currentSchemaVersion {
		if marker.LastCompleted != currentSchemaVersion {
			return fmt.Errorf("migration marker reports last completed version %d but database is at %d", marker.LastCompleted, current)
		}
		return nil
	}
	if current < marker.LastCompleted {
		return fmt.Errorf("migration marker is inconsistent: database schema is %d but marker last completed version is %d", current, marker.LastCompleted)
	}
	if current == marker.SourceSchema && marker.SourceSHA256 != "" {
		actual, err := sha256File(path)
		if err != nil || actual != marker.SourceSHA256 {
			return fmt.Errorf("database checksum does not match migration marker; restore %q before retrying", marker.BackupPath)
		}
	}
	return nil
}

func checkpointSQLite(db *gorm.DB, path string) error {
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
	if err := db.Exec("PRAGMA wal_checkpoint(TRUNCATE)").Error; err != nil {
		return fmt.Errorf("checkpoint SQLite database %q: %w", path, err)
	}
	return nil
}

func newMigrationRunID() (string, error) {
	data := make([]byte, 16)
	if _, err := rand.Read(data); err != nil {
		return "", fmt.Errorf("generate migration run id: %w", err)
	}
	return hex.EncodeToString(data), nil
}

func sha256File(path string) (string, error) {
	file, err := os.Open(path)
	if errors.Is(err, os.ErrNotExist) {
		return "", nil
	}
	if err != nil {
		return "", err
	}
	defer file.Close()
	hash := sha256.New()
	if _, err := io.Copy(hash, file); err != nil {
		return "", fmt.Errorf("hash %q: %w", path, err)
	}
	return hex.EncodeToString(hash.Sum(nil)), nil
}

func copyFile(source, destination string) error {
	input, err := os.Open(source)
	if err != nil {
		return err
	}
	defer input.Close()
	if err := os.MkdirAll(filepath.Dir(destination), 0o700); err != nil {
		return err
	}
	output, err := os.OpenFile(destination, os.O_CREATE|os.O_WRONLY|os.O_EXCL, 0o600)
	if err != nil {
		return err
	}
	ok := false
	defer func() {
		if !ok {
			_ = output.Close()
			_ = os.Remove(destination)
		}
	}()
	if _, err := io.Copy(output, input); err != nil {
		return err
	}
	if err := output.Sync(); err != nil {
		return err
	}
	if err := output.Close(); err != nil {
		return err
	}
	if err := syncDirectory(filepath.Dir(destination)); err != nil && runtime.GOOS != "windows" {
		return err
	}
	ok = true
	return nil
}

func readMigrationMarker(path string) (migrationMarker, bool, error) {
	data, err := os.ReadFile(path)
	if errors.Is(err, os.ErrNotExist) {
		return migrationMarker{}, false, nil
	}
	if err != nil {
		return migrationMarker{}, false, fmt.Errorf("read migration marker: %w", err)
	}
	var marker migrationMarker
	if err := json.Unmarshal(data, &marker); err != nil {
		return migrationMarker{}, false, fmt.Errorf("decode migration marker: %w", err)
	}
	if marker.FormatVersion != migrationMarkerFormat {
		return migrationMarker{}, false, fmt.Errorf("unsupported migration marker format %d", marker.FormatVersion)
	}
	return marker, true, nil
}

func writeMigrationMarker(path string, marker migrationMarker) error {
	data, err := json.MarshalIndent(marker, "", "  ")
	if err != nil {
		return err
	}
	if err := os.MkdirAll(filepath.Dir(path), 0o700); err != nil {
		return err
	}
	temp, err := os.CreateTemp(filepath.Dir(path), ".migration-*.tmp")
	if err != nil {
		return err
	}
	tempPath := temp.Name()
	defer os.Remove(tempPath)
	if err := temp.Chmod(0o600); err != nil {
		_ = temp.Close()
		return err
	}
	if _, err := temp.Write(append(data, '\n')); err != nil {
		_ = temp.Close()
		return err
	}
	if err := temp.Sync(); err != nil {
		_ = temp.Close()
		return err
	}
	if err := temp.Close(); err != nil {
		return err
	}
	if err := os.Rename(tempPath, path); err != nil {
		return err
	}
	if err := syncDirectory(filepath.Dir(path)); err != nil && runtime.GOOS != "windows" {
		return err
	}
	return nil
}

func syncDirectory(path string) error {
	directory, err := os.Open(path)
	if err != nil {
		return err
	}
	defer directory.Close()
	return directory.Sync()
}

func markMigrationFailed(path string, marker migrationMarker, migrationErr error) error {
	marker.Status = migrationFailed
	marker.Error = migrationErr.Error()
	marker.CompletedAt = time.Time{}
	if err := writeMigrationMarker(path, marker); err != nil {
		return fmt.Errorf("%w; also failed to write migration marker: %v", migrationErr, err)
	}
	return migrationErr
}

// InspectSQLiteMigration reads migration state without running migrations.
// It is used by mango doctor when the daemon cannot start.
func InspectSQLiteMigration(path string) (MigrationStatus, error) {
	absolute, err := filepath.Abs(path)
	if err != nil {
		return MigrationStatus{}, err
	}
	result := MigrationStatus{Status: "not_created", TargetVersion: currentSchemaVersion, MarkerPath: absolute + ".migration.json"}
	marker, markerExists, markerErr := readMigrationMarker(result.MarkerPath)
	if markerErr != nil {
		result.Status = migrationBlocked
		result.Error = markerErr.Error()
		result.Recovery = "repair or remove the invalid marker only after validating the database backup"
		return result, nil
	}
	if markerExists {
		result.Status = marker.Status
		result.BackupPath = marker.BackupPath
		result.TargetVersion = marker.TargetSchema
		if result.TargetVersion == 0 {
			result.TargetVersion = currentSchemaVersion
		}
		result.Error = marker.Error
	}
	if _, err := os.Stat(absolute); errors.Is(err, os.ErrNotExist) {
		if markerExists && marker.Status != migrationCompleted {
			result.Status = migrationBlocked
			result.Error = "migration database is missing"
			result.Recovery = "restore the migration backup before starting mangod"
		}
		return result, nil
	} else if err != nil {
		return result, err
	}

	readOnlyDSN := "file:" + filepath.ToSlash(absolute) + "?mode=ro"
	db, err := gorm.Open(sqlite.Open(readOnlyDSN), &gorm.Config{Logger: logger.Default.LogMode(logger.Silent)})
	if err != nil {
		result.Status = migrationBlocked
		result.Error = fmt.Sprintf("open SQLite database read-only for inspection: %v", err)
		result.Recovery = "inspect or restore the migration backup"
		return result, nil
	}
	sqlDB, sqlErr := db.DB()
	if sqlErr == nil {
		defer sqlDB.Close()
	}
	_ = db.Exec("PRAGMA query_only = ON").Error
	current, err := readSchemaVersion(db)
	if err != nil {
		result.Status = migrationBlocked
		result.Error = fmt.Sprintf("read SQLite schema version: %v", err)
		result.Recovery = "inspect or restore the migration backup"
		return result, nil
	}
	result.CurrentVersion = current
	if current > currentSchemaVersion {
		result.Status = migrationBlocked
		result.Error = fmt.Sprintf("history schema version %d is newer than supported version %d", current, currentSchemaVersion)
		result.Recovery = "use a newer Mango binary; database downgrade is unsupported"
		return result, nil
	}
	if err := verifyMigrationLedger(db); err != nil {
		result.Status = migrationBlocked
		result.ChecksumMismatch = strings.Contains(err.Error(), "checksum") || strings.Contains(err.Error(), "ledger")
		result.Error = err.Error()
		result.Recovery = "restore the verified backup or use a binary that supports this schema"
		return result, nil
	}
	if markerExists && marker.Status == migrationCompleted && current != currentSchemaVersion {
		result.Status = migrationBlocked
		result.Error = fmt.Sprintf("completed migration marker is inconsistent with schema version %d", current)
		result.Recovery = "restore the marker backup before retrying"
		return result, nil
	}
	if markerExists && marker.SourceSchema > 0 && marker.BackupPath == "" {
		result.Status = migrationBlocked
		result.Error = "migration marker is missing the required upgrade backup"
		result.Recovery = "restore the database from an operator backup before retrying"
		return result, nil
	}
	if !markerExists {
		if current >= currentSchemaVersion {
			result.Status = "complete"
		} else {
			result.Status = migrationPending
		}
		return result, nil
	}
	if marker.BackupPath != "" && marker.BackupSHA256 != "" {
		backupHash, hashErr := sha256File(marker.BackupPath)
		if hashErr != nil || backupHash != marker.BackupSHA256 {
			result.Status = migrationBlocked
			result.ChecksumMismatch = true
			result.Error = fmt.Sprintf("migration backup checksum mismatch for %q", marker.BackupPath)
			result.Recovery = "restore a verified backup before retrying"
			return result, nil
		}
	}
	if marker.SourceSHA256 != "" && marker.LastCompleted == current && current < marker.TargetSchema {
		databaseHash, hashErr := sha256File(absolute)
		if hashErr != nil || databaseHash != marker.SourceSHA256 {
			result.Status = migrationBlocked
			result.ChecksumMismatch = true
			result.Error = "database changed after an incomplete migration"
			result.Recovery = "restore the marker backup, then retry mangod"
			return result, nil
		}
	}
	if marker.Status == migrationFailed || marker.Status == migrationBlocked {
		result.Recovery = "restore the verified backup if needed, then retry mangod"
	} else if marker.Status == migrationRunning {
		result.Recovery = "the next mangod start will resume the idempotent migration"
	}
	return result, nil
}
