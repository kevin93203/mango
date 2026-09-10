package history

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/glebarez/sqlite"
	"gorm.io/gorm"
	"gorm.io/gorm/logger"
)

func openMigrationTestDB(t *testing.T, path string) *gorm.DB {
	t.Helper()
	db, err := gorm.Open(sqlite.Open(path), &gorm.Config{Logger: logger.Default.LogMode(logger.Silent)})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		sqlDB, dbErr := db.DB()
		if dbErr == nil {
			_ = sqlDB.Close()
		}
	})
	return db
}

func createLegacySchema(t *testing.T, path string, target int) {
	t.Helper()
	db := openMigrationTestDB(t, path)
	if err := ensureSchemaVersionTable(db); err != nil {
		t.Fatal(err)
	}
	for version := 1; version <= target; version++ {
		if err := applyMigration(db, version); err != nil {
			t.Fatalf("apply legacy migration %d: %v", version, err)
		}
		if err := db.Save(&schemaVersionModel{ID: 1, Version: version, AppliedAt: time.Now().UTC()}).Error; err != nil {
			t.Fatalf("save legacy schema version %d: %v", version, err)
		}
	}
	sqlDB, err := db.DB()
	if err != nil {
		t.Fatal(err)
	}
	if err := sqlDB.Close(); err != nil {
		t.Fatal(err)
	}
}

func TestSQLiteSchemaVersionsMigrateToEleven(t *testing.T) {
	for _, source := range []int{1, 3, 9, 10} {
		t.Run(fmt.Sprintf("v%d", source), func(t *testing.T) {
			path := filepath.Join(t.TempDir(), "history.db")
			createLegacySchema(t, path, source)

			repository, err := Open(Config{Driver: "sqlite", Path: path})
			if err != nil {
				t.Fatal(err)
			}
			defer repository.Close()

			version, err := readSchemaVersion(repository.db)
			if err != nil || version != currentSchemaVersion {
				t.Fatalf("schema version = %d, err=%v, want %d", version, err, currentSchemaVersion)
			}
			var ledgerRows int64
			if err := repository.db.Model(&schemaMigrationModel{}).Count(&ledgerRows).Error; err != nil {
				t.Fatal(err)
			}
			if ledgerRows != int64(currentSchemaVersion) {
				t.Fatalf("ledger rows = %d, want %d", ledgerRows, currentSchemaVersion)
			}
			marker, exists, err := readMigrationMarker(path + ".migration.json")
			if err != nil || !exists || marker.Status != migrationCompleted {
				t.Fatalf("migration marker = %+v, exists=%v, err=%v", marker, exists, err)
			}
			if marker.SourceSchema != source || marker.TargetSchema != currentSchemaVersion || marker.BackupSHA256 == "" {
				t.Fatalf("migration marker metadata = %+v", marker)
			}
			if _, err := os.Stat(marker.BackupPath); err != nil {
				t.Fatalf("run-specific backup = %q: %v", marker.BackupPath, err)
			}
		})
	}
}

func TestSQLiteFreshMigrationDoesNotCreateBackupAndReopenIsIdempotent(t *testing.T) {
	path := filepath.Join(t.TempDir(), "history.db")
	repository, err := Open(Config{Driver: "sqlite", Path: path})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(path + ".bak"); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("fresh database backup = %v, want no backup", err)
	}
	if err := repository.Close(); err != nil {
		t.Fatal(err)
	}

	repository, err = Open(Config{Driver: "sqlite", Path: path})
	if err != nil {
		t.Fatal(err)
	}
	defer repository.Close()
	var rows []schemaMigrationModel
	if err := repository.db.Order("version ASC").Find(&rows).Error; err != nil {
		t.Fatal(err)
	}
	if len(rows) != currentSchemaVersion {
		t.Fatalf("ledger rows after reopen = %d, want %d", len(rows), currentSchemaVersion)
	}
	marker, exists, err := readMigrationMarker(path + ".migration.json")
	if err != nil || !exists || marker.Status != migrationCompleted {
		t.Fatalf("reopen marker = %+v, exists=%v, err=%v", marker, exists, err)
	}
}

func TestSQLiteMigrationPreservesExistingCompatibilityBackup(t *testing.T) {
	path := filepath.Join(t.TempDir(), "history.db")
	createLegacySchema(t, path, 10)
	legacyBackup := []byte("operator-owned backup")
	if err := os.WriteFile(path+".bak", legacyBackup, 0o600); err != nil {
		t.Fatal(err)
	}
	repository, err := Open(Config{Driver: "sqlite", Path: path})
	if err != nil {
		t.Fatal(err)
	}
	defer repository.Close()
	got, err := os.ReadFile(path + ".bak")
	if err != nil {
		t.Fatal(err)
	}
	if string(got) != string(legacyBackup) {
		t.Fatalf("compatibility backup = %q, want it unchanged", got)
	}
}

func TestSQLiteMigrationRetryAfterCommittedStepWithStaleMarker(t *testing.T) {
	path := filepath.Join(t.TempDir(), "history.db")
	createLegacySchema(t, path, 9)
	sourceSHA, err := sha256File(path)
	if err != nil {
		t.Fatal(err)
	}
	backupPath := path + ".migration-retry.bak"
	if err := copyFile(path, backupPath); err != nil {
		t.Fatal(err)
	}
	backupSHA, err := sha256File(backupPath)
	if err != nil {
		t.Fatal(err)
	}
	marker := migrationMarker{
		FormatVersion: migrationMarkerFormat, Status: migrationRunning, RunID: "retry-run",
		DatabasePath: path, SourceSchema: 9, TargetSchema: currentSchemaVersion,
		LastCompleted: 9, SourceSHA256: sourceSHA, BackupPath: backupPath, BackupSHA256: backupSHA,
		StartedAt: time.Now().UTC(),
	}
	if err := writeMigrationMarker(path+".migration.json", marker); err != nil {
		t.Fatal(err)
	}

	// Simulate a process dying after the v10 transaction committed but before
	// the marker progress write. The ledger is still absent, so v11 must
	// backfill it and finish the upgrade without replaying v10.
	db := openMigrationTestDB(t, path)
	if err := applyMigration(db, 10); err != nil {
		t.Fatal(err)
	}
	if err := db.Save(&schemaVersionModel{ID: 1, Version: 10, AppliedAt: time.Now().UTC()}).Error; err != nil {
		t.Fatal(err)
	}
	sqlDB, err := db.DB()
	if err != nil {
		t.Fatal(err)
	}
	if err := sqlDB.Close(); err != nil {
		t.Fatal(err)
	}

	repository, err := Open(Config{Driver: "sqlite", Path: path})
	if err != nil {
		t.Fatal(err)
	}
	defer repository.Close()
	version, err := readSchemaVersion(repository.db)
	if err != nil || version != currentSchemaVersion {
		t.Fatalf("retried schema version = %d, err=%v", version, err)
	}
	completed, exists, err := readMigrationMarker(path + ".migration.json")
	if err != nil || !exists || completed.Status != migrationCompleted || completed.RunID != "retry-run" {
		t.Fatalf("retried marker = %+v, exists=%v, err=%v", completed, exists, err)
	}
}

func TestSQLiteMigrationBlocksMissingBackupAndChecksumMismatch(t *testing.T) {
	t.Run("stale completed marker", func(t *testing.T) {
		path := filepath.Join(t.TempDir(), "history.db")
		createLegacySchema(t, path, 10)
		backupPath := path + ".stale.bak"
		if err := copyFile(path, backupPath); err != nil {
			t.Fatal(err)
		}
		backupSHA, err := sha256File(backupPath)
		if err != nil {
			t.Fatal(err)
		}
		marker := migrationMarker{FormatVersion: migrationMarkerFormat, Status: migrationCompleted, RunID: "stale", DatabasePath: path, SourceSchema: 10, TargetSchema: currentSchemaVersion, LastCompleted: 10, BackupPath: backupPath, BackupSHA256: backupSHA, StartedAt: time.Now().UTC(), CompletedAt: time.Now().UTC()}
		if err := writeMigrationMarker(path+".migration.json", marker); err != nil {
			t.Fatal(err)
		}
		if _, err := Open(Config{Driver: "sqlite", Path: path}); err == nil || !strings.Contains(err.Error(), "completed migration marker") {
			t.Fatalf("stale marker open error = %v", err)
		}
	})

	t.Run("missing backup", func(t *testing.T) {
		path := filepath.Join(t.TempDir(), "history.db")
		createLegacySchema(t, path, 10)
		marker := migrationMarker{FormatVersion: migrationMarkerFormat, Status: migrationRunning, RunID: "missing-backup", DatabasePath: path, SourceSchema: 10, TargetSchema: currentSchemaVersion, LastCompleted: 10, BackupPath: path + ".missing", BackupSHA256: "missing", StartedAt: time.Now().UTC()}
		if err := writeMigrationMarker(path+".migration.json", marker); err != nil {
			t.Fatal(err)
		}
		if _, err := Open(Config{Driver: "sqlite", Path: path}); err == nil || !strings.Contains(err.Error(), "backup checksum mismatch") {
			t.Fatalf("missing backup open error = %v", err)
		}
	})

	t.Run("database checksum", func(t *testing.T) {
		path := filepath.Join(t.TempDir(), "history.db")
		createLegacySchema(t, path, 10)
		sourceSHA, err := sha256File(path)
		if err != nil {
			t.Fatal(err)
		}
		backupPath := path + ".migration-checksum.bak"
		if err := copyFile(path, backupPath); err != nil {
			t.Fatal(err)
		}
		backupSHA, err := sha256File(backupPath)
		if err != nil {
			t.Fatal(err)
		}
		// Change the database after the source checksum was recorded while
		// keeping its schema at v10.
		db := openMigrationTestDB(t, path)
		if err := db.Exec("UPDATE schema_version SET applied_at = ? WHERE id = 1", time.Now().UTC()).Error; err != nil {
			t.Fatal(err)
		}
		sqlDB, err := db.DB()
		if err != nil {
			t.Fatal(err)
		}
		if err := sqlDB.Close(); err != nil {
			t.Fatal(err)
		}
		marker := migrationMarker{FormatVersion: migrationMarkerFormat, Status: migrationFailed, RunID: "checksum-mismatch", DatabasePath: path, SourceSchema: 10, TargetSchema: currentSchemaVersion, LastCompleted: 10, SourceSHA256: sourceSHA, BackupPath: backupPath, BackupSHA256: backupSHA, StartedAt: time.Now().UTC()}
		if err := writeMigrationMarker(path+".migration.json", marker); err != nil {
			t.Fatal(err)
		}
		if _, err := Open(Config{Driver: "sqlite", Path: path}); err == nil || !strings.Contains(err.Error(), "checksum") {
			t.Fatalf("checksum mismatch open error = %v", err)
		}
	})
}

func TestInspectSQLiteMigrationIsReadOnly(t *testing.T) {
	path := filepath.Join(t.TempDir(), "history.db")
	createLegacySchema(t, path, 10)
	status, err := InspectSQLiteMigration(path)
	if err != nil {
		t.Fatal(err)
	}
	if status.Status != migrationPending || status.CurrentVersion != 10 || status.TargetVersion != currentSchemaVersion {
		t.Fatalf("inspection status = %+v", status)
	}
	db := openMigrationTestDB(t, path)
	version, err := readSchemaVersion(db)
	if err != nil || version != 10 {
		t.Fatalf("inspection changed schema version = %d, err=%v", version, err)
	}
}
