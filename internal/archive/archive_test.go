package archive

import (
	"archive/tar"
	"compress/gzip"
	"context"
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/kevin93203/mango/internal/config"
	"github.com/kevin93203/mango/internal/generation"
	"github.com/kevin93203/mango/internal/history"
	"github.com/kevin93203/mango/internal/instance"
	"github.com/kevin93203/mango/internal/observability"
	"github.com/kevin93203/mango/internal/paths"
	"github.com/kevin93203/mango/internal/registry"
	"github.com/kevin93203/mango/internal/scheduler"
)

func TestExportImportRoundTrip(t *testing.T) {
	source := testLayout(t)
	configPath := writeProjectConfig(t, source, "demo")
	writeStateFiles(t, source, []string{"demo/api"}, []string{"demo/nightly"})
	writeGeneration(t, source, "demo", configPath, 3)
	writeFile(t, filepath.Join(source.Logs, "demo", "api", "stdout.log"), "hello\n")
	writeFile(t, filepath.Join(source.Logs, "demo", "api", "stdout.log.1"), "old\n")

	repository := openArchiveTestHistory(t, source)
	started := time.Date(2026, time.January, 2, 3, 4, 5, 0, time.UTC)
	if err := repository.Record(context.Background(), scheduler.Record{
		RunID: "demo-run", Project: "demo", TargetType: "task", Target: "job", Status: scheduler.StatusSuccess,
		Started: started, Finished: started.Add(time.Second), StdoutPath: filepath.Join(source.Logs, "demo", "api", "stdout.log"),
		Tasks: []scheduler.TaskRecord{{RunID: "demo-task", Task: "job", Status: scheduler.StatusSuccess, StdoutPath: filepath.Join(source.Logs, "demo", "api", "stdout.log")}},
	}, 0); err != nil {
		t.Fatal(err)
	}
	if _, err := repository.AppendEvent(context.Background(), eventForTest("demo", "demo-run")); err != nil {
		t.Fatal(err)
	}
	if _, _, err := repository.ClaimScheduleOccurrence(context.Background(), scheduler.ScheduleOccurrence{
		ID: scheduler.ScheduleOccurrenceID("demo", "nightly", started), Project: "demo", Schedule: "nightly", ScheduledAt: started,
	}); err != nil {
		t.Fatal(err)
	}
	if _, _, err := repository.ClaimWebhookDelivery(context.Background(), scheduler.WebhookDelivery{
		WebhookKey: "demo/hook", IdempotencyKey: "event-1", BodySHA256: "hash", BodySize: 4, RunID: "demo-run",
	}); err != nil {
		t.Fatal(err)
	}
	if err := repository.ReplaceSecurityMetadata(context.Background(), "demo", nil, nil); err != nil {
		t.Fatal(err)
	}
	if err := repository.Close(); err != nil {
		t.Fatal(err)
	}

	archivePath := filepath.Join(t.TempDir(), "demo.mango.tar.gz")
	if _, err := Export(context.Background(), source, ExportOptions{Output: archivePath}); err != nil {
		t.Fatal(err)
	}
	destination := testLayout(t)
	result, err := Import(context.Background(), destination, ImportOptions{Archive: archivePath, ConfigDir: filepath.Join(destination.Root, "configs")})
	if err != nil {
		t.Fatal(err)
	}
	if len(result.Projects) != 1 || result.Projects[0].Project != "demo" {
		t.Fatalf("import result = %+v", result)
	}

	reg, err := registry.Load(destination.Registry)
	if err != nil {
		t.Fatal(err)
	}
	project, ok := reg.Projects["demo"]
	if !ok || project.Enabled != true || project.ConfigurationGeneration != 3 {
		t.Fatalf("imported registry project = %+v, present=%v", project, ok)
	}
	wantConfig := filepath.Join(destination.Root, "configs", "demo", "mango.yaml")
	if project.ConfigPath != wantConfig {
		t.Fatalf("config path = %q, want %q", project.ConfigPath, wantConfig)
	}
	if got := string(mustReadFile(t, wantConfig)); !strings.Contains(got, "name: demo") {
		t.Fatalf("imported config = %q", got)
	}
	if got := string(mustReadFile(t, filepath.Join(destination.Logs, "demo", "api", "stdout.log.1"))); got != "old\n" {
		t.Fatalf("rotated log = %q", got)
	}
	state, err := readProjectState(destination, "demo")
	if err != nil {
		t.Fatal(err)
	}
	if len(state.ServicesDisabled) != 1 || state.ServicesDisabled[0] != "api" || len(state.SchedulesDisabled) != 1 || state.SchedulesDisabled[0] != "nightly" {
		t.Fatalf("imported state = %+v", state)
	}
	if _, err := os.Stat(generationPath(destination, "demo", 3)); err != nil {
		t.Fatalf("generation missing: %v", err)
	}
	snapshot, err := generation.LoadSnapshot(generationPath(destination, "demo", 3), "demo", 3)
	if err != nil {
		t.Fatal(err)
	}
	if snapshot.Config.Path != "" {
		t.Fatalf("imported generation config path = %q, want portable empty path", snapshot.Config.Path)
	}

	destinationHistory := openArchiveTestHistory(t, destination)
	records, err := destinationHistory.Query(context.Background(), scheduler.HistoryQuery{})
	if err != nil {
		t.Fatal(err)
	}
	if len(records) != 1 || records[0].RunID != "demo-run" {
		t.Fatalf("imported history = %+v", records)
	}
	if records[0].StdoutPath != filepath.Join(destination.Logs, "demo", "api", "stdout.log") || len(records[0].Tasks) != 1 {
		t.Fatalf("imported history paths = %+v", records[0])
	}
}

func TestImportRenameRewritesProjectKeys(t *testing.T) {
	source := testLayout(t)
	configPath := writeProjectConfig(t, source, "demo")
	writeStateFiles(t, source, []string{"demo/api"}, []string{"demo/nightly"})
	writeGeneration(t, source, "demo", configPath, 1)
	repository := openArchiveTestHistory(t, source)
	when := time.Date(2026, time.February, 3, 4, 5, 6, 0, time.UTC)
	occurrenceID := scheduler.ScheduleOccurrenceID("demo", "nightly", when)
	if _, _, err := repository.ClaimScheduleOccurrence(context.Background(), scheduler.ScheduleOccurrence{
		ID: occurrenceID, Project: "demo", Schedule: "nightly", ScheduledAt: when,
	}); err != nil {
		t.Fatal(err)
	}
	if err := repository.Record(context.Background(), scheduler.Record{
		RunID: "scheduled-run", Project: "demo", TargetType: "task", Target: "job", Status: scheduler.StatusSuccess,
		Trigger:        scheduler.TriggerRef{Type: scheduler.TriggerSchedule, Name: "nightly", EventID: occurrenceID},
		IdempotencyKey: scheduler.ScheduleOccurrenceIdempotencyKey(occurrenceID), Started: when, Finished: when.Add(time.Second),
	}, 0); err != nil {
		t.Fatal(err)
	}
	if _, _, err := repository.ClaimWebhookDelivery(context.Background(), scheduler.WebhookDelivery{
		WebhookKey: "demo/hook", IdempotencyKey: "event-1", BodySHA256: "hash", BodySize: 4, RunID: "scheduled-run",
	}); err != nil {
		t.Fatal(err)
	}
	if err := repository.Close(); err != nil {
		t.Fatal(err)
	}

	archivePath := filepath.Join(t.TempDir(), "demo.mango.tar.gz")
	if _, err := Export(context.Background(), source, ExportOptions{Output: archivePath}); err != nil {
		t.Fatal(err)
	}
	destination := testLayout(t)
	if _, err := Import(context.Background(), destination, ImportOptions{
		Archive: archivePath, ConfigDir: filepath.Join(destination.Root, "configs"), Rename: map[string]string{"demo": "copy"},
	}); err != nil {
		t.Fatal(err)
	}
	reg, err := registry.Load(destination.Registry)
	if err != nil {
		t.Fatal(err)
	}
	if _, ok := reg.Projects["demo"]; ok {
		t.Fatal("source project was imported under its old name")
	}
	if _, ok := reg.Projects["copy"]; !ok {
		t.Fatal("renamed project is missing")
	}
	state, err := readProjectState(destination, "copy")
	if err != nil {
		t.Fatal(err)
	}
	if len(state.ServicesDisabled) != 1 || state.ServicesDisabled[0] != "api" {
		t.Fatalf("renamed service state = %+v", state)
	}
	destinationHistory := openArchiveTestHistory(t, destination)
	occurrences, err := destinationHistory.ExportProject(context.Background(), "copy")
	if err != nil {
		t.Fatal(err)
	}
	newOccurrenceID := scheduler.ScheduleOccurrenceID("copy", "nightly", when)
	if len(occurrences.ScheduleOccurrences) != 1 || occurrences.ScheduleOccurrences[0].ID != newOccurrenceID {
		t.Fatalf("renamed occurrences = %+v", occurrences.ScheduleOccurrences)
	}
	records, err := destinationHistory.Query(context.Background(), scheduler.HistoryQuery{})
	if err != nil {
		t.Fatal(err)
	}
	if len(records) != 1 || records[0].Project != "copy" || records[0].Trigger.EventID != newOccurrenceID || records[0].IdempotencyKey != scheduler.ScheduleOccurrenceIdempotencyKey(newOccurrenceID) {
		t.Fatalf("renamed history = %+v", records)
	}
}

func TestImportRejectsConflictAndRequiresConfirmation(t *testing.T) {
	source := testLayout(t)
	configPath := writeProjectConfig(t, source, "demo")
	writeGeneration(t, source, "demo", configPath, 1)
	archivePath := filepath.Join(t.TempDir(), "demo.mango.tar.gz")
	if _, err := Export(context.Background(), source, ExportOptions{Output: archivePath}); err != nil {
		t.Fatal(err)
	}
	destination := testLayout(t)
	configDir := filepath.Join(destination.Root, "configs")
	if _, err := Import(context.Background(), destination, ImportOptions{Archive: archivePath, ConfigDir: configDir}); err != nil {
		t.Fatal(err)
	}
	if _, err := Import(context.Background(), destination, ImportOptions{Archive: archivePath, ConfigDir: configDir}); err == nil || !strings.Contains(err.Error(), "already") {
		t.Fatalf("second import error = %v", err)
	}
	if _, err := Import(context.Background(), destination, ImportOptions{Archive: archivePath, ConfigDir: configDir, Replace: true}); err == nil || !strings.Contains(err.Error(), "requires --yes") {
		t.Fatalf("unconfirmed replace error = %v", err)
	}
}

func TestImportRollsBackOnHistoryConflict(t *testing.T) {
	source := testLayout(t)
	configPath := writeProjectConfig(t, source, "demo")
	writeGeneration(t, source, "demo", configPath, 1)
	sourceHistory, err := history.Open(history.Config{Driver: "sqlite", Path: filepath.Join(source.State, "history.db")})
	if err != nil {
		t.Fatal(err)
	}
	if err := sourceHistory.Record(context.Background(), scheduler.Record{RunID: "shared-run", Project: "demo", Status: scheduler.StatusSuccess}, 0); err != nil {
		t.Fatal(err)
	}
	if err := sourceHistory.Close(); err != nil {
		t.Fatal(err)
	}
	archivePath := filepath.Join(t.TempDir(), "demo.mango.tar.gz")
	if _, err := Export(context.Background(), source, ExportOptions{Output: archivePath}); err != nil {
		t.Fatal(err)
	}

	destination := testLayout(t)
	destinationHistory, err := history.Open(history.Config{Driver: "sqlite", Path: filepath.Join(destination.State, "history.db")})
	if err != nil {
		t.Fatal(err)
	}
	if err := destinationHistory.Record(context.Background(), scheduler.Record{RunID: "shared-run", Project: "other", Status: scheduler.StatusSuccess}, 0); err != nil {
		t.Fatal(err)
	}
	if err := destinationHistory.Close(); err != nil {
		t.Fatal(err)
	}
	configDir := filepath.Join(destination.Root, "configs")
	if _, err := Import(context.Background(), destination, ImportOptions{Archive: archivePath, ConfigDir: configDir}); err == nil || !strings.Contains(err.Error(), "already exists") {
		t.Fatalf("history conflict error = %v", err)
	}
	if _, err := os.Stat(filepath.Join(configDir, "demo", "mango.yaml")); !os.IsNotExist(err) {
		t.Fatalf("rolled-back config stat = %v", err)
	}
	if _, err := os.Stat(importJournalPath(destination)); !os.IsNotExist(err) {
		t.Fatalf("rolled-back journal stat = %v", err)
	}
	checkHistory, err := history.Open(history.Config{Driver: "sqlite", Path: filepath.Join(destination.State, "history.db")})
	if err != nil {
		t.Fatal(err)
	}
	defer checkHistory.Close()
	demoHistory, err := checkHistory.ExportProject(context.Background(), "demo")
	if err != nil {
		t.Fatal(err)
	}
	if !historyEmpty(demoHistory) {
		t.Fatalf("rolled-back demo history = %+v", demoHistory)
	}
	otherHistory, err := checkHistory.ExportProject(context.Background(), "other")
	if err != nil {
		t.Fatal(err)
	}
	if len(otherHistory.Runs) != 1 || otherHistory.Runs[0].RunID != "shared-run" {
		t.Fatalf("unrelated history = %+v", otherHistory)
	}
}

func TestRecoverPendingImportRestoresFiles(t *testing.T) {
	layout := testLayout(t)
	configPath := filepath.Join(layout.Root, "configs", "demo", "mango.yaml")
	writeFile(t, configPath, "before\n")
	projects := []importProject{{Destination: "demo", ConfigPath: configPath, LogRoot: filepath.Join(layout.Logs, "demo"), GenerationRoot: filepath.Join(layout.Generations, "demo")}}
	backup, err := newBackup(layout, projects, map[string]history.ProjectHistory{
		"demo": {Version: history.ProjectHistoryVersion, Project: "demo"},
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := backup.WriteJournal(layout, importPhaseFiles); err != nil {
		backup.Cleanup()
		t.Fatal(err)
	}
	writeFile(t, configPath, "after\n")
	if err := RecoverPending(context.Background(), layout); err != nil {
		t.Fatal(err)
	}
	if got := string(mustReadFile(t, configPath)); got != "before\n" {
		t.Fatalf("recovered config = %q", got)
	}
	if _, err := os.Stat(importJournalPath(layout)); !os.IsNotExist(err) {
		t.Fatalf("recovered journal stat = %v", err)
	}
	if _, err := os.Stat(backup.Root); !os.IsNotExist(err) {
		t.Fatalf("recovered backup stat = %v", err)
	}
}

func TestImportDryRunDoesNotInstall(t *testing.T) {
	source := testLayout(t)
	configPath := writeProjectConfig(t, source, "demo")
	writeGeneration(t, source, "demo", configPath, 1)
	archivePath := filepath.Join(t.TempDir(), "demo.mango.tar.gz")
	if _, err := Export(context.Background(), source, ExportOptions{Output: archivePath}); err != nil {
		t.Fatal(err)
	}
	destination := testLayout(t)
	configDir := filepath.Join(destination.Root, "not-created")
	result, err := Import(context.Background(), destination, ImportOptions{Archive: archivePath, ConfigDir: configDir, DryRun: true})
	if err != nil {
		t.Fatal(err)
	}
	if !result.DryRun {
		t.Fatal("dry run result is not marked dry_run")
	}
	if _, err := os.Stat(configDir); !os.IsNotExist(err) {
		t.Fatalf("dry run config directory stat = %v, want not exist", err)
	}
	reg, err := registry.Load(destination.Registry)
	if err != nil {
		t.Fatal(err)
	}
	if len(reg.Projects) != 0 {
		t.Fatalf("dry run registry = %+v, want empty", reg.Projects)
	}
}

func TestExportForceReplacesExistingArchive(t *testing.T) {
	source := testLayout(t)
	configPath := writeProjectConfig(t, source, "demo")
	writeGeneration(t, source, "demo", configPath, 1)
	archivePath := filepath.Join(t.TempDir(), "demo.mango.tar.gz")
	if _, err := Export(context.Background(), source, ExportOptions{Output: archivePath}); err != nil {
		t.Fatal(err)
	}
	if _, err := Export(context.Background(), source, ExportOptions{Output: archivePath}); err == nil || !strings.Contains(err.Error(), "--force") {
		t.Fatalf("overwrite without force error = %v", err)
	}
	if _, err := Export(context.Background(), source, ExportOptions{Output: archivePath, Force: true}); err != nil {
		t.Fatal(err)
	}
}

func TestImportRejectsUnsafeArchiveEntry(t *testing.T) {
	archivePath := filepath.Join(t.TempDir(), "unsafe.tar.gz")
	writeTestTar(t, archivePath, Manifest{
		FormatVersion: FormatVersion, Projects: []string{"demo"},
		Files: []ManifestFile{{Path: "../escape", Size: 1, SHA256: strings.Repeat("0", 64)}},
	}, map[string][]byte{"../escape": []byte("x")})
	if _, err := readArchive(archivePath); err == nil || !strings.Contains(err.Error(), "invalid file") {
		t.Fatalf("unsafe archive error = %v", err)
	}
}

func TestExportAndImportRespectDaemonLock(t *testing.T) {
	source := testLayout(t)
	configPath := writeProjectConfig(t, source, "demo")
	writeGeneration(t, source, "demo", configPath, 1)
	lock, err := instance.Acquire(source.LockPath)
	if err != nil {
		t.Fatal(err)
	}
	defer lock.Close()
	if _, err := Export(context.Background(), source, ExportOptions{Output: filepath.Join(t.TempDir(), "blocked.tar.gz")}); err == nil || !strings.Contains(err.Error(), "daemon must be stopped") {
		t.Fatalf("locked export error = %v", err)
	}
}

func testLayout(t *testing.T) paths.Layout {
	root := t.TempDir()
	layout := paths.Layout{
		Root: root, Runtime: filepath.Join(root, "runtime"), Logs: filepath.Join(root, "logs"), State: filepath.Join(root, "state"),
		Generations: filepath.Join(root, "state", "generations"), ScheduleState: filepath.Join(root, "state", "schedules.json"),
		ServiceState: filepath.Join(root, "state", "services.json"), Registry: filepath.Join(root, "projects.json"),
		DaemonConfig: filepath.Join(root, "daemon.yaml"), LockPath: filepath.Join(root, "runtime", "daemon.lock"),
	}
	if err := paths.Ensure(layout); err != nil {
		t.Fatal(err)
	}
	return layout
}

func writeProjectConfig(t *testing.T, layout paths.Layout, project string) string {
	directory := filepath.Join(layout.Root, "source", project)
	path := filepath.Join(directory, "mango.yaml")
	writeFile(t, path, "version: 4\nname: "+project+"\ntasks:\n  job:\n    command: echo\n")
	projectRecord := registry.Project{Name: project, ConfigPath: path, Enabled: true, ConfigVersion: config.CurrentVersion, ConfigurationGeneration: 1}
	file, err := registry.Load(layout.Registry)
	if err != nil {
		t.Fatal(err)
	}
	file.Projects[project] = projectRecord
	if err := registry.Save(layout.Registry, file); err != nil {
		t.Fatal(err)
	}
	return path
}

func writeGeneration(t *testing.T, layout paths.Layout, project, configPath string, value uint64) {
	if _, err := generation.SaveSnapshot(layout.State, generation.Snapshot{
		Version: generation.SnapshotVersion, Project: project, Generation: value, Status: generation.SnapshotCommit,
		AppliedAt: time.Date(2026, time.January, 2, 0, 0, 0, 0, time.UTC), Config: config.File{Version: config.CurrentVersion, Name: project, Path: configPath},
	}); err != nil {
		t.Fatal(err)
	}
	file, err := registry.Load(layout.Registry)
	if err != nil {
		t.Fatal(err)
	}
	record := file.Projects[project]
	record.ConfigurationGeneration = value
	record.DesiredStatePath = generationPath(layout, project, value)
	file.Projects[project] = record
	if err := registry.Save(layout.Registry, file); err != nil {
		t.Fatal(err)
	}
}

func writeStateFiles(t *testing.T, layout paths.Layout, services, schedules []string) {
	writeFile(t, layout.ServiceState, `{"version":1,"disabled":["`+strings.Join(services, `","`)+`"]}`+"\n")
	writeFile(t, layout.ScheduleState, `{"version":1,"disabled":["`+strings.Join(schedules, `","`)+`"]}`+"\n")
}

func openArchiveTestHistory(t *testing.T, layout paths.Layout) *history.Repository {
	repository, err := history.Open(history.Config{Driver: "sqlite", Path: filepath.Join(layout.State, "history.db")})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = repository.Close() })
	return repository
}

func eventForTest(project, runID string) observability.Event {
	return observability.Event{Project: project, RunID: runID, Type: "test", Timestamp: time.Now().UTC()}
}

func writeTestTar(t *testing.T, archivePath string, manifest Manifest, files map[string][]byte) {
	t.Helper()
	manifestBytes, err := json.Marshal(manifest)
	if err != nil {
		t.Fatal(err)
	}
	output, err := os.Create(archivePath)
	if err != nil {
		t.Fatal(err)
	}
	gzipWriter := gzip.NewWriter(output)
	tarWriter := tar.NewWriter(gzipWriter)
	if err := tarWriter.WriteHeader(&tar.Header{Name: "manifest.json", Mode: 0o600, Size: int64(len(manifestBytes)), Typeflag: tar.TypeReg}); err != nil {
		t.Fatal(err)
	}
	if _, err := tarWriter.Write(manifestBytes); err != nil {
		t.Fatal(err)
	}
	for name, data := range files {
		if err := tarWriter.WriteHeader(&tar.Header{Name: name, Mode: 0o600, Size: int64(len(data)), Typeflag: tar.TypeReg}); err != nil {
			t.Fatal(err)
		}
		if _, err := tarWriter.Write(data); err != nil {
			t.Fatal(err)
		}
	}
	if err := tarWriter.Close(); err != nil {
		t.Fatal(err)
	}
	if err := gzipWriter.Close(); err != nil {
		t.Fatal(err)
	}
	if err := output.Close(); err != nil {
		t.Fatal(err)
	}
}

func writeFile(t *testing.T, path, contents string) {
	t.Helper()
	if err := os.MkdirAll(filepath.Dir(path), 0o700); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(path, []byte(contents), 0o600); err != nil {
		t.Fatal(err)
	}
}

func mustReadFile(t *testing.T, path string) []byte {
	t.Helper()
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	return data
}
