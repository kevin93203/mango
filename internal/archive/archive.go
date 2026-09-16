// Package archive implements the driver-independent project export format.
package archive

import (
	"archive/tar"
	"bytes"
	"compress/gzip"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"path"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/kevin93203/mango/internal/config"
	"github.com/kevin93203/mango/internal/generation"
	"github.com/kevin93203/mango/internal/history"
	"github.com/kevin93203/mango/internal/instance"
	"github.com/kevin93203/mango/internal/paths"
	"github.com/kevin93203/mango/internal/reconcile"
	"github.com/kevin93203/mango/internal/registry"
	"github.com/kevin93203/mango/internal/scheduler"
)

const (
	FormatVersion        = 1
	projectDataVersion   = 1
	stateDataVersion     = 1
	importJournalVersion = 1
	maxManifestBytes     = 16 << 20
	archiveFileMode      = 0o600
	archiveDirectoryMode = 0o700
	importJournalName    = "import-journal.json"
	importPhasePrepared  = "prepared"
	importPhaseFiles     = "files_installed"
	importPhaseHistory   = "history_importing"
	importPhaseCommitted = "committed"
)

type ExportOptions struct {
	Projects []string
	Output   string
	Force    bool
}

type ImportOptions struct {
	Archive   string
	Projects  []string
	ConfigDir string
	Rename    map[string]string
	Replace   bool
	Yes       bool
	DryRun    bool
}

type Result struct {
	Operation string          `json:"operation"`
	Archive   string          `json:"archive"`
	Projects  []ProjectResult `json:"projects"`
	Bytes     int64           `json:"bytes,omitempty"`
	DryRun    bool            `json:"dry_run,omitempty"`
}

type ProjectResult struct {
	Source             string `json:"source"`
	Project            string `json:"project"`
	Generations        int    `json:"generations"`
	LogFiles           int    `json:"log_files"`
	HistoryRuns        int    `json:"history_runs"`
	HistoryEvents      int    `json:"history_events"`
	HistoryOccurrences int    `json:"history_occurrences"`
}

type Manifest struct {
	FormatVersion int            `json:"format_version"`
	CreatedAt     time.Time      `json:"created_at"`
	Projects      []string       `json:"projects"`
	Files         []ManifestFile `json:"files"`
}

type ManifestFile struct {
	Path   string `json:"path"`
	Size   int64  `json:"size"`
	SHA256 string `json:"sha256"`
}

type projectData struct {
	Version  int              `json:"version"`
	Registry registry.Project `json:"registry"`
}

type stateData struct {
	Version           int      `json:"version"`
	ServicesDisabled  []string `json:"services_disabled,omitempty"`
	SchedulesDisabled []string `json:"schedules_disabled,omitempty"`
}

type archiveFile struct {
	Name   string
	Data   []byte
	Source string
	Size   int64
	SHA256 string
}

type extractedArchive struct {
	Root     string
	Manifest Manifest
	Files    map[string]string
}

type importProject struct {
	Source         string
	Destination    string
	Config         []byte
	Registry       registry.Project
	State          stateData
	Generations    map[uint64][]byte
	History        history.ProjectHistory
	ConfigPath     string
	LogRoot        string
	GenerationRoot string
	LogFiles       map[string]string
}

// Export creates a portable project archive. It never starts the daemon and
// refuses to run while another process owns the daemon lock.
func Export(ctx context.Context, layout paths.Layout, options ExportOptions) (Result, error) {
	if strings.TrimSpace(options.Output) == "" {
		return Result{}, errors.New("export output path is required")
	}
	lock, err := acquireLock(layout)
	if err != nil {
		return Result{}, err
	}
	defer lock.Close()
	if err := recoverPendingLocked(ctx, layout); err != nil {
		return Result{}, err
	}

	registryFile, err := registry.Load(layout.Registry)
	if err != nil {
		return Result{}, err
	}
	names, err := selectRegisteredProjects(registryFile, options.Projects)
	if err != nil {
		return Result{}, err
	}
	if len(names) == 0 {
		return Result{}, errors.New("no projects to export")
	}

	historyRepository, err := openHistoryReadOnly(layout)
	if err != nil {
		return Result{}, err
	}
	if historyRepository != nil {
		defer historyRepository.Close()
	}

	files := make([]archiveFile, 0)
	results := make([]ProjectResult, 0, len(names))
	for _, name := range names {
		if err := ctx.Err(); err != nil {
			return Result{}, err
		}
		project := registryFile.Projects[name]
		project.Name = name
		configBytes, err := readProjectConfig(project.ConfigPath)
		if err != nil {
			return Result{}, fmt.Errorf("export project %q config: %w", name, err)
		}
		if _, err := config.Load(project.ConfigPath); err != nil {
			return Result{}, fmt.Errorf("export project %q config: %w", name, err)
		}

		project.ConfigPath = ""
		project.DesiredStatePath = ""
		project.ProcessIDs = nil
		project.ShimInstances = nil
		projectJSON, err := marshalJSON(projectData{Version: projectDataVersion, Registry: project})
		if err != nil {
			return Result{}, fmt.Errorf("encode project %q metadata: %w", name, err)
		}
		state, err := readProjectState(layout, name)
		if err != nil {
			return Result{}, fmt.Errorf("export project %q state: %w", name, err)
		}
		stateJSON, err := marshalJSON(state)
		if err != nil {
			return Result{}, fmt.Errorf("encode project %q state: %w", name, err)
		}

		historyData := history.ProjectHistory{Version: history.ProjectHistoryVersion, Project: name}
		if historyRepository != nil {
			historyData, err = historyRepository.ExportProject(ctx, name)
			if err != nil {
				return Result{}, fmt.Errorf("export project %q history: %w", name, err)
			}
		}
		if err := logicalizeHistoryPaths(&historyData, layout.Logs); err != nil {
			return Result{}, fmt.Errorf("export project %q history paths: %w", name, err)
		}
		historyJSON, err := marshalJSON(historyData)
		if err != nil {
			return Result{}, fmt.Errorf("encode project %q history: %w", name, err)
		}

		files = append(files,
			dataFile(projectArchivePath(name, "config/mango.yaml"), configBytes),
			dataFile(projectArchivePath(name, "project.json"), projectJSON),
			dataFile(projectArchivePath(name, "state.json"), stateJSON),
			dataFile(projectArchivePath(name, "history.json"), historyJSON),
		)

		generations, err := projectGenerations(layout, name)
		if err != nil {
			return Result{}, fmt.Errorf("export project %q generations: %w", name, err)
		}
		if project.ConfigurationGeneration > 0 {
			if !containsGeneration(generations, project.ConfigurationGeneration) {
				return Result{}, fmt.Errorf("project %q points to missing generation %d", name, project.ConfigurationGeneration)
			}
			accepted, err := generation.LoadSnapshot(generationPath(layout, name, project.ConfigurationGeneration), name, project.ConfigurationGeneration)
			if err != nil {
				return Result{}, fmt.Errorf("project %q accepted generation %d: %w", name, project.ConfigurationGeneration, err)
			}
			if accepted.Status != generation.SnapshotCommit {
				return Result{}, fmt.Errorf("project %q accepted generation %d has status %q", name, project.ConfigurationGeneration, accepted.Status)
			}
		}
		for _, value := range generations {
			generationFile := generationPath(layout, name, value)
			generationBytes, err := readRegularFile(generationFile)
			if err != nil {
				return Result{}, fmt.Errorf("read project %q generation %d: %w", name, value, err)
			}
			if _, err := generation.LoadSnapshot(generationFile, name, value); err != nil {
				return Result{}, fmt.Errorf("validate project %q generation %d: %w", name, value, err)
			}
			files = append(files, dataFile(projectArchivePath(name, fmt.Sprintf("generations/%d.json", value)), generationBytes))
		}

		logRoot := projectPath(layout.Logs, name)
		logFiles, err := collectRegularFiles(logRoot)
		if err != nil {
			return Result{}, fmt.Errorf("export project %q logs: %w", name, err)
		}
		for _, logFile := range logFiles {
			relative, err := filepath.Rel(logRoot, logFile)
			if err != nil {
				return Result{}, err
			}
			files = append(files, sourceFile(projectArchivePath(name, "logs/"+filepath.ToSlash(relative)), logFile))
		}
		results = append(results, ProjectResult{
			Source: name, Project: name, Generations: len(generations), LogFiles: len(logFiles),
			HistoryRuns: len(historyData.Runs), HistoryEvents: len(historyData.Events), HistoryOccurrences: len(historyData.ScheduleOccurrences),
		})
	}

	output, err := filepath.Abs(options.Output)
	if err != nil {
		return Result{}, fmt.Errorf("resolve export output: %w", err)
	}
	bytesWritten, err := writeArchive(output, names, files, options.Force)
	if err != nil {
		return Result{}, err
	}
	return Result{Operation: "export", Archive: output, Projects: results, Bytes: bytesWritten}, nil
}

// Import restores one or more projects from a portable archive. All archive
// validation and conflict checks happen before the first destination write.
func Import(ctx context.Context, layout paths.Layout, options ImportOptions) (result Result, err error) {
	if strings.TrimSpace(options.Archive) == "" {
		return Result{}, errors.New("import archive path is required")
	}
	if options.Replace && !options.Yes {
		return Result{}, errors.New("import --replace requires --yes")
	}
	if options.Yes && !options.Replace {
		return Result{}, errors.New("import --yes requires --replace")
	}
	lock, err := acquireLock(layout)
	if err != nil {
		return Result{}, err
	}
	defer lock.Close()
	if err := recoverPendingLocked(ctx, layout); err != nil {
		return Result{}, err
	}

	extracted, err := readArchive(options.Archive)
	if err != nil {
		return Result{}, err
	}
	defer os.RemoveAll(extracted.Root)

	names, err := selectArchiveProjects(extracted.Manifest.Projects, options.Projects)
	if err != nil {
		return Result{}, err
	}
	mappings, err := normalizeMappings(names, options.Rename)
	if err != nil {
		return Result{}, err
	}
	configDir := options.ConfigDir
	if configDir == "" {
		configDir = "."
	}
	configDir, err = filepath.Abs(configDir)
	if err != nil {
		return Result{}, fmt.Errorf("resolve import config directory: %w", err)
	}
	destinationRegistry, err := registry.Load(layout.Registry)
	if err != nil {
		return Result{}, err
	}
	existingHistory, err := openHistoryReadOnly(layout)
	if err != nil {
		return Result{}, err
	}
	historyDatabaseAvailable := existingHistory != nil
	if existingHistory != nil {
		defer func() {
			if existingHistory != nil {
				_ = existingHistory.Close()
			}
		}()
	}
	existingServiceState, err := readDisabledState(layout.ServiceState, "service")
	if err != nil {
		return Result{}, err
	}
	existingScheduleState, err := readDisabledState(layout.ScheduleState, "schedule")
	if err != nil {
		return Result{}, err
	}

	projects := make([]importProject, 0, len(names))
	results := make([]ProjectResult, 0, len(names))
	existingHistories := make(map[string]history.ProjectHistory, len(names))
	seenDestinations := map[string]bool{}
	for _, source := range names {
		destination := mappings[source]
		if seenDestinations[destination] {
			return Result{}, fmt.Errorf("multiple archive projects map to %q", destination)
		}
		seenDestinations[destination] = true
		project, err := loadImportProject(extracted, source, destination, configDir, layout)
		if err != nil {
			return Result{}, err
		}
		if existing, err := projectHistoryFor(existingHistory, ctx, destination); err != nil {
			return Result{}, err
		} else if !options.Replace && !historyEmpty(existing) {
			return Result{}, fmt.Errorf("destination project %q already has history", destination)
		} else {
			existingHistories[destination] = existing
		}
		if conflict := destinationConflict(layout, destinationRegistry, destination, project.ConfigPath, existingServiceState, existingScheduleState, options.Replace); conflict != nil {
			return Result{}, conflict
		}
		if err := rewriteProject(project, layout); err != nil {
			return Result{}, fmt.Errorf("prepare project %q import: %w", source, err)
		}
		projects = append(projects, *project)
		results = append(results, ProjectResult{
			Source: source, Project: destination, Generations: len(project.Generations), LogFiles: len(project.LogFiles),
			HistoryRuns: len(project.History.Runs), HistoryEvents: len(project.History.Events), HistoryOccurrences: len(project.History.ScheduleOccurrences),
		})
	}

	if options.DryRun {
		return Result{Operation: "import", Archive: options.Archive, Projects: results, DryRun: true}, nil
	}
	if existingHistory != nil {
		if err := existingHistory.Close(); err != nil {
			return Result{}, fmt.Errorf("close history database: %w", err)
		}
		existingHistory = nil
	}

	backup, err := newBackup(layout, projects, existingHistories)
	if err != nil {
		return Result{}, err
	}
	if err := backup.WriteJournal(layout, importPhasePrepared); err != nil {
		_ = backup.Cleanup()
		return Result{}, err
	}
	phase := importPhasePrepared
	committed := false
	defer func() {
		if committed {
			if removeErr := backup.RemoveJournal(); removeErr != nil {
				if err == nil {
					err = fmt.Errorf("remove import journal: %w", removeErr)
				}
				return
			}
			if cleanupErr := backup.Cleanup(); err == nil && cleanupErr != nil {
				err = fmt.Errorf("cleanup import backup: %w", cleanupErr)
			}
			return
		}
		if rollbackErr := backup.RestoreAll(ctx, layout, phase == importPhaseHistory); rollbackErr != nil {
			if err == nil {
				err = fmt.Errorf("import rollback failed: %w", rollbackErr)
			} else {
				err = fmt.Errorf("%w; import rollback failed: %v", err, rollbackErr)
			}
			return
		}
		if removeErr := backup.RemoveJournal(); removeErr != nil {
			if err == nil {
				err = fmt.Errorf("remove import journal after rollback: %w", removeErr)
			} else {
				err = fmt.Errorf("%w; remove import journal after rollback: %v", err, removeErr)
			}
			return
		}
		if cleanupErr := backup.Cleanup(); err == nil && cleanupErr != nil {
			err = fmt.Errorf("cleanup import backup: %w", cleanupErr)
		}
	}()

	if err := installProjects(layout, projects, destinationRegistry, existingServiceState, existingScheduleState, options.Replace); err != nil {
		return Result{}, err
	}
	if err := backup.WriteJournal(layout, importPhaseFiles); err != nil {
		return Result{}, err
	}
	phase = importPhaseFiles
	if err := backup.WriteJournal(layout, importPhaseHistory); err != nil {
		return Result{}, err
	}
	phase = importPhaseHistory
	if err := importHistory(ctx, layout, projects, historyDatabaseAvailable, options.Replace); err != nil {
		return Result{}, err
	}
	if err := backup.WriteJournal(layout, importPhaseCommitted); err != nil {
		return Result{}, err
	}
	phase = importPhaseCommitted
	committed = true
	return Result{Operation: "import", Archive: options.Archive, Projects: results}, nil
}

// RecoverPending rolls back an interrupted import left by a process crash.
// It is safe to call during daemon startup and from export/import operations.
func RecoverPending(ctx context.Context, layout paths.Layout) error {
	lock, err := acquireLock(layout)
	if err != nil {
		return err
	}
	defer lock.Close()
	return recoverPendingLocked(ctx, layout)
}

func recoverPendingLocked(ctx context.Context, layout paths.Layout) error {
	journal, found, err := readImportJournal(layout)
	if err != nil || !found {
		return err
	}
	backup, err := backupFromJournal(layout, journal)
	if err != nil {
		return err
	}
	if journal.Phase == importPhaseCommitted {
		if err := backup.RemoveJournal(); err != nil {
			return fmt.Errorf("remove committed import journal: %w", err)
		}
		return backup.Cleanup()
	}
	if err := backup.RestoreAll(ctx, layout, journal.Phase == importPhaseHistory); err != nil {
		return fmt.Errorf("recover interrupted import: %w", err)
	}
	if err := backup.RemoveJournal(); err != nil {
		return fmt.Errorf("remove recovered import journal: %w", err)
	}
	return backup.Cleanup()
}

func acquireLock(layout paths.Layout) (*instance.Lock, error) {
	if layout.LockPath == "" {
		return nil, errors.New("daemon lock path is not configured")
	}
	if err := os.MkdirAll(filepath.Dir(layout.LockPath), archiveDirectoryMode); err != nil {
		return nil, fmt.Errorf("create daemon runtime directory: %w", err)
	}
	lock, err := instance.Acquire(layout.LockPath)
	if errors.Is(err, instance.ErrAlreadyRunning) {
		return nil, errors.New("daemon must be stopped before export/import")
	}
	if err != nil {
		return nil, fmt.Errorf("acquire daemon lock: %w", err)
	}
	return lock, nil
}

func selectRegisteredProjects(file registry.File, requested []string) ([]string, error) {
	if len(requested) == 0 {
		result := make([]string, 0, len(file.Projects))
		for name := range file.Projects {
			result = append(result, name)
		}
		sort.Strings(result)
		return result, nil
	}
	return selectNames(requested, func(name string) bool {
		_, ok := file.Projects[name]
		return ok
	}, "registered project")
}

func selectArchiveProjects(available, requested []string) ([]string, error) {
	set := make(map[string]bool, len(available))
	for _, name := range available {
		set[name] = true
	}
	if len(requested) == 0 {
		result := append([]string(nil), available...)
		sort.Strings(result)
		return result, nil
	}
	return selectNames(requested, func(name string) bool { return set[name] }, "archive project")
}

func selectNames(requested []string, exists func(string) bool, label string) ([]string, error) {
	result := make([]string, 0, len(requested))
	seen := map[string]bool{}
	for _, name := range requested {
		if err := config.ValidateProjectName(name); err != nil {
			return nil, err
		}
		if seen[name] {
			return nil, fmt.Errorf("duplicate %s %q", label, name)
		}
		if !exists(name) {
			return nil, fmt.Errorf("%s %q was not found", label, name)
		}
		seen[name] = true
		result = append(result, name)
	}
	sort.Strings(result)
	return result, nil
}

func normalizeMappings(selected []string, requested map[string]string) (map[string]string, error) {
	set := make(map[string]bool, len(selected))
	for _, name := range selected {
		set[name] = true
	}
	result := make(map[string]string, len(selected))
	for _, name := range selected {
		result[name] = name
	}
	for source, destination := range requested {
		if !set[source] {
			return nil, fmt.Errorf("--rename source %q is not selected", source)
		}
		if err := config.ValidateProjectName(destination); err != nil {
			return nil, fmt.Errorf("--rename %s=%s: %w", source, destination, err)
		}
		result[source] = destination
	}
	seen := map[string]string{}
	for _, source := range selected {
		destination := result[source]
		if previous, ok := seen[destination]; ok {
			return nil, fmt.Errorf("projects %q and %q both map to %q", previous, source, destination)
		}
		seen[destination] = source
	}
	return result, nil
}

func readProjectConfig(path string) ([]byte, error) {
	if strings.TrimSpace(path) == "" {
		return nil, errors.New("project config path is empty")
	}
	return readRegularFile(path)
}

func readProjectState(layout paths.Layout, project string) (stateData, error) {
	services, err := readDisabledState(layout.ServiceState, "service")
	if err != nil {
		return stateData{}, err
	}
	schedules, err := readDisabledState(layout.ScheduleState, "schedule")
	if err != nil {
		return stateData{}, err
	}
	return stateData{
		Version:           stateDataVersion,
		ServicesDisabled:  localDisabledKeys(services, project),
		SchedulesDisabled: localDisabledKeys(schedules, project),
	}, nil
}

func readDisabledState(filePath, label string) (map[string]bool, error) {
	result := map[string]bool{}
	if strings.TrimSpace(filePath) == "" {
		return result, nil
	}
	data, err := os.ReadFile(filePath)
	if errors.Is(err, os.ErrNotExist) {
		return result, nil
	}
	if err != nil {
		return nil, fmt.Errorf("read %s state: %w", label, err)
	}
	var state struct {
		Version  int      `json:"version"`
		Disabled []string `json:"disabled"`
	}
	if err := json.Unmarshal(data, &state); err != nil {
		return nil, fmt.Errorf("decode %s state: %w", label, err)
	}
	if state.Version != stateDataVersion {
		return nil, fmt.Errorf("unsupported %s state version %d", label, state.Version)
	}
	for _, key := range state.Disabled {
		if key == "" {
			return nil, fmt.Errorf("%s state contains an empty key", label)
		}
		result[key] = true
	}
	return result, nil
}

func localDisabledKeys(state map[string]bool, project string) []string {
	prefix := project + "/"
	result := make([]string, 0)
	for key, disabled := range state {
		if disabled && strings.HasPrefix(key, prefix) {
			result = append(result, strings.TrimPrefix(key, prefix))
		}
	}
	sort.Strings(result)
	return result
}

func validateLocalStateNames(values []string, label, project string) error {
	seen := make(map[string]bool, len(values))
	for _, value := range values {
		if value == "" || value == "." || value == ".." || strings.ContainsAny(value, "/\\") || seen[value] {
			return fmt.Errorf("archive project %q has invalid %s state key %q", project, label, value)
		}
		seen[value] = true
	}
	return nil
}

func writeDisabledState(path string, label string, state map[string]bool) error {
	keys := make([]string, 0, len(state))
	for key, disabled := range state {
		if disabled {
			keys = append(keys, key)
		}
	}
	sort.Strings(keys)
	data, err := marshalJSON(struct {
		Version  int      `json:"version"`
		Disabled []string `json:"disabled,omitempty"`
	}{Version: stateDataVersion, Disabled: keys})
	if err != nil {
		return fmt.Errorf("encode %s state: %w", label, err)
	}
	return writeBytesAtomic(path, data)
}

func projectGenerations(layout paths.Layout, project string) ([]uint64, error) {
	root := generationRoot(layout)
	if root == "" {
		return nil, nil
	}
	projectRoot := projectPath(root, project)
	info, err := os.Lstat(projectRoot)
	if errors.Is(err, os.ErrNotExist) {
		return []uint64{}, nil
	}
	if err != nil {
		return nil, err
	}
	if info.Mode()&os.ModeSymlink != 0 || !info.IsDir() {
		return nil, fmt.Errorf("%s is not a directory", projectRoot)
	}
	entries, err := os.ReadDir(projectRoot)
	if errors.Is(err, os.ErrNotExist) {
		return []uint64{}, nil
	}
	if err != nil {
		return nil, err
	}
	values := make([]uint64, 0, len(entries))
	for _, entry := range entries {
		if entry.IsDir() || filepath.Ext(entry.Name()) != ".json" {
			continue
		}
		value, err := strconv.ParseUint(strings.TrimSuffix(entry.Name(), ".json"), 10, 64)
		if err != nil || value == 0 {
			continue
		}
		values = append(values, value)
	}
	sort.Slice(values, func(i, j int) bool { return values[i] < values[j] })
	return values, nil
}

func generationRoot(layout paths.Layout) string {
	if layout.Generations != "" {
		return layout.Generations
	}
	if layout.State != "" {
		return filepath.Join(layout.State, "generations")
	}
	return ""
}

func generationPath(layout paths.Layout, project string, value uint64) string {
	root := projectPath(generationRoot(layout), project)
	if root == "" {
		return ""
	}
	return filepath.Join(root, fmt.Sprintf("%d.json", value))
}

func projectPath(root, project string) string {
	if strings.TrimSpace(root) == "" {
		return ""
	}
	return filepath.Join(root, project)
}

func containsGeneration(values []uint64, target uint64) bool {
	for _, value := range values {
		if value == target {
			return true
		}
	}
	return false
}

func collectRegularFiles(root string) ([]string, error) {
	if root == "" {
		return nil, nil
	}
	info, err := os.Lstat(root)
	if errors.Is(err, os.ErrNotExist) {
		return []string{}, nil
	}
	if err != nil {
		return nil, err
	}
	if !info.IsDir() {
		return nil, fmt.Errorf("%s is not a directory", root)
	}
	result := make([]string, 0)
	err = filepath.WalkDir(root, func(filePath string, entry os.DirEntry, walkErr error) error {
		if walkErr != nil {
			return walkErr
		}
		if entry.Type()&os.ModeSymlink != 0 {
			return fmt.Errorf("symlink is not supported: %s", filePath)
		}
		if entry.IsDir() {
			return nil
		}
		info, err := entry.Info()
		if err != nil {
			return err
		}
		if !info.Mode().IsRegular() {
			return fmt.Errorf("special file is not supported: %s", filePath)
		}
		result = append(result, filePath)
		return nil
	})
	if err != nil {
		return nil, err
	}
	sort.Strings(result)
	return result, nil
}

func openHistoryReadOnly(layout paths.Layout) (*history.Repository, error) {
	daemonConfig, err := config.LoadDaemonConfig(layout.DaemonConfig)
	if err != nil {
		return nil, err
	}
	database, err := history.ResolveConfig(layout, daemonConfig.History.Database)
	if err != nil {
		return nil, err
	}
	if database.Driver == "sqlite" {
		if _, err := os.Stat(database.Path); errors.Is(err, os.ErrNotExist) {
			return nil, nil
		} else if err != nil {
			return nil, err
		}
	}
	repository, err := history.OpenReadOnly(database)
	if err != nil {
		return nil, err
	}
	missing, err := repository.MissingTables(context.Background())
	if err != nil {
		repository.Close()
		return nil, err
	}
	if len(missing) > 0 {
		repository.Close()
		return nil, fmt.Errorf("history database is missing tables: %s", strings.Join(missing, ", "))
	}
	return repository, nil
}

func openHistoryWritable(layout paths.Layout) (*history.Repository, error) {
	daemonConfig, err := config.LoadDaemonConfig(layout.DaemonConfig)
	if err != nil {
		return nil, err
	}
	database, err := history.ResolveConfig(layout, daemonConfig.History.Database)
	if err != nil {
		return nil, err
	}
	return history.Open(database)
}

func projectHistoryFor(repository *history.Repository, ctx context.Context, project string) (history.ProjectHistory, error) {
	if repository == nil {
		return history.ProjectHistory{Version: history.ProjectHistoryVersion, Project: project}, nil
	}
	return repository.ExportProject(ctx, project)
}

func historyEmpty(data history.ProjectHistory) bool {
	return len(data.Runs) == 0 && len(data.ExecutionEvents) == 0 && len(data.Events) == 0 &&
		len(data.ScheduleOccurrences) == 0 && len(data.WebhookDeliveries) == 0 && len(data.SecretReferences) == 0 &&
		len(data.ResourcePolicies) == 0 && len(data.Counters) == 0
}

func loadImportProject(archiveData *extractedArchive, source, destination, configDir string, layout paths.Layout) (*importProject, error) {
	projectJSON, err := readExtractedJSON[projectData](archiveData, projectArchivePath(source, "project.json"))
	if err != nil {
		return nil, fmt.Errorf("read archive project %q metadata: %w", source, err)
	}
	if projectJSON.Version != projectDataVersion {
		return nil, fmt.Errorf("archive project %q has unsupported metadata version %d", source, projectJSON.Version)
	}
	projectJSON.Registry.Name = source
	state, err := readExtractedJSON[stateData](archiveData, projectArchivePath(source, "state.json"))
	if err != nil {
		return nil, fmt.Errorf("read archive project %q state: %w", source, err)
	}
	if state.Version != stateDataVersion {
		return nil, fmt.Errorf("archive project %q has unsupported state version %d", source, state.Version)
	}
	if err := validateLocalStateNames(state.ServicesDisabled, "service", source); err != nil {
		return nil, err
	}
	if err := validateLocalStateNames(state.SchedulesDisabled, "schedule", source); err != nil {
		return nil, err
	}
	historyData, err := readExtractedJSON[history.ProjectHistory](archiveData, projectArchivePath(source, "history.json"))
	if err != nil {
		return nil, fmt.Errorf("read archive project %q history: %w", source, err)
	}
	if historyData.Version != history.ProjectHistoryVersion || historyData.Project != source {
		return nil, fmt.Errorf("archive project %q has invalid history metadata", source)
	}
	if err := history.ValidateProjectHistory(historyData); err != nil {
		return nil, fmt.Errorf("validate archive project %q history: %w", source, err)
	}
	configPath := filepath.Join(configDir, destination, "mango.yaml")
	configBytes, err := readExtractedFile(archiveData, projectArchivePath(source, "config/mango.yaml"))
	if err != nil {
		return nil, fmt.Errorf("read archive project %q config: %w", source, err)
	}
	configStage, err := os.CreateTemp("", ".mango-import-config-*.yaml")
	if err != nil {
		return nil, err
	}
	configStagePath := configStage.Name()
	defer func() {
		configStage.Close()
		os.Remove(configStagePath)
	}()
	if err := configStage.Chmod(archiveFileMode); err != nil {
		configStage.Close()
		return nil, err
	}
	if _, err := configStage.Write(configBytes); err != nil {
		configStage.Close()
		return nil, err
	}
	if err := configStage.Close(); err != nil {
		return nil, err
	}
	loadedConfig, err := config.Load(configStagePath)
	if err != nil {
		return nil, fmt.Errorf("validate archive project %q config: %w", source, err)
	}
	if _, err := reconcile.Compile(loadedConfig, destination); err != nil {
		return nil, fmt.Errorf("compile archive project %q config: %w", source, err)
	}

	generations := map[uint64][]byte{}
	prefix := projectArchivePath(source, "generations/")
	for name, filePath := range archiveData.Files {
		if !strings.HasPrefix(name, prefix) {
			continue
		}
		base := strings.TrimPrefix(name, prefix)
		if strings.Contains(base, "/") || !strings.HasSuffix(base, ".json") {
			return nil, fmt.Errorf("archive project %q has invalid generation path %q", source, name)
		}
		value, err := strconv.ParseUint(strings.TrimSuffix(base, ".json"), 10, 64)
		if err != nil || value == 0 {
			return nil, fmt.Errorf("archive project %q has invalid generation path %q", source, name)
		}
		bytes, err := os.ReadFile(filePath)
		if err != nil {
			return nil, err
		}
		generationStage, err := os.CreateTemp("", ".mango-import-generation-*.json")
		if err != nil {
			return nil, err
		}
		generationStagePath := generationStage.Name()
		if _, err := generationStage.Write(bytes); err != nil {
			generationStage.Close()
			os.Remove(generationStagePath)
			return nil, err
		}
		if err := generationStage.Close(); err != nil {
			os.Remove(generationStagePath)
			return nil, err
		}
		if _, err := generation.LoadSnapshot(generationStagePath, source, value); err != nil {
			os.Remove(generationStagePath)
			return nil, fmt.Errorf("validate archive project %q generation %d: %w", source, value, err)
		}
		os.Remove(generationStagePath)
		generations[value] = bytes
	}
	if projectJSON.Registry.ConfigurationGeneration > 0 && generations[projectJSON.Registry.ConfigurationGeneration] == nil {
		return nil, fmt.Errorf("archive project %q points to missing generation %d", source, projectJSON.Registry.ConfigurationGeneration)
	}

	logFiles := map[string]string{}
	logPrefix := projectArchivePath(source, "logs/")
	for name, filePath := range archiveData.Files {
		if strings.HasPrefix(name, logPrefix) {
			relative := strings.TrimPrefix(name, logPrefix)
			if relative == "" || !safeArchiveRelativePath(relative) {
				return nil, fmt.Errorf("archive project %q has invalid log path %q", source, name)
			}
			logFiles[relative] = filePath
		}
	}
	return &importProject{
		Source: source, Destination: destination, Config: configBytes, Registry: projectJSON.Registry, State: state,
		Generations: generations, History: historyData, ConfigPath: configPath,
		LogRoot: projectPath(layout.Logs, destination), GenerationRoot: projectPath(generationRoot(layout), destination), LogFiles: logFiles,
	}, nil
}

func rewriteProject(project *importProject, layout paths.Layout) error {
	source, destination := project.Source, project.Destination
	if err := rewriteHistory(&project.History, source, destination, filepath.Join(layout.Logs, source), filepath.Join(layout.Logs, destination)); err != nil {
		return err
	}
	for value, data := range project.Generations {
		stage, err := os.CreateTemp("", ".mango-import-rewrite-*.json")
		if err != nil {
			return err
		}
		stagePath := stage.Name()
		if _, err := stage.Write(data); err != nil {
			stage.Close()
			os.Remove(stagePath)
			return err
		}
		if err := stage.Close(); err != nil {
			os.Remove(stagePath)
			return err
		}
		snapshot, err := generation.LoadSnapshot(stagePath, source, value)
		os.Remove(stagePath)
		if err != nil {
			return err
		}
		snapshot.Project = destination
		snapshot.Config.Name = destination
		snapshot.Config.Path = ""
		if source != destination {
			rewriteDesiredState(&snapshot, destination)
		}
		data, err = marshalJSON(snapshot)
		if err != nil {
			return err
		}
		project.Generations[value] = data
	}
	project.Registry.Name = destination
	project.Registry.ConfigPath = project.ConfigPath
	project.Registry.DesiredStatePath = ""
	project.Registry.ProcessIDs = nil
	project.Registry.ShimInstances = nil
	if project.Registry.ConfigurationGeneration > 0 {
		if project.GenerationRoot == "" {
			return errors.New("generation root is not configured")
		}
		project.Registry.DesiredStatePath = filepath.Join(project.GenerationRoot, fmt.Sprintf("%d.json", project.Registry.ConfigurationGeneration))
	}
	return nil
}

func rewriteDesiredState(snapshot *generation.Snapshot, project string) {
	if snapshot.Desired == nil {
		return
	}
	state := snapshot.Desired
	for index := range state.Services {
		state.Services[index].Project = project
	}
	for name, task := range state.Tasks {
		task.Project = project
		state.Tasks[name] = task
	}
	for name, workflow := range state.Workflows {
		workflow.Project = project
		state.Workflows[name] = workflow
	}
	for index := range state.Schedules {
		state.Schedules[index].Project = project
		state.Schedules[index].RunID = ""
		state.Schedules[index].OccurrenceID = ""
	}
	for index := range state.Webhooks {
		state.Webhooks[index].Project = project
	}
}

func rewriteHistory(data *history.ProjectHistory, source, destination, sourceLogs, destinationLogs string) error {
	data.Project = destination
	occurrences := make(map[string]string, len(data.ScheduleOccurrences))
	runIDs := make(map[string]bool, len(data.Runs))
	for index := range data.ScheduleOccurrences {
		occurrence := &data.ScheduleOccurrences[index]
		if source != destination {
			newID := scheduler.ScheduleOccurrenceID(destination, occurrence.Schedule, occurrence.ScheduledAt)
			occurrences[occurrence.ID] = newID
			occurrence.ID = newID
		}
		occurrence.Project = destination
	}
	for index := range data.Runs {
		run := &data.Runs[index]
		runIDs[run.RunID] = true
		run.Project = destination
		if mapped, ok := occurrences[run.Trigger.EventID]; ok {
			run.Trigger.EventID = mapped
			run.IdempotencyKey = replaceScheduleIdempotencyKey(run.IdempotencyKey, occurrences)
		}
		if source != destination {
			run.IdempotencyKey = strings.Replace(run.IdempotencyKey, "webhook:"+source+"/", "webhook:"+destination+"/", 1)
		}
		if err := rewriteHistoryLogPaths(&run.StdoutPath, &run.StderrPath, sourceLogs, destinationLogs); err != nil {
			return err
		}
		for taskIndex := range run.Tasks {
			task := &run.Tasks[taskIndex]
			if err := rewriteHistoryLogPaths(&task.StdoutPath, &task.StderrPath, sourceLogs, destinationLogs); err != nil {
				return err
			}
		}
	}
	for index := range data.Events {
		if data.Events[index].Project == source || runIDs[data.Events[index].RunID] {
			data.Events[index].Project = destination
		}
	}
	for index := range data.WebhookDeliveries {
		key := data.WebhookDeliveries[index].WebhookKey
		if source != destination {
			if key == source {
				key = destination
			} else if strings.HasPrefix(key, source+"/") {
				key = destination + strings.TrimPrefix(key, source)
			}
		}
		data.WebhookDeliveries[index].WebhookKey = key
	}
	for index := range data.SecretReferences {
		data.SecretReferences[index].Project = destination
	}
	for index := range data.ResourcePolicies {
		data.ResourcePolicies[index].Project = destination
	}
	for index := range data.Counters {
		parts := strings.SplitN(data.Counters[index].Key, "|", 3)
		if len(parts) < 2 || parts[1] != source {
			return fmt.Errorf("history counter %q is not owned by %q", data.Counters[index].Key, source)
		}
		parts[1] = destination
		data.Counters[index].Key = strings.Join(parts, "|")
	}
	return nil
}

func rewriteHistoryLogPaths(data *string, errorPath *string, sourceRoot, destinationRoot string) error {
	value, err := destinationLogPath(*data, sourceRoot, destinationRoot)
	if err != nil {
		return err
	}
	*data = value
	value, err = destinationLogPath(*errorPath, sourceRoot, destinationRoot)
	if err != nil {
		return err
	}
	*errorPath = value
	return nil
}

func logicalizeHistoryPaths(data *history.ProjectHistory, logsRoot string) error {
	for index := range data.Runs {
		run := &data.Runs[index]
		if err := archiveLogPath(&run.StdoutPath, filepath.Join(logsRoot, data.Project)); err != nil {
			return err
		}
		if err := archiveLogPath(&run.StderrPath, filepath.Join(logsRoot, data.Project)); err != nil {
			return err
		}
		for taskIndex := range run.Tasks {
			if err := archiveLogPath(&run.Tasks[taskIndex].StdoutPath, filepath.Join(logsRoot, data.Project)); err != nil {
				return err
			}
			if err := archiveLogPath(&run.Tasks[taskIndex].StderrPath, filepath.Join(logsRoot, data.Project)); err != nil {
				return err
			}
		}
	}
	return nil
}

func archiveLogPath(value *string, root string) error {
	if *value == "" {
		return nil
	}
	if strings.HasPrefix(filepath.ToSlash(*value), "logs/") {
		if !safeArchiveRelativePath(strings.TrimPrefix(filepath.ToSlash(*value), "logs/")) {
			return fmt.Errorf("history log path %q is invalid", *value)
		}
		return nil
	}
	absolute := *value
	if !filepath.IsAbs(absolute) {
		var err error
		absolute, err = filepath.Abs(absolute)
		if err != nil {
			return err
		}
	}
	relative, err := filepath.Rel(root, absolute)
	if err != nil || relative == ".." || strings.HasPrefix(relative, ".."+string(filepath.Separator)) {
		return fmt.Errorf("history log path %q is outside project log directory", *value)
	}
	*value = path.Join("logs", filepath.ToSlash(relative))
	return nil
}

func destinationLogPath(value, sourceRoot, destinationRoot string) (string, error) {
	if value == "" {
		return "", nil
	}
	if destinationRoot == "" {
		return "", errors.New("project log root is not configured")
	}
	if strings.HasPrefix(filepath.ToSlash(value), "logs/") {
		relative := strings.TrimPrefix(filepath.ToSlash(value), "logs/")
		if !safeArchiveRelativePath(relative) {
			return "", fmt.Errorf("invalid logical history log path %q", value)
		}
		return filepath.Join(destinationRoot, filepath.FromSlash(relative)), nil
	}
	if filepath.IsAbs(value) {
		relative, err := filepath.Rel(sourceRoot, value)
		if err != nil || relative == ".." || strings.HasPrefix(relative, ".."+string(filepath.Separator)) {
			return "", fmt.Errorf("history log path %q is outside project log directory", value)
		}
		return filepath.Join(destinationRoot, relative), nil
	}
	return "", fmt.Errorf("history log path %q is not portable", value)
}

func writeArchive(output string, projects []string, files []archiveFile, force bool) (int64, error) {
	if err := os.MkdirAll(filepath.Dir(output), archiveDirectoryMode); err != nil {
		return 0, fmt.Errorf("create archive directory: %w", err)
	}
	if info, err := os.Lstat(output); err == nil {
		if info.Mode()&os.ModeSymlink != 0 {
			return 0, fmt.Errorf("refusing to overwrite symlink %q", output)
		}
		if !info.Mode().IsRegular() {
			return 0, fmt.Errorf("archive output %q is not a regular file", output)
		}
		if !force {
			return 0, fmt.Errorf("archive output %q already exists; use --force to overwrite", output)
		}
	} else if !errors.Is(err, os.ErrNotExist) {
		return 0, err
	}
	for index := range files {
		if files[index].Source != "" {
			info, err := os.Stat(files[index].Source)
			if err != nil {
				return 0, err
			}
			files[index].Size = info.Size()
			files[index].SHA256, err = hashFile(files[index].Source)
			if err != nil {
				return 0, err
			}
		} else {
			files[index].Size = int64(len(files[index].Data))
			digest := sha256.Sum256(files[index].Data)
			files[index].SHA256 = hex.EncodeToString(digest[:])
		}
	}
	sort.Slice(files, func(i, j int) bool { return files[i].Name < files[j].Name })
	seen := map[string]bool{}
	manifestFiles := make([]ManifestFile, 0, len(files))
	for _, file := range files {
		if !safeArchiveRelativePath(file.Name) || file.Name == "manifest.json" || seen[file.Name] {
			return 0, fmt.Errorf("invalid or duplicate archive file %q", file.Name)
		}
		seen[file.Name] = true
		manifestFiles = append(manifestFiles, ManifestFile{Path: file.Name, Size: file.Size, SHA256: file.SHA256})
	}
	manifest, err := marshalJSON(Manifest{FormatVersion: FormatVersion, CreatedAt: time.Now().UTC(), Projects: append([]string(nil), projects...), Files: manifestFiles})
	if err != nil {
		return 0, err
	}
	temp, err := os.CreateTemp(filepath.Dir(output), ".mango-export-*.tmp")
	if err != nil {
		return 0, err
	}
	tempPath := temp.Name()
	defer os.Remove(tempPath)
	if err := temp.Chmod(archiveFileMode); err != nil {
		temp.Close()
		return 0, err
	}
	gzipWriter := gzip.NewWriter(temp)
	tarWriter := tar.NewWriter(gzipWriter)
	if err := writeTarData(tarWriter, "manifest.json", manifest); err != nil {
		tarWriter.Close()
		gzipWriter.Close()
		temp.Close()
		return 0, err
	}
	for _, file := range files {
		if err := writeTarFile(tarWriter, file); err != nil {
			tarWriter.Close()
			gzipWriter.Close()
			temp.Close()
			return 0, err
		}
	}
	if err := tarWriter.Close(); err != nil {
		gzipWriter.Close()
		temp.Close()
		return 0, err
	}
	if err := gzipWriter.Close(); err != nil {
		temp.Close()
		return 0, err
	}
	if err := temp.Sync(); err != nil {
		temp.Close()
		return 0, err
	}
	if err := temp.Close(); err != nil {
		return 0, err
	}
	if err := os.Rename(tempPath, output); err != nil {
		if !force {
			return 0, err
		}
		if removeErr := os.Remove(output); removeErr != nil {
			return 0, err
		}
		if err := os.Rename(tempPath, output); err != nil {
			return 0, err
		}
	}
	info, err := os.Stat(output)
	if err != nil {
		return 0, err
	}
	return info.Size(), nil
}

func writeTarData(writer *tar.Writer, name string, data []byte) error {
	if err := writer.WriteHeader(&tar.Header{Name: name, Mode: archiveFileMode, Size: int64(len(data)), Typeflag: tar.TypeReg}); err != nil {
		return err
	}
	_, err := writer.Write(data)
	return err
}

func writeTarFile(writer *tar.Writer, file archiveFile) error {
	if err := writer.WriteHeader(&tar.Header{Name: file.Name, Mode: archiveFileMode, Size: file.Size, Typeflag: tar.TypeReg}); err != nil {
		return err
	}
	var reader io.Reader
	var closer io.Closer
	if file.Source != "" {
		input, err := os.Open(file.Source)
		if err != nil {
			return err
		}
		reader, closer = input, input
	} else {
		reader = bytes.NewReader(file.Data)
	}
	if closer != nil {
		defer closer.Close()
	}
	hasher := sha256.New()
	written, err := io.Copy(io.MultiWriter(writer, hasher), reader)
	if err != nil {
		return err
	}
	if written != file.Size || hex.EncodeToString(hasher.Sum(nil)) != file.SHA256 {
		return fmt.Errorf("source file %q changed during export", file.Source)
	}
	return nil
}

func readArchive(filePath string) (*extractedArchive, error) {
	input, err := os.Open(filePath)
	if err != nil {
		return nil, fmt.Errorf("open archive: %w", err)
	}
	defer input.Close()
	gzipReader, err := gzip.NewReader(input)
	if err != nil {
		return nil, fmt.Errorf("read archive gzip: %w", err)
	}
	defer gzipReader.Close()
	tarReader := tar.NewReader(gzipReader)
	header, err := tarReader.Next()
	if err != nil {
		return nil, fmt.Errorf("read archive manifest: %w", err)
	}
	if header.Name != "manifest.json" || header.Typeflag != tar.TypeReg || header.Size < 0 || header.Size > maxManifestBytes {
		return nil, errors.New("archive must begin with manifest.json")
	}
	manifestBytes, err := io.ReadAll(io.LimitReader(tarReader, maxManifestBytes+1))
	if err != nil {
		return nil, fmt.Errorf("read archive manifest: %w", err)
	}
	if int64(len(manifestBytes)) != header.Size {
		return nil, errors.New("archive manifest size is invalid")
	}
	var manifest Manifest
	if err := json.Unmarshal(manifestBytes, &manifest); err != nil {
		return nil, fmt.Errorf("decode archive manifest: %w", err)
	}
	if err := validateManifest(manifest); err != nil {
		return nil, err
	}
	root, err := os.MkdirTemp("", ".mango-import-*")
	if err != nil {
		return nil, err
	}
	result := &extractedArchive{Root: root, Manifest: manifest, Files: map[string]string{}}
	fileByName := make(map[string]ManifestFile, len(manifest.Files))
	for _, file := range manifest.Files {
		fileByName[file.Path] = file
	}
	for {
		header, err = tarReader.Next()
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil {
			os.RemoveAll(root)
			return nil, fmt.Errorf("read archive entry: %w", err)
		}
		if header.Typeflag != tar.TypeReg || header.Size < 0 || !safeArchiveRelativePath(header.Name) || header.Name == "manifest.json" {
			os.RemoveAll(root)
			return nil, fmt.Errorf("archive entry %q is not a safe regular file", header.Name)
		}
		expected, ok := fileByName[header.Name]
		if !ok {
			os.RemoveAll(root)
			return nil, fmt.Errorf("archive contains unlisted file %q", header.Name)
		}
		if _, exists := result.Files[header.Name]; exists {
			os.RemoveAll(root)
			return nil, fmt.Errorf("archive contains duplicate file %q", header.Name)
		}
		stagePath, err := safeJoin(root, header.Name)
		if err != nil {
			os.RemoveAll(root)
			return nil, err
		}
		if err := os.MkdirAll(filepath.Dir(stagePath), archiveDirectoryMode); err != nil {
			os.RemoveAll(root)
			return nil, err
		}
		output, err := os.OpenFile(stagePath, os.O_CREATE|os.O_EXCL|os.O_WRONLY, archiveFileMode)
		if err != nil {
			os.RemoveAll(root)
			return nil, err
		}
		hasher := sha256.New()
		written, copyErr := io.Copy(io.MultiWriter(output, hasher), io.LimitReader(tarReader, header.Size))
		closeErr := output.Close()
		if copyErr != nil {
			os.RemoveAll(root)
			return nil, copyErr
		}
		if closeErr != nil {
			os.RemoveAll(root)
			return nil, closeErr
		}
		if header.Size != expected.Size {
			os.RemoveAll(root)
			return nil, fmt.Errorf("archive entry %q size does not match manifest", header.Name)
		}
		if written != header.Size || expected.SHA256 != hex.EncodeToString(hasher.Sum(nil)) {
			os.RemoveAll(root)
			return nil, fmt.Errorf("archive checksum mismatch for %q", header.Name)
		}
		result.Files[header.Name] = stagePath
	}
	if len(result.Files) != len(manifest.Files) {
		os.RemoveAll(root)
		return nil, errors.New("archive is missing one or more manifest files")
	}
	return result, nil
}

func validateManifest(manifest Manifest) error {
	if manifest.FormatVersion != FormatVersion {
		return fmt.Errorf("unsupported archive format version %d", manifest.FormatVersion)
	}
	if len(manifest.Projects) == 0 {
		return errors.New("archive contains no projects")
	}
	seenProjects := map[string]bool{}
	for _, project := range manifest.Projects {
		if err := config.ValidateProjectName(project); err != nil {
			return fmt.Errorf("archive project %q: %w", project, err)
		}
		if seenProjects[project] {
			return fmt.Errorf("archive contains duplicate project %q", project)
		}
		seenProjects[project] = true
	}
	seenFiles := map[string]bool{}
	required := make(map[string]map[string]bool, len(manifest.Projects))
	for _, project := range manifest.Projects {
		required[project] = map[string]bool{}
	}
	for _, file := range manifest.Files {
		if !safeArchiveRelativePath(file.Path) || file.Path == "manifest.json" || file.Size < 0 || len(file.SHA256) != sha256.Size*2 || seenFiles[file.Path] {
			return fmt.Errorf("archive manifest contains invalid file %q", file.Path)
		}
		if _, err := hex.DecodeString(file.SHA256); err != nil {
			return fmt.Errorf("archive manifest contains invalid checksum for %q", file.Path)
		}
		parts := strings.Split(file.Path, "/")
		if len(parts) < 3 || parts[0] != "projects" || required[parts[1]] == nil {
			return fmt.Errorf("archive manifest contains file outside a project: %q", file.Path)
		}
		suffix := strings.Join(parts[2:], "/")
		switch {
		case suffix == "config/mango.yaml", suffix == "project.json", suffix == "state.json", suffix == "history.json":
			required[parts[1]][suffix] = true
		case strings.HasPrefix(suffix, "generations/"), strings.HasPrefix(suffix, "logs/"):
		default:
			return fmt.Errorf("archive manifest contains unsupported project file %q", file.Path)
		}
		seenFiles[file.Path] = true
	}
	for _, project := range manifest.Projects {
		for _, suffix := range []string{"config/mango.yaml", "project.json", "state.json", "history.json"} {
			if !required[project][suffix] {
				return fmt.Errorf("archive project %q is missing %s", project, suffix)
			}
		}
	}
	return nil
}

func readExtractedJSON[T any](archiveData *extractedArchive, name string) (T, error) {
	var result T
	data, err := readExtractedFile(archiveData, name)
	if err != nil {
		return result, err
	}
	if err := json.Unmarshal(data, &result); err != nil {
		return result, err
	}
	return result, nil
}

func readExtractedFile(archiveData *extractedArchive, name string) ([]byte, error) {
	filePath, ok := archiveData.Files[name]
	if !ok {
		return nil, fmt.Errorf("archive file %q is missing", name)
	}
	return os.ReadFile(filePath)
}

func projectArchivePath(project, suffix string) string {
	return "projects/" + project + "/" + suffix
}

func safeArchiveRelativePath(value string) bool {
	if value == "" || strings.Contains(value, "\\") || strings.HasPrefix(value, "/") {
		return false
	}
	clean := path.Clean(value)
	return clean == value && clean != "." && clean != ".." && !strings.HasPrefix(clean, "../")
}

func safeJoin(root, relative string) (string, error) {
	if !safeArchiveRelativePath(relative) {
		return "", fmt.Errorf("unsafe archive path %q", relative)
	}
	joined := filepath.Join(root, filepath.FromSlash(relative))
	rootAbs, err := filepath.Abs(root)
	if err != nil {
		return "", err
	}
	joinedAbs, err := filepath.Abs(joined)
	if err != nil {
		return "", err
	}
	if joinedAbs != rootAbs && !strings.HasPrefix(joinedAbs, rootAbs+string(filepath.Separator)) {
		return "", fmt.Errorf("unsafe archive path %q", relative)
	}
	return joined, nil
}

func dataFile(name string, data []byte) archiveFile {
	return archiveFile{Name: name, Data: data}
}

func sourceFile(name, source string) archiveFile {
	return archiveFile{Name: name, Source: source}
}

func marshalJSON(value interface{}) ([]byte, error) {
	data, err := json.MarshalIndent(value, "", "  ")
	if err != nil {
		return nil, err
	}
	return append(data, '\n'), nil
}

func readRegularFile(filePath string) ([]byte, error) {
	info, err := os.Lstat(filePath)
	if err != nil {
		return nil, err
	}
	if info.Mode()&os.ModeSymlink != 0 || !info.Mode().IsRegular() {
		return nil, fmt.Errorf("%s is not a regular file", filePath)
	}
	return os.ReadFile(filePath)
}

func hashFile(filePath string) (string, error) {
	input, err := os.Open(filePath)
	if err != nil {
		return "", err
	}
	defer input.Close()
	hasher := sha256.New()
	if _, err := io.Copy(hasher, input); err != nil {
		return "", err
	}
	return hex.EncodeToString(hasher.Sum(nil)), nil
}

func writeBytesAtomic(filePath string, data []byte) error {
	if strings.TrimSpace(filePath) == "" {
		return errors.New("cannot write empty path")
	}
	if err := os.MkdirAll(filepath.Dir(filePath), archiveDirectoryMode); err != nil {
		return err
	}
	temp, err := os.CreateTemp(filepath.Dir(filePath), ".mango-archive-*.tmp")
	if err != nil {
		return err
	}
	tempPath := temp.Name()
	defer os.Remove(tempPath)
	if err := temp.Chmod(archiveFileMode); err != nil {
		temp.Close()
		return err
	}
	if _, err := temp.Write(data); err != nil {
		temp.Close()
		return err
	}
	if err := temp.Sync(); err != nil {
		temp.Close()
		return err
	}
	if err := temp.Close(); err != nil {
		return err
	}
	return renameReplacing(tempPath, filePath)
}

func renameReplacing(source, destination string) error {
	if err := os.Rename(source, destination); err == nil {
		return nil
	} else if _, statErr := os.Lstat(destination); statErr != nil {
		return err
	}
	if err := os.Remove(destination); err != nil {
		return err
	}
	return os.Rename(source, destination)
}

func installProjects(layout paths.Layout, projects []importProject, destinationRegistry registry.File, services, schedules map[string]bool, replace bool) error {
	for _, project := range projects {
		if err := installProjectFiles(project); err != nil {
			return err
		}
	}
	for _, project := range projects {
		if replace {
			prefix := project.Destination + "/"
			for key := range services {
				if strings.HasPrefix(key, prefix) {
					delete(services, key)
				}
			}
			for key := range schedules {
				if strings.HasPrefix(key, prefix) {
					delete(schedules, key)
				}
			}
		}
		for _, local := range project.State.ServicesDisabled {
			services[project.Destination+"/"+local] = true
		}
		for _, local := range project.State.SchedulesDisabled {
			schedules[project.Destination+"/"+local] = true
		}
	}
	if err := writeDisabledState(layout.ServiceState, "service", services); err != nil {
		return err
	}
	if err := writeDisabledState(layout.ScheduleState, "schedule", schedules); err != nil {
		return err
	}
	for _, project := range projects {
		destinationRegistry.Projects[project.Destination] = project.Registry
	}
	return registry.Save(layout.Registry, destinationRegistry)
}

func installProjectFiles(project importProject) error {
	if err := os.MkdirAll(filepath.Dir(project.ConfigPath), archiveDirectoryMode); err != nil {
		return err
	}
	if err := writeBytesAtomic(project.ConfigPath, project.Config); err != nil {
		return fmt.Errorf("install project %q config: %w", project.Destination, err)
	}
	if project.GenerationRoot != "" {
		if err := os.RemoveAll(project.GenerationRoot); err != nil {
			return fmt.Errorf("replace project %q generations: %w", project.Destination, err)
		}
	}
	if project.GenerationRoot != "" && len(project.Generations) > 0 {
		if err := os.MkdirAll(project.GenerationRoot, archiveDirectoryMode); err != nil {
			return err
		}
		values := make([]uint64, 0, len(project.Generations))
		for value := range project.Generations {
			values = append(values, value)
		}
		sort.Slice(values, func(i, j int) bool { return values[i] < values[j] })
		for _, value := range values {
			if err := writeBytesAtomic(filepath.Join(project.GenerationRoot, fmt.Sprintf("%d.json", value)), project.Generations[value]); err != nil {
				return err
			}
		}
	}
	if project.LogRoot != "" {
		if err := os.RemoveAll(project.LogRoot); err != nil {
			return fmt.Errorf("replace project %q logs: %w", project.Destination, err)
		}
	}
	for relative, stagePath := range project.LogFiles {
		if project.LogRoot == "" {
			return fmt.Errorf("project %q log root is not configured", project.Destination)
		}
		destination, err := safeLocalJoin(project.LogRoot, relative)
		if err != nil {
			return err
		}
		if err := copyRegularFile(stagePath, destination); err != nil {
			return fmt.Errorf("install project %q log: %w", project.Destination, err)
		}
	}
	return nil
}

func safeLocalJoin(root, relative string) (string, error) {
	if relative == "" || filepath.IsAbs(relative) {
		return "", fmt.Errorf("invalid local archive path %q", relative)
	}
	joined := filepath.Join(root, filepath.FromSlash(relative))
	rootAbs, err := filepath.Abs(root)
	if err != nil {
		return "", err
	}
	joinedAbs, err := filepath.Abs(joined)
	if err != nil {
		return "", err
	}
	if joinedAbs != rootAbs && !strings.HasPrefix(joinedAbs, rootAbs+string(filepath.Separator)) {
		return "", fmt.Errorf("invalid local archive path %q", relative)
	}
	return joined, nil
}

func importHistory(ctx context.Context, layout paths.Layout, projects []importProject, historyDatabaseAvailable, replace bool) error {
	needsHistory := historyDatabaseAvailable
	data := make([]history.ProjectHistory, 0, len(projects))
	for _, project := range projects {
		data = append(data, project.History)
		needsHistory = needsHistory || !historyEmpty(project.History)
	}
	if !needsHistory {
		return nil
	}
	repository, err := openHistoryWritable(layout)
	if err != nil {
		return err
	}
	defer repository.Close()
	return repository.ImportProjects(ctx, data, replace)
}

func destinationConflict(layout paths.Layout, current registry.File, destination, configPath string, services, schedules map[string]bool, replace bool) error {
	managedPaths := []string{configPath, projectPath(layout.Logs, destination), projectPath(generationRoot(layout), destination)}
	if replace {
		for _, filePath := range managedPaths {
			if filePath == "" {
				continue
			}
			info, err := os.Lstat(filePath)
			if err == nil && info.Mode()&os.ModeSymlink != 0 {
				return fmt.Errorf("refusing to replace symlink %q", filePath)
			}
			if err != nil && !errors.Is(err, os.ErrNotExist) {
				return err
			}
		}
		return nil
	}
	if _, ok := current.Projects[destination]; ok {
		return fmt.Errorf("destination project %q already exists; use --replace --yes", destination)
	}
	for _, filePath := range managedPaths {
		if _, err := os.Lstat(filePath); err == nil {
			return fmt.Errorf("destination project %q already has data at %s; use --replace --yes", destination, filePath)
		} else if !errors.Is(err, os.ErrNotExist) {
			return err
		}
	}
	prefix := destination + "/"
	for key := range services {
		if strings.HasPrefix(key, prefix) {
			return fmt.Errorf("destination project %q already has service state; use --replace --yes", destination)
		}
	}
	for key := range schedules {
		if strings.HasPrefix(key, prefix) {
			return fmt.Errorf("destination project %q already has schedule state; use --replace --yes", destination)
		}
	}
	return nil
}

type backup struct {
	Root        string
	JournalPath string
	Entries     []backupEntry
	Histories   map[string]string
}

type backupEntry struct {
	Original string `json:"original"`
	Saved    string `json:"saved"`
	Exists   bool   `json:"exists"`
}

type importJournal struct {
	Version    int               `json:"version"`
	Phase      string            `json:"phase"`
	BackupRoot string            `json:"backup_root"`
	Entries    []backupEntry     `json:"entries"`
	Histories  map[string]string `json:"histories"`
}

func newBackup(layout paths.Layout, projects []importProject, histories map[string]history.ProjectHistory) (*backup, error) {
	runtimeRoot := layout.Runtime
	if runtimeRoot == "" {
		runtimeRoot = filepath.Dir(layout.LockPath)
	}
	if runtimeRoot == "" {
		return nil, errors.New("import backup directory is not configured")
	}
	runtimeRoot, err := filepath.Abs(runtimeRoot)
	if err != nil {
		return nil, err
	}
	if err := os.MkdirAll(runtimeRoot, archiveDirectoryMode); err != nil {
		return nil, err
	}
	root, err := os.MkdirTemp(runtimeRoot, ".mango-import-backup-*")
	if err != nil {
		return nil, err
	}
	result := &backup{Root: root, Histories: map[string]string{}}
	pathsToSave := []string{layout.Registry, layout.ServiceState, layout.ScheduleState}
	for _, project := range projects {
		pathsToSave = append(pathsToSave, project.ConfigPath, project.LogRoot, project.GenerationRoot)
	}
	seen := map[string]bool{}
	for _, original := range pathsToSave {
		if original == "" || seen[original] {
			continue
		}
		seen[original] = true
		entry, err := result.save(original, len(result.Entries))
		if err != nil {
			result.Cleanup()
			return nil, err
		}
		result.Entries = append(result.Entries, entry)
	}
	if err := os.MkdirAll(filepath.Join(root, "history"), archiveDirectoryMode); err != nil {
		result.Cleanup()
		return nil, err
	}
	projectsByName := append([]importProject(nil), projects...)
	sort.Slice(projectsByName, func(i, j int) bool { return projectsByName[i].Destination < projectsByName[j].Destination })
	for index, project := range projectsByName {
		data := histories[project.Destination]
		if data.Version == 0 {
			data = history.ProjectHistory{Version: history.ProjectHistoryVersion, Project: project.Destination}
		}
		dataBytes, err := marshalJSON(data)
		if err != nil {
			result.Cleanup()
			return nil, err
		}
		saved := filepath.Join(root, "history", fmt.Sprintf("%d.json", index))
		if err := writeBytesAtomic(saved, dataBytes); err != nil {
			result.Cleanup()
			return nil, err
		}
		result.Histories[project.Destination] = saved
	}
	return result, nil
}

func (b *backup) WriteJournal(layout paths.Layout, phase string) error {
	if b == nil || b.Root == "" {
		return errors.New("import backup is not initialized")
	}
	if phase != importPhasePrepared && phase != importPhaseFiles && phase != importPhaseHistory && phase != importPhaseCommitted {
		return fmt.Errorf("invalid import journal phase %q", phase)
	}
	b.JournalPath = importJournalPath(layout)
	if b.JournalPath == "" {
		return errors.New("import journal path is not configured")
	}
	data, err := marshalJSON(importJournal{
		Version: importJournalVersion, Phase: phase, BackupRoot: b.Root,
		Entries: b.Entries, Histories: b.Histories,
	})
	if err != nil {
		return err
	}
	if err := writeBytesAtomic(b.JournalPath, data); err != nil {
		return fmt.Errorf("write import journal: %w", err)
	}
	return nil
}

func (b *backup) RemoveJournal() error {
	if b == nil || b.JournalPath == "" {
		return nil
	}
	if err := os.Remove(b.JournalPath); err != nil && !errors.Is(err, os.ErrNotExist) {
		return err
	}
	return nil
}

func (b *backup) save(original string, index int) (backupEntry, error) {
	var err error
	original, err = filepath.Abs(original)
	if err != nil {
		return backupEntry{}, err
	}
	entry := backupEntry{Original: original, Saved: filepath.Join(b.Root, strconv.Itoa(index))}
	info, err := os.Lstat(original)
	if errors.Is(err, os.ErrNotExist) {
		return entry, nil
	}
	if err != nil {
		return entry, err
	}
	if info.Mode()&os.ModeSymlink != 0 {
		return entry, fmt.Errorf("refusing to backup symlink %q", original)
	}
	entry.Exists = true
	if err := copyPath(original, entry.Saved); err != nil {
		return entry, err
	}
	return entry, nil
}

func (b *backup) Restore() error {
	var failures []string
	for index := len(b.Entries) - 1; index >= 0; index-- {
		entry := b.Entries[index]
		if err := os.RemoveAll(entry.Original); err != nil {
			failures = append(failures, fmt.Sprintf("remove %s: %v", entry.Original, err))
			continue
		}
		if !entry.Exists {
			continue
		}
		if err := copyPath(entry.Saved, entry.Original); err != nil {
			failures = append(failures, fmt.Sprintf("restore %s: %v", entry.Original, err))
		}
	}
	if len(failures) > 0 {
		return errors.New(strings.Join(failures, "; "))
	}
	return nil
}

func (b *backup) RestoreAll(ctx context.Context, layout paths.Layout, restoreHistory bool) error {
	var failures []string
	if restoreHistory {
		if err := b.RestoreHistory(ctx, layout); err != nil {
			failures = append(failures, fmt.Sprintf("restore history: %v", err))
		}
	}
	if err := b.Restore(); err != nil {
		failures = append(failures, fmt.Sprintf("restore files: %v", err))
	}
	if len(failures) > 0 {
		return errors.New(strings.Join(failures, "; "))
	}
	return nil
}

func (b *backup) RestoreHistory(ctx context.Context, layout paths.Layout) error {
	if len(b.Histories) == 0 {
		return nil
	}
	projects := make([]string, 0, len(b.Histories))
	data := make([]history.ProjectHistory, 0, len(b.Histories))
	for project := range b.Histories {
		projects = append(projects, project)
	}
	sort.Strings(projects)
	for _, project := range projects {
		filePath := b.Histories[project]
		value, err := readRegularFile(filePath)
		if err != nil {
			return err
		}
		var restored history.ProjectHistory
		if err := json.Unmarshal(value, &restored); err != nil {
			return err
		}
		if restored.Project != project || restored.Version != history.ProjectHistoryVersion {
			return fmt.Errorf("backup history for %q is invalid", project)
		}
		data = append(data, restored)
	}
	repository, err := openHistoryWritable(layout)
	if err != nil {
		return err
	}
	defer repository.Close()
	return repository.ImportProjects(ctx, data, true)
}

func (b *backup) Cleanup() error {
	if b == nil || b.Root == "" {
		return nil
	}
	return os.RemoveAll(b.Root)
}

func importJournalPath(layout paths.Layout) string {
	runtimeRoot := layout.Runtime
	if runtimeRoot == "" {
		runtimeRoot = filepath.Dir(layout.LockPath)
	}
	if runtimeRoot == "" {
		return ""
	}
	runtimeRoot, err := filepath.Abs(runtimeRoot)
	if err != nil {
		return ""
	}
	return filepath.Join(runtimeRoot, importJournalName)
}

func readImportJournal(layout paths.Layout) (importJournal, bool, error) {
	journalPath := importJournalPath(layout)
	if journalPath == "" {
		return importJournal{}, false, errors.New("import journal path is not configured")
	}
	info, err := os.Lstat(journalPath)
	if errors.Is(err, os.ErrNotExist) {
		return importJournal{}, false, nil
	}
	if err != nil {
		return importJournal{}, false, err
	}
	if info.Mode()&os.ModeSymlink != 0 || !info.Mode().IsRegular() {
		return importJournal{}, true, errors.New("import journal is not a regular file")
	}
	data, err := os.ReadFile(journalPath)
	if err != nil {
		return importJournal{}, false, err
	}
	var journal importJournal
	if err := json.Unmarshal(data, &journal); err != nil {
		return importJournal{}, true, fmt.Errorf("decode import journal: %w", err)
	}
	if err := validateImportJournal(layout, journal); err != nil {
		return importJournal{}, true, err
	}
	return journal, true, nil
}

func validateImportJournal(layout paths.Layout, journal importJournal) error {
	if journal.Version != importJournalVersion {
		return fmt.Errorf("unsupported import journal version %d", journal.Version)
	}
	if journal.Phase != importPhasePrepared && journal.Phase != importPhaseFiles && journal.Phase != importPhaseHistory && journal.Phase != importPhaseCommitted {
		return fmt.Errorf("invalid import journal phase %q", journal.Phase)
	}
	runtimeRoot := layout.Runtime
	if runtimeRoot == "" {
		runtimeRoot = filepath.Dir(layout.LockPath)
	}
	runtimeRoot, err := filepath.Abs(runtimeRoot)
	if err != nil || runtimeRoot == "" {
		return errors.New("import journal runtime path is invalid")
	}
	root, err := filepath.Abs(journal.BackupRoot)
	if err != nil || !pathWithin(runtimeRoot, root) || root == filepath.Clean(runtimeRoot) {
		return errors.New("import journal backup path is invalid")
	}
	if info, err := os.Stat(root); err != nil || !info.IsDir() {
		if err == nil {
			err = errors.New("not a directory")
		}
		return fmt.Errorf("import journal backup is unavailable: %w", err)
	}
	if len(journal.Entries) == 0 || len(journal.Histories) == 0 {
		return errors.New("import journal is incomplete")
	}
	seen := map[string]bool{}
	for _, entry := range journal.Entries {
		original, err := filepath.Abs(entry.Original)
		if err != nil || !filepath.IsAbs(entry.Original) || filepath.Clean(entry.Original) != entry.Original || filepath.Dir(original) == original || seen[original] || (entry.Exists && (!pathWithin(root, entry.Saved) || filepath.Clean(entry.Saved) == filepath.Clean(root))) {
			return errors.New("import journal contains invalid backup entry")
		}
		seen[original] = true
	}
	for project, filePath := range journal.Histories {
		if err := config.ValidateProjectName(project); err != nil {
			return fmt.Errorf("import journal history project %q: %w", project, err)
		}
		if !pathWithin(root, filePath) || filepath.Clean(filePath) == filepath.Clean(root) {
			return fmt.Errorf("import journal history path for %q is invalid", project)
		}
	}
	return nil
}

func backupFromJournal(layout paths.Layout, journal importJournal) (*backup, error) {
	if err := validateImportJournal(layout, journal); err != nil {
		return nil, err
	}
	root, _ := filepath.Abs(journal.BackupRoot)
	entries := append([]backupEntry(nil), journal.Entries...)
	for index := range entries {
		entries[index].Original, _ = filepath.Abs(entries[index].Original)
		if entries[index].Exists {
			entries[index].Saved, _ = filepath.Abs(entries[index].Saved)
		}
	}
	histories := make(map[string]string, len(journal.Histories))
	for project, filePath := range journal.Histories {
		histories[project], _ = filepath.Abs(filePath)
	}
	return &backup{Root: root, JournalPath: importJournalPath(layout), Entries: entries, Histories: histories}, nil
}

func pathWithin(root, candidate string) bool {
	rootAbs, err := filepath.Abs(root)
	if err != nil {
		return false
	}
	candidateAbs, err := filepath.Abs(candidate)
	if err != nil {
		return false
	}
	rootAbs = filepath.Clean(rootAbs)
	candidateAbs = filepath.Clean(candidateAbs)
	return candidateAbs == rootAbs || strings.HasPrefix(candidateAbs, rootAbs+string(filepath.Separator))
}

func copyPath(source, destination string) error {
	info, err := os.Lstat(source)
	if err != nil {
		return err
	}
	if info.Mode()&os.ModeSymlink != 0 {
		return fmt.Errorf("symlink is not supported: %s", source)
	}
	if info.IsDir() {
		if err := os.MkdirAll(destination, archiveDirectoryMode); err != nil {
			return err
		}
		entries, err := os.ReadDir(source)
		if err != nil {
			return err
		}
		for _, entry := range entries {
			if err := copyPath(filepath.Join(source, entry.Name()), filepath.Join(destination, entry.Name())); err != nil {
				return err
			}
		}
		return os.Chmod(destination, archiveDirectoryMode)
	}
	if !info.Mode().IsRegular() {
		return fmt.Errorf("special file is not supported: %s", source)
	}
	return copyRegularFile(source, destination)
}

func copyRegularFile(source, destination string) error {
	if err := os.MkdirAll(filepath.Dir(destination), archiveDirectoryMode); err != nil {
		return err
	}
	input, err := os.Open(source)
	if err != nil {
		return err
	}
	defer input.Close()
	output, err := os.OpenFile(destination, os.O_CREATE|os.O_TRUNC|os.O_WRONLY, archiveFileMode)
	if err != nil {
		return err
	}
	if _, err := io.Copy(output, input); err != nil {
		output.Close()
		return err
	}
	if err := output.Chmod(archiveFileMode); err != nil {
		output.Close()
		return err
	}
	return output.Close()
}

func replaceScheduleIdempotencyKey(value string, occurrences map[string]string) string {
	for oldID, newID := range occurrences {
		if value == scheduler.ScheduleOccurrenceIdempotencyKey(oldID) {
			return scheduler.ScheduleOccurrenceIdempotencyKey(newID)
		}
	}
	return value
}
