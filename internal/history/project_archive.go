package history

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"strings"
	"time"

	"github.com/kevin93203/mango/internal/observability"
	"gorm.io/gorm"
)

// ProjectHistoryVersion is the version of the logical, driver-independent
// project history payload. It is intentionally separate from the SQL schema
// version.
const ProjectHistoryVersion = 1

// ProjectHistory is the durable history owned by one project. Database row
// IDs are deliberately omitted; RunID and archive-local refs preserve the
// relationships that users can observe without coupling the archive to one
// database engine.
type ProjectHistory struct {
	Version             int                         `json:"version"`
	Project             string                      `json:"project"`
	Runs                []HistoryRun                `json:"runs,omitempty"`
	ExecutionEvents     []HistoryExecutionEvent     `json:"execution_events,omitempty"`
	Events              []observability.Event       `json:"events,omitempty"`
	ScheduleOccurrences []HistoryScheduleOccurrence `json:"schedule_occurrences,omitempty"`
	WebhookDeliveries   []HistoryWebhookDelivery    `json:"webhook_deliveries,omitempty"`
	SecretReferences    []HistorySecretReference    `json:"secret_references,omitempty"`
	ResourcePolicies    []HistoryResourcePolicy     `json:"resource_policies,omitempty"`
	Counters            []HistoryCounter            `json:"counters,omitempty"`
}

type HistoryRun struct {
	Ref                     string           `json:"ref"`
	RunID                   string           `json:"run_id"`
	Project                 string           `json:"project"`
	Name                    string           `json:"name"`
	TargetType              string           `json:"target_type"`
	Target                  string           `json:"target"`
	Trigger                 HistoryTrigger   `json:"trigger"`
	Status                  string           `json:"status"`
	Started                 *time.Time       `json:"started"`
	Finished                *time.Time       `json:"finished"`
	ExitCode                int              `json:"exit_code"`
	Error                   string           `json:"error"`
	Stderr                  string           `json:"stderr"`
	StdoutPath              string           `json:"stdout_path"`
	StderrPath              string           `json:"stderr_path"`
	IdempotencyKey          string           `json:"idempotency_key"`
	ConfigurationGeneration uint64           `json:"configuration_generation"`
	RetriedFromRunID        string           `json:"retried_from_run_id"`
	CreatedAt               time.Time        `json:"created_at"`
	UpdatedAt               time.Time        `json:"updated_at"`
	Attempts                []HistoryAttempt `json:"attempts,omitempty"`
	Tasks                   []HistoryTask    `json:"tasks,omitempty"`
}

type HistoryTrigger struct {
	Type    string `json:"type,omitempty"`
	Name    string `json:"name,omitempty"`
	Mode    string `json:"mode,omitempty"`
	EventID string `json:"event_id,omitempty"`
}

type HistoryTask struct {
	Ref             string            `json:"ref"`
	RunRef          string            `json:"run_ref"`
	RunID           string            `json:"run_id"`
	ParentRunID     string            `json:"parent_run_id"`
	Node            string            `json:"node"`
	Task            string            `json:"task"`
	Command         string            `json:"command"`
	Args            []string          `json:"args"`
	WorkingDir      string            `json:"working_dir"`
	EnvKeys         []string          `json:"env_keys"`
	ArgsRedacted    bool              `json:"args_redacted"`
	Status          string            `json:"status"`
	SkipReason      string            `json:"skip_reason"`
	TimeoutNanos    *int64            `json:"timeout_nanos"`
	RetryCount      *int              `json:"retry_count"`
	RetryDelayNanos *int64            `json:"retry_delay_nanos"`
	AllowFailure    *bool             `json:"allow_failure"`
	PolicyResolved  *bool             `json:"policy_resolved"`
	Started         *time.Time        `json:"started"`
	Finished        *time.Time        `json:"finished"`
	DurationSeconds float64           `json:"duration_seconds"`
	ExitCode        int               `json:"exit_code"`
	Error           string            `json:"error"`
	Stderr          string            `json:"stderr"`
	StdoutPath      string            `json:"stdout_path"`
	StderrPath      string            `json:"stderr_path"`
	Attempts        []HistoryAttempt  `json:"attempts,omitempty"`
	Artifacts       []HistoryArtifact `json:"artifacts,omitempty"`
}

type HistoryAttempt struct {
	Number          int        `json:"number"`
	Started         *time.Time `json:"started"`
	Finished        *time.Time `json:"finished"`
	DurationSeconds float64    `json:"duration_seconds"`
	ExitCode        int        `json:"exit_code"`
	Error           string     `json:"error"`
	Stderr          string     `json:"stderr"`
}

type HistoryArtifact struct {
	Path   string `json:"path"`
	Exists bool   `json:"exists"`
	Size   int64  `json:"size"`
	SHA256 string `json:"sha256"`
}

type HistoryExecutionEvent struct {
	RunID     string    `json:"run_id"`
	Type      string    `json:"type"`
	Status    string    `json:"status"`
	Details   string    `json:"details"`
	CreatedAt time.Time `json:"created_at"`
}

type HistoryScheduleOccurrence struct {
	ID          string    `json:"id"`
	Project     string    `json:"project"`
	Schedule    string    `json:"schedule"`
	ScheduledAt time.Time `json:"scheduled_at"`
	Status      string    `json:"status"`
	RunID       string    `json:"run_id"`
	Misfire     string    `json:"misfire"`
	CreatedAt   time.Time `json:"created_at"`
	UpdatedAt   time.Time `json:"updated_at"`
}

type HistoryWebhookDelivery struct {
	WebhookKey     string    `json:"webhook_key"`
	IdempotencyKey string    `json:"idempotency_key"`
	BodySHA256     string    `json:"body_sha256"`
	BodySize       int64     `json:"body_size"`
	RunID          string    `json:"run_id"`
	CreatedAt      time.Time `json:"created_at"`
}

type HistorySecretReference struct {
	Project   string    `json:"project"`
	Target    string    `json:"target"`
	Name      string    `json:"name"`
	Provider  string    `json:"provider"`
	Reference string    `json:"reference"`
	CreatedAt time.Time `json:"created_at"`
}

type HistoryResourcePolicy struct {
	Project      string    `json:"project"`
	Target       string    `json:"target"`
	ProcessLimit int       `json:"process_limit"`
	Memory       string    `json:"memory"`
	CPUPercent   int       `json:"cpu_percent"`
	Status       string    `json:"status"`
	CreatedAt    time.Time `json:"created_at"`
}

type HistoryCounter struct {
	Key   string `json:"key"`
	Value uint64 `json:"value"`
}

// ExportProject reads all project-owned rows in one database transaction.
func (r *Repository) ExportProject(ctx context.Context, project string) (ProjectHistory, error) {
	if strings.TrimSpace(project) == "" {
		return ProjectHistory{}, errors.New("project is required")
	}
	result := ProjectHistory{Version: ProjectHistoryVersion, Project: project}
	err := r.db.WithContext(ctx).Transaction(func(tx *gorm.DB) error {
		var runs []runModel
		if err := tx.Where("project = ?", project).Order("id ASC").Find(&runs).Error; err != nil {
			return err
		}
		runRefs := make(map[int64]string, len(runs))
		runIDs := make([]string, 0, len(runs))
		runRowIDs := make([]int64, 0, len(runs))
		for _, run := range runs {
			ref := fmt.Sprintf("run-%d", run.ID)
			runRefs[run.ID] = ref
			runRowIDs = append(runRowIDs, run.ID)
			if run.RunID != "" {
				runIDs = append(runIDs, run.RunID)
			}
			result.Runs = append(result.Runs, HistoryRun{
				Ref: ref, RunID: compatibleRunID(run), Project: run.Project, Name: run.Name,
				TargetType: run.TargetType, Target: run.Target,
				Trigger: HistoryTrigger{Type: run.TriggerType, Name: run.TriggerName, Mode: run.TriggerMode, EventID: run.EventID},
				Status:  run.Status, Started: cloneTime(run.Started), Finished: cloneTime(run.Finished), ExitCode: run.ExitCode,
				Error: run.Error, Stderr: run.Stderr, StdoutPath: run.StdoutPath, StderrPath: run.StderrPath,
				IdempotencyKey: run.IdempotencyKey, ConfigurationGeneration: run.ConfigurationGeneration,
				RetriedFromRunID: stringValue(run.RetriedFromRunID), CreatedAt: run.CreatedAt, UpdatedAt: run.UpdatedAt,
			})
		}

		var tasks []taskModel
		if len(runRowIDs) > 0 {
			if err := tx.Where("run_row_id IN ?", runRowIDs).Order("id ASC").Find(&tasks).Error; err != nil {
				return err
			}
		}
		var attempts []attemptModel
		if len(runRowIDs) > 0 {
			if err := tx.Where("run_row_id IN ?", runRowIDs).Order("id ASC").Find(&attempts).Error; err != nil {
				return err
			}
		}
		attemptsByRun := make(map[int64][]HistoryAttempt, len(attempts))
		attemptsByTask := make(map[int64][]HistoryAttempt, len(attempts))
		for _, attempt := range attempts {
			converted := historyAttemptFromModel(attempt)
			if attempt.TaskRowID == 0 {
				attemptsByRun[attempt.RunRowID] = append(attemptsByRun[attempt.RunRowID], converted)
			} else {
				attemptsByTask[attempt.TaskRowID] = append(attemptsByTask[attempt.TaskRowID], converted)
			}
		}
		artifactsByTask := map[int64][]HistoryArtifact{}
		if tx.Migrator().HasTable(&artifactModel{}) && len(tasks) > 0 {
			var artifacts []artifactModel
			if err := tx.Where("task_row_id IN (SELECT id FROM history_tasks WHERE run_row_id IN ?)", runRowIDs).Order("id ASC").Find(&artifacts).Error; err != nil {
				return err
			}
			for _, artifact := range artifacts {
				artifactsByTask[artifact.TaskRowID] = append(artifactsByTask[artifact.TaskRowID], HistoryArtifact{
					Path: artifact.Path, Exists: artifact.Exists, Size: artifact.Size, SHA256: artifact.SHA256,
				})
			}
		}
		for index := range result.Runs {
			result.Runs[index].Attempts = append([]HistoryAttempt(nil), attemptsByRun[runRowIDs[index]]...)
		}
		runsByRef := make(map[string]*HistoryRun, len(result.Runs))
		for index := range result.Runs {
			runsByRef[result.Runs[index].Ref] = &result.Runs[index]
		}
		for _, task := range tasks {
			run := runsByRef[runRefs[task.RunRowID]]
			if run == nil {
				return fmt.Errorf("history task %d references unknown run", task.ID)
			}
			run.Tasks = append(run.Tasks, HistoryTask{
				Ref: fmt.Sprintf("task-%d", task.ID), RunRef: runRefs[task.RunRowID], RunID: task.TaskRunID,
				ParentRunID: task.ParentRunID, Node: task.Node, Task: task.Task, Command: task.Command,
				Args: decodeStrings(task.ArgsJSON), WorkingDir: task.WorkingDir, EnvKeys: decodeStrings(task.EnvKeysJSON),
				ArgsRedacted: task.ArgsRedacted, Status: task.Status, SkipReason: task.SkipReason,
				TimeoutNanos: cloneInt64(task.TimeoutNanos), RetryCount: cloneInt(task.RetryCount), RetryDelayNanos: cloneInt64(task.RetryDelayNanos),
				AllowFailure: cloneBool(task.AllowFailure), PolicyResolved: cloneBool(task.PolicyResolved),
				Started: cloneTime(task.Started), Finished: cloneTime(task.Finished), DurationSeconds: task.DurationSeconds,
				ExitCode: task.ExitCode, Error: task.Error, Stderr: task.Stderr, StdoutPath: task.StdoutPath, StderrPath: task.StderrPath,
				Attempts: append([]HistoryAttempt(nil), attemptsByTask[task.ID]...), Artifacts: append([]HistoryArtifact(nil), artifactsByTask[task.ID]...),
			})
		}

		if len(runIDs) > 0 {
			var executionEvents []eventModel
			if err := tx.Where("run_id IN ?", runIDs).Order("id ASC").Find(&executionEvents).Error; err != nil {
				return err
			}
			for _, event := range executionEvents {
				result.ExecutionEvents = append(result.ExecutionEvents, HistoryExecutionEvent{
					RunID: event.RunID, Type: event.Type, Status: event.Status, Details: event.Details, CreatedAt: event.CreatedAt,
				})
			}
		}

		var events []observabilityEventModel
		eventQuery := tx.Where("project = ?", project)
		if len(runIDs) > 0 {
			eventQuery = eventQuery.Or("run_id IN ?", runIDs)
		}
		if err := eventQuery.Order("id ASC").Find(&events).Error; err != nil {
			return err
		}
		for _, event := range events {
			value := observability.Event{
				Timestamp: event.Timestamp, Type: event.Type, Actor: event.Actor, Project: event.Project,
				Target: event.Target, RunID: event.RunID, OperationID: event.OperationID,
				ConfigurationGeneration: event.ConfigurationGeneration,
			}
			if event.Metadata != "" {
				if err := json.Unmarshal([]byte(event.Metadata), &value.Metadata); err != nil {
					return fmt.Errorf("decode event metadata: %w", err)
				}
			}
			result.Events = append(result.Events, value)
		}

		var occurrences []scheduleOccurrenceModel
		if err := tx.Where("project = ?", project).Order("id ASC").Find(&occurrences).Error; err != nil {
			return err
		}
		for _, occurrence := range occurrences {
			result.ScheduleOccurrences = append(result.ScheduleOccurrences, HistoryScheduleOccurrence{
				ID: occurrence.ID, Project: occurrence.Project, Schedule: occurrence.Schedule, ScheduledAt: occurrence.ScheduledAt,
				Status: occurrence.Status, RunID: occurrence.RunID, Misfire: occurrence.Misfire,
				CreatedAt: occurrence.CreatedAt, UpdatedAt: occurrence.UpdatedAt,
			})
		}

		webhookQuery := tx.Where("webhook_key = ? OR webhook_key LIKE ? ESCAPE '\\'", project, escapeLikePattern(project)+"/%")
		if len(runIDs) > 0 {
			webhookQuery = webhookQuery.Or("run_id IN ?", runIDs)
		}
		var deliveries []webhookDeliveryModel
		if err := webhookQuery.Order("id ASC").Find(&deliveries).Error; err != nil {
			return err
		}
		for _, delivery := range deliveries {
			result.WebhookDeliveries = append(result.WebhookDeliveries, HistoryWebhookDelivery{
				WebhookKey: delivery.WebhookKey, IdempotencyKey: delivery.IdempotencyKey,
				BodySHA256: delivery.BodySHA256, BodySize: delivery.BodySize, RunID: delivery.RunID, CreatedAt: delivery.CreatedAt,
			})
		}

		var refs []secretReferenceModel
		if err := tx.Where("project = ?", project).Order("id ASC").Find(&refs).Error; err != nil {
			return err
		}
		for _, ref := range refs {
			result.SecretReferences = append(result.SecretReferences, HistorySecretReference{
				Project: ref.Project, Target: ref.Target, Name: ref.Name, Provider: ref.Provider, Reference: ref.Reference, CreatedAt: ref.CreatedAt,
			})
		}
		var policies []resourcePolicyModel
		if err := tx.Where("project = ?", project).Order("id ASC").Find(&policies).Error; err != nil {
			return err
		}
		for _, policy := range policies {
			result.ResourcePolicies = append(result.ResourcePolicies, HistoryResourcePolicy{
				Project: policy.Project, Target: policy.Target, ProcessLimit: policy.ProcessLimit, Memory: policy.Memory,
				CPUPercent: policy.CPUPercent, Status: policy.Status, CreatedAt: policy.CreatedAt,
			})
		}
		var counters []counterModel
		if err := tx.Order("key ASC").Find(&counters).Error; err != nil {
			return err
		}
		for _, counter := range counters {
			parts := strings.SplitN(counter.Key, "|", 3)
			if len(parts) < 2 || parts[1] != project {
				continue
			}
			if counter.Value < 0 {
				return fmt.Errorf("history counter %q has negative value", counter.Key)
			}
			result.Counters = append(result.Counters, HistoryCounter{Key: counter.Key, Value: uint64(counter.Value)})
		}
		return nil
	})
	if err != nil {
		return ProjectHistory{}, err
	}
	return result, nil
}

// ImportProject restores a logical project history without invoking normal
// execution write paths, so imported timestamps, statuses, counters, and
// events remain unchanged.
func (r *Repository) ImportProject(ctx context.Context, data ProjectHistory, replace bool) error {
	return r.ImportProjects(ctx, []ProjectHistory{data}, replace)
}

// ImportProjects restores several projects in one database transaction.
func (r *Repository) ImportProjects(ctx context.Context, data []ProjectHistory, replace bool) error {
	if len(data) == 0 {
		return nil
	}
	for _, project := range data {
		if err := validateProjectHistory(project); err != nil {
			return err
		}
	}
	r.writeMu.Lock()
	defer r.writeMu.Unlock()
	return r.db.WithContext(ctx).Transaction(func(tx *gorm.DB) error {
		for _, project := range data {
			if replace {
				if err := deleteProjectRows(tx, project.Project); err != nil {
					return err
				}
			}
			if err := importProjectRows(tx, project); err != nil {
				return err
			}
		}
		return nil
	})
}

// ValidateProjectHistory checks a logical history payload without opening a
// database or changing state.
func ValidateProjectHistory(data ProjectHistory) error {
	return validateProjectHistory(data)
}

func validateProjectHistory(data ProjectHistory) error {
	if data.Version != ProjectHistoryVersion {
		return fmt.Errorf("unsupported project history version %d", data.Version)
	}
	if strings.TrimSpace(data.Project) == "" {
		return errors.New("project history requires project")
	}
	runRefs := make(map[string]bool, len(data.Runs))
	runIDs := make(map[string]bool, len(data.Runs))
	for _, run := range data.Runs {
		if run.Ref == "" || runRefs[run.Ref] {
			return fmt.Errorf("duplicate or empty history run ref %q", run.Ref)
		}
		if run.RunID == "" || runIDs[run.RunID] {
			return fmt.Errorf("duplicate or empty history run ID %q", run.RunID)
		}
		if run.Project != "" && run.Project != data.Project {
			return fmt.Errorf("history run %q belongs to project %q", run.Ref, run.Project)
		}
		runRefs[run.Ref] = true
		runIDs[run.RunID] = true
		taskRefs := make(map[string]bool, len(run.Tasks))
		for _, task := range run.Tasks {
			if task.Ref == "" || taskRefs[task.Ref] {
				return fmt.Errorf("duplicate or empty history task ref %q", task.Ref)
			}
			if task.RunRef != run.Ref {
				return fmt.Errorf("history task %q references run %q", task.Ref, task.RunRef)
			}
			taskRefs[task.Ref] = true
		}
	}
	seenCounters := map[string]bool{}
	for _, counter := range data.Counters {
		parts := strings.SplitN(counter.Key, "|", 3)
		if len(parts) < 2 || parts[1] != data.Project {
			return fmt.Errorf("history counter %q is not owned by project %q", counter.Key, data.Project)
		}
		if counter.Value > math.MaxInt64 || seenCounters[counter.Key] {
			return fmt.Errorf("invalid history counter %q", counter.Key)
		}
		seenCounters[counter.Key] = true
	}
	seenOccurrences := make(map[string]bool, len(data.ScheduleOccurrences))
	for _, occurrence := range data.ScheduleOccurrences {
		if occurrence.ID == "" || seenOccurrences[occurrence.ID] {
			return fmt.Errorf("duplicate or empty schedule occurrence ID %q", occurrence.ID)
		}
		if occurrence.Project != data.Project {
			return fmt.Errorf("schedule occurrence %q belongs to project %q", occurrence.ID, occurrence.Project)
		}
		seenOccurrences[occurrence.ID] = true
	}
	for _, ref := range data.SecretReferences {
		if ref.Project != data.Project {
			return fmt.Errorf("secret reference belongs to project %q", ref.Project)
		}
	}
	for _, policy := range data.ResourcePolicies {
		if policy.Project != data.Project {
			return fmt.Errorf("resource policy belongs to project %q", policy.Project)
		}
	}
	return nil
}

func importProjectRows(tx *gorm.DB, data ProjectHistory) error {
	runIDs := make(map[string]bool, len(data.Runs))
	runRows := make(map[string]int64, len(data.Runs))
	for _, source := range data.Runs {
		var existing runModel
		err := tx.Where("run_id = ?", source.RunID).First(&existing).Error
		if err == nil {
			return fmt.Errorf("history run ID %q already exists", source.RunID)
		}
		if !errors.Is(err, gorm.ErrRecordNotFound) {
			return err
		}
		if runIDs[source.RunID] {
			return fmt.Errorf("duplicate history run ID %q", source.RunID)
		}
		model := runModel{
			RunID: source.RunID, Project: data.Project, Name: source.Name, TargetType: source.TargetType, Target: source.Target,
			TriggerType: source.Trigger.Type, TriggerName: source.Trigger.Name, TriggerMode: source.Trigger.Mode, EventID: source.Trigger.EventID,
			Status: source.Status, Started: cloneTime(source.Started), Finished: cloneTime(source.Finished), ExitCode: source.ExitCode,
			Error: source.Error, Stderr: source.Stderr, StdoutPath: source.StdoutPath, StderrPath: source.StderrPath,
			IdempotencyKey: source.IdempotencyKey, ConfigurationGeneration: source.ConfigurationGeneration,
			RetriedFromRunID: stringPointer(source.RetriedFromRunID), CreatedAt: source.CreatedAt, UpdatedAt: source.UpdatedAt,
		}
		if err := tx.Create(&model).Error; err != nil {
			return err
		}
		runIDs[source.RunID] = true
		runRows[source.Ref] = model.ID
	}
	for _, source := range data.Runs {
		runRowID := runRows[source.Ref]
		if err := insertHistoryAttempts(tx, runRowID, 0, source.Attempts); err != nil {
			return err
		}
		for _, task := range source.Tasks {
			model := taskModel{
				RunRowID: runRowID, TaskRunID: task.RunID, ParentRunID: task.ParentRunID, Node: task.Node, Task: task.Task,
				Command: task.Command, ArgsJSON: encodeStrings(task.Args), WorkingDir: task.WorkingDir, EnvKeysJSON: encodeStrings(task.EnvKeys),
				ArgsRedacted: task.ArgsRedacted, Status: task.Status, SkipReason: task.SkipReason,
				TimeoutNanos: cloneInt64(task.TimeoutNanos), RetryCount: cloneInt(task.RetryCount), RetryDelayNanos: cloneInt64(task.RetryDelayNanos),
				AllowFailure: cloneBool(task.AllowFailure), PolicyResolved: cloneBool(task.PolicyResolved), Started: cloneTime(task.Started), Finished: cloneTime(task.Finished),
				DurationSeconds: task.DurationSeconds, ExitCode: task.ExitCode, Error: task.Error, Stderr: task.Stderr,
				StdoutPath: task.StdoutPath, StderrPath: task.StderrPath,
			}
			if err := tx.Create(&model).Error; err != nil {
				return err
			}
			if err := insertHistoryAttempts(tx, runRowID, model.ID, task.Attempts); err != nil {
				return err
			}
			for _, artifact := range task.Artifacts {
				if err := tx.Create(&artifactModel{TaskRowID: model.ID, Path: artifact.Path, Exists: artifact.Exists, Size: artifact.Size, SHA256: artifact.SHA256}).Error; err != nil {
					return err
				}
			}
		}
	}
	for _, event := range data.ExecutionEvents {
		if err := tx.Create(&eventModel{RunID: event.RunID, Type: event.Type, Status: event.Status, Details: event.Details, CreatedAt: event.CreatedAt}).Error; err != nil {
			return err
		}
	}
	for _, event := range data.Events {
		metadata := ""
		if event.Metadata != nil {
			encoded, err := json.Marshal(event.Metadata)
			if err != nil {
				return err
			}
			metadata = string(encoded)
		}
		if err := tx.Create(&observabilityEventModel{
			Timestamp: event.Timestamp, Type: event.Type, Actor: event.Actor, Project: event.Project, Target: event.Target,
			RunID: event.RunID, OperationID: event.OperationID, ConfigurationGeneration: event.ConfigurationGeneration, Metadata: metadata,
		}).Error; err != nil {
			return err
		}
	}
	for _, occurrence := range data.ScheduleOccurrences {
		if err := tx.Create(&scheduleOccurrenceModel{
			ID: occurrence.ID, Project: occurrence.Project, Schedule: occurrence.Schedule, ScheduledAt: occurrence.ScheduledAt,
			Status: occurrence.Status, RunID: occurrence.RunID, Misfire: occurrence.Misfire, CreatedAt: occurrence.CreatedAt, UpdatedAt: occurrence.UpdatedAt,
		}).Error; err != nil {
			return err
		}
	}
	for _, delivery := range data.WebhookDeliveries {
		if err := tx.Create(&webhookDeliveryModel{
			WebhookKey: delivery.WebhookKey, IdempotencyKey: delivery.IdempotencyKey, BodySHA256: delivery.BodySHA256,
			BodySize: delivery.BodySize, RunID: delivery.RunID, CreatedAt: delivery.CreatedAt,
		}).Error; err != nil {
			return err
		}
	}
	for _, ref := range data.SecretReferences {
		if err := tx.Create(&secretReferenceModel{Project: ref.Project, Target: ref.Target, Name: ref.Name, Provider: ref.Provider, Reference: ref.Reference, CreatedAt: ref.CreatedAt}).Error; err != nil {
			return err
		}
	}
	for _, policy := range data.ResourcePolicies {
		if err := tx.Create(&resourcePolicyModel{Project: policy.Project, Target: policy.Target, ProcessLimit: policy.ProcessLimit, Memory: policy.Memory, CPUPercent: policy.CPUPercent, Status: policy.Status, CreatedAt: policy.CreatedAt}).Error; err != nil {
			return err
		}
	}
	for _, counter := range data.Counters {
		if err := tx.Create(&counterModel{Key: counter.Key, Value: int64(counter.Value)}).Error; err != nil {
			return err
		}
	}
	return nil
}

func insertHistoryAttempts(tx *gorm.DB, runRowID, taskRowID int64, attempts []HistoryAttempt) error {
	for _, attempt := range attempts {
		if err := tx.Create(&attemptModel{
			RunRowID: runRowID, TaskRowID: taskRowID, Number: attempt.Number, Started: cloneTime(attempt.Started), Finished: cloneTime(attempt.Finished),
			DurationSeconds: attempt.DurationSeconds, ExitCode: attempt.ExitCode, Error: attempt.Error, Stderr: attempt.Stderr,
		}).Error; err != nil {
			return err
		}
	}
	return nil
}

func historyAttemptFromModel(value attemptModel) HistoryAttempt {
	return HistoryAttempt{
		Number: value.Number, Started: cloneTime(value.Started), Finished: cloneTime(value.Finished),
		DurationSeconds: value.DurationSeconds, ExitCode: value.ExitCode, Error: value.Error, Stderr: value.Stderr,
	}
}

func cloneTime(value *time.Time) *time.Time {
	if value == nil {
		return nil
	}
	result := *value
	return &result
}

func cloneInt64(value *int64) *int64 {
	if value == nil {
		return nil
	}
	result := *value
	return &result
}

func cloneInt(value *int) *int {
	if value == nil {
		return nil
	}
	result := *value
	return &result
}

func cloneBool(value *bool) *bool {
	if value == nil {
		return nil
	}
	result := *value
	return &result
}

func deleteProjectRows(tx *gorm.DB, project string) error {
	var runs []runModel
	if err := tx.Where("project = ?", project).Find(&runs).Error; err != nil {
		return err
	}
	runRowIDs := make([]int64, 0, len(runs))
	runIDs := make([]string, 0, len(runs))
	for _, run := range runs {
		runRowIDs = append(runRowIDs, run.ID)
		if run.RunID != "" {
			runIDs = append(runIDs, run.RunID)
		}
	}
	var taskRowIDs []int64
	if len(runRowIDs) > 0 {
		if err := tx.Model(&taskModel{}).Where("run_row_id IN ?", runRowIDs).Pluck("id", &taskRowIDs).Error; err != nil {
			return err
		}
	}
	if len(taskRowIDs) > 0 {
		if err := tx.Where("task_row_id IN ?", taskRowIDs).Delete(&artifactModel{}).Error; err != nil {
			return err
		}
		if err := tx.Where("task_row_id IN ?", taskRowIDs).Delete(&attemptModel{}).Error; err != nil {
			return err
		}
	}
	if len(runRowIDs) > 0 {
		if err := tx.Where("run_row_id IN ?", runRowIDs).Delete(&attemptModel{}).Error; err != nil {
			return err
		}
		if err := tx.Where("run_row_id IN ?", runRowIDs).Delete(&taskModel{}).Error; err != nil {
			return err
		}
		if err := tx.Where("id IN ?", runRowIDs).Delete(&runModel{}).Error; err != nil {
			return err
		}
	}
	if len(runIDs) > 0 {
		if err := tx.Where("run_id IN ?", runIDs).Delete(&eventModel{}).Error; err != nil {
			return err
		}
		if err := tx.Where("run_id IN ?", runIDs).Delete(&webhookDeliveryModel{}).Error; err != nil {
			return err
		}
	}
	if err := tx.Where("project = ?", project).Delete(&scheduleOccurrenceModel{}).Error; err != nil {
		return err
	}
	observabilityEvents := tx.Where("project = ?", project)
	if len(runIDs) > 0 {
		observabilityEvents = observabilityEvents.Or("run_id IN ?", runIDs)
	}
	if err := observabilityEvents.Delete(&observabilityEventModel{}).Error; err != nil {
		return err
	}
	if err := tx.Where("project = ?", project).Delete(&secretReferenceModel{}).Error; err != nil {
		return err
	}
	if err := tx.Where("project = ?", project).Delete(&resourcePolicyModel{}).Error; err != nil {
		return err
	}
	webhookDeliveries := tx.Where("webhook_key = ? OR webhook_key LIKE ? ESCAPE '\\'", project, escapeLikePattern(project)+"/%")
	if len(runIDs) > 0 {
		webhookDeliveries = webhookDeliveries.Or("run_id IN ?", runIDs)
	}
	if err := webhookDeliveries.Delete(&webhookDeliveryModel{}).Error; err != nil {
		return err
	}
	var counters []counterModel
	if err := tx.Find(&counters).Error; err != nil {
		return err
	}
	for _, counter := range counters {
		parts := strings.SplitN(counter.Key, "|", 3)
		if len(parts) < 2 || parts[1] != project {
			continue
		}
		if err := tx.Where("key = ?", counter.Key).Delete(&counterModel{}).Error; err != nil {
			return err
		}
	}
	return nil
}
