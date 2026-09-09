// Package workflow executes project-local tasks and workflow DAGs.
package workflow

import (
	"context"
	"errors"
	"fmt"
	"maps"
	"sort"
	"sync"
	"time"

	"github.com/kevin93203/mango/internal/config"
	"github.com/kevin93203/mango/internal/scheduler"
)

const (
	StatusIdle               = scheduler.StatusIdle
	StatusRunning            = scheduler.StatusRunning
	StatusSuccess            = scheduler.StatusSuccess
	StatusFailed             = scheduler.StatusFailed
	StatusSkipped            = scheduler.StatusSkipped
	StatusCancelled          = scheduler.StatusCancelled
	nodeStatusUpstreamFailed = "upstream_failed"
)

// Invocation describes the execution context of a task. It is also used by
// the daemon to choose an isolated execution log name.
type Invocation struct {
	Project     string
	Workflow    string
	Node        string
	RunID       string
	Trigger     scheduler.TriggerRef
	ParentRunID string
	// AttemptNumber is internal execution context used to isolate the
	// process output produced by each retry attempt.
	AttemptNumber int
}

type TaskRunner func(context.Context, config.EffectiveTask, Invocation) scheduler.ExecutionResult
type HistorySink func(scheduler.Record)
type ProgressSink func(scheduler.Record)

type TaskSnapshot struct {
	Task            config.EffectiveTask
	Status          string
	LastRun         *time.Time
	DurationSeconds *float64
}

type WorkflowSnapshot struct {
	Workflow        config.EffectiveWorkflow
	Status          string
	LastRun         *time.Time
	DurationSeconds *float64
}

type nodeState struct {
	status string
	record scheduler.TaskRecord
}

type Executor struct {
	mu sync.Mutex

	tasks     map[string]config.EffectiveTask
	workflows map[string]config.EffectiveWorkflow

	taskRunning    map[string]int
	taskWaiters    map[string]chan struct{}
	workflowActive map[string]int
	taskLatest     map[string]scheduler.TaskRecord
	wfLatest       map[string]scheduler.Record

	runner   TaskRunner
	sink     HistorySink
	progress ProgressSink
	runs     sync.WaitGroup
}

func New(runner TaskRunner, sink HistorySink) *Executor {
	return &Executor{
		tasks:          map[string]config.EffectiveTask{},
		workflows:      map[string]config.EffectiveWorkflow{},
		taskRunning:    map[string]int{},
		taskWaiters:    map[string]chan struct{}{},
		workflowActive: map[string]int{},
		taskLatest:     map[string]scheduler.TaskRecord{},
		wfLatest:       map[string]scheduler.Record{},
		runner:         runner,
		sink:           sink,
	}
}

// SetProgressSink installs a callback for active workflow snapshots. The
// callback is intentionally separate from the terminal history sink so active
// node state can be persisted without creating terminal history rows.
func (e *Executor) SetProgressSink(sink ProgressSink) {
	e.mu.Lock()
	e.progress = sink
	e.mu.Unlock()
}

// Apply replaces the definitions used by future runs. Existing runs retain
// the effective task definitions they already received.
func (e *Executor) Apply(tasks map[string]config.EffectiveTask, workflows map[string]config.EffectiveWorkflow) {
	e.mu.Lock()
	deferred := make(map[string]config.EffectiveTask, len(tasks))
	for key, task := range tasks {
		task.Args = append([]string(nil), task.Args...)
		task.Outputs = append([]string(nil), task.Outputs...)
		task.Env = maps.Clone(task.Env)
		task.DeclaredEnv = maps.Clone(task.DeclaredEnv)
		deferred[key] = task
	}
	e.tasks = deferred
	updated := make(map[string]config.EffectiveWorkflow, len(workflows))
	for key, wf := range workflows {
		copyWorkflow := wf
		copyWorkflow.Tasks = make(map[string]config.EffectiveWorkflowTask, len(wf.Tasks))
		for node, spec := range wf.Tasks {
			spec.Needs = append([]string(nil), spec.Needs...)
			copyWorkflow.Tasks[node] = spec
		}
		updated[key] = copyWorkflow
	}
	e.workflows = updated
	e.mu.Unlock()
}

func (e *Executor) HasTask(key string) bool {
	e.mu.Lock()
	defer e.mu.Unlock()
	_, ok := e.tasks[key]
	return ok
}

func (e *Executor) HasWorkflow(key string) bool {
	e.mu.Lock()
	defer e.mu.Unlock()
	_, ok := e.workflows[key]
	return ok
}

func (e *Executor) HasWorkflowNode(project, workflowName, node string) bool {
	e.mu.Lock()
	defer e.mu.Unlock()
	wf, ok := e.workflows[project+"/"+workflowName]
	if !ok {
		return false
	}
	_, ok = wf.Tasks[node]
	return ok
}

func (e *Executor) ListTasks(project string) []TaskSnapshot {
	e.mu.Lock()
	defer e.mu.Unlock()
	keys := make([]string, 0)
	for key := range e.tasks {
		if project == "" || len(key) > len(project) && key[:len(project)+1] == project+"/" {
			keys = append(keys, key)
		}
	}
	sort.Strings(keys)
	result := make([]TaskSnapshot, 0, len(keys))
	for _, key := range keys {
		task := e.tasks[key]
		item := TaskSnapshot{Task: task, Status: StatusIdle}
		if e.taskRunning[key] > 0 {
			item.Status = StatusRunning
		}
		if record, ok := e.taskLatest[key]; ok {
			if item.Status != StatusRunning {
				item.Status = record.Status
				item.LastRun = timePtr(record.Started)
				item.DurationSeconds = floatPtr(record.DurationSeconds)
			}
		}
		result = append(result, item)
	}
	return result
}

func (e *Executor) ListWorkflows(project string) []WorkflowSnapshot {
	e.mu.Lock()
	defer e.mu.Unlock()
	keys := make([]string, 0)
	for key := range e.workflows {
		if project == "" || len(key) > len(project) && key[:len(project)+1] == project+"/" {
			keys = append(keys, key)
		}
	}
	sort.Strings(keys)
	result := make([]WorkflowSnapshot, 0, len(keys))
	for _, key := range keys {
		wf := e.workflows[key]
		item := WorkflowSnapshot{Workflow: wf, Status: StatusIdle}
		if e.workflowActive[key] > 0 {
			item.Status = StatusRunning
		}
		if record, ok := e.wfLatest[key]; ok && item.Status != StatusRunning {
			item.Status = record.Status
			item.LastRun = timePtr(record.Started)
			item.DurationSeconds = recordDuration(record)
		}
		result = append(result, item)
	}
	return result
}

// Run executes a workflow synchronously. The returned record is not persisted
// by the executor; the caller decides whether it belongs to a cron run or a
// manual run and sends it to the common history sink.
func (e *Executor) Run(ctx context.Context, project, workflowName string, trigger scheduler.TriggerRef) scheduler.ExecutionResult {
	return e.RunWithID(ctx, project, workflowName, trigger, scheduler.NewRunID())
}

// RunWithID executes a workflow using the caller-provided logical run ID.
// This lets the daemon persist a queued execution before any process starts.
func (e *Executor) RunWithID(ctx context.Context, project, workflowName string, trigger scheduler.TriggerRef, runID string) scheduler.ExecutionResult {
	key := project + "/" + workflowName
	if runID == "" {
		runID = scheduler.NewRunID()
	}
	e.mu.Lock()
	wf, ok := e.workflows[key]
	if ok {
		if wf.Concurrency == "forbid" && e.workflowActive[key] > 0 {
			e.mu.Unlock()
			now := time.Now()
			record := scheduler.Record{RunID: runID, Project: project, Name: workflowName, TargetType: "workflow", Target: workflowName, Trigger: trigger, Status: StatusSkipped, Started: now, Finished: now}
			return scheduler.ExecutionResult{ExitCode: 0, Record: &record}
		}
		e.workflowActive[key]++
	}
	e.mu.Unlock()
	if !ok {
		return failedExecution(fmt.Errorf("workflow %s not found", key))
	}

	record := e.executeWorkflow(ctx, wf, runID, trigger)
	e.mu.Lock()
	e.workflowActive[key]--
	e.wfLatest[key] = record
	e.mu.Unlock()
	return scheduler.ExecutionResult{ExitCode: record.ExitCode, Err: errorFromRecord(record), Stderr: record.Stderr, Record: &record}
}

// RunTask executes one direct task synchronously.
func (e *Executor) RunTask(ctx context.Context, project, taskName string, trigger scheduler.TriggerRef) scheduler.ExecutionResult {
	return e.RunTaskWithID(ctx, project, taskName, trigger, scheduler.NewRunID())
}

// RunTaskWithID executes a direct task using the caller-provided logical run
// ID. The task record still receives its own child run ID.
func (e *Executor) RunTaskWithID(ctx context.Context, project, taskName string, trigger scheduler.TriggerRef, rootRunID string) scheduler.ExecutionResult {
	key := project + "/" + taskName
	e.mu.Lock()
	task, ok := e.tasks[key]
	e.mu.Unlock()
	if !ok {
		return failedExecution(fmt.Errorf("task %s not found", key))
	}
	if rootRunID == "" {
		rootRunID = scheduler.NewRunID()
	}
	taskRunID := scheduler.NewRunID()
	result, attempts := e.executeTask(ctx, task, Invocation{Project: project, Node: taskName, RunID: taskRunID, Trigger: trigger, ParentRunID: rootRunID})
	now := time.Now()
	started := now
	if len(attempts) > 0 {
		started = attempts[0].Started
	}
	taskRecord := makeTaskRecord(taskName, task, taskRunID, rootRunID, started, now, result, attempts, taskStatus(ctx, result), false)
	e.setTaskLatest(key, taskRecord)
	record := scheduler.Record{
		RunID: rootRunID, Project: project, Name: taskName, TargetType: "task", Target: taskName, Trigger: trigger,
		Status: taskRecord.Status, Started: started, Finished: now, ExitCode: taskRecord.ExitCode,
		Error: taskRecord.Error, Stderr: taskRecord.Stderr, StdoutPath: result.StdoutPath, StderrPath: result.StderrPath,
		Attempts: attempts, Tasks: []scheduler.TaskRecord{taskRecord},
	}
	return scheduler.ExecutionResult{ExitCode: record.ExitCode, Err: errorFromRecord(record), Stderr: record.Stderr, Record: &record}
}

func (e *Executor) RunNowWorkflow(ctx context.Context, project, workflowName string, trigger scheduler.TriggerRef) error {
	return e.RunNowWorkflowWithID(ctx, project, workflowName, trigger, scheduler.NewRunID())
}

func (e *Executor) RunNowWorkflowWithID(ctx context.Context, project, workflowName string, trigger scheduler.TriggerRef, runID string) error {
	if !e.HasWorkflow(project + "/" + workflowName) {
		return fmt.Errorf("workflow %s/%s not found", project, workflowName)
	}
	e.runs.Add(1)
	go func() {
		defer e.runs.Done()
		result := e.RunWithID(ctx, project, workflowName, trigger, runID)
		if result.Record != nil && e.sink != nil {
			e.sink(*result.Record)
		}
	}()
	return nil
}

func (e *Executor) RunNowTask(ctx context.Context, project, taskName string, trigger scheduler.TriggerRef) error {
	return e.RunNowTaskWithID(ctx, project, taskName, trigger, scheduler.NewRunID())
}

func (e *Executor) RunNowTaskWithID(ctx context.Context, project, taskName string, trigger scheduler.TriggerRef, runID string) error {
	if !e.HasTask(project + "/" + taskName) {
		return fmt.Errorf("task %s/%s not found", project, taskName)
	}
	e.runs.Add(1)
	go func() {
		defer e.runs.Done()
		result := e.RunTaskWithID(ctx, project, taskName, trigger, runID)
		if result.Record != nil && e.sink != nil {
			e.sink(*result.Record)
		}
	}()
	return nil
}

func (e *Executor) Wait() { e.runs.Wait() }

func (e *Executor) executeWorkflow(ctx context.Context, wf config.EffectiveWorkflow, runID string, trigger scheduler.TriggerRef) scheduler.Record {
	started := time.Now()
	type nodeResult struct {
		node   string
		record scheduler.TaskRecord
	}

	nodes := make(map[string]nodeState, len(wf.Tasks))
	for node := range wf.Tasks {
		nodes[node] = nodeState{status: "pending"}
	}
	ordered := sortedWorkflowNodes(wf)
	results := make(chan nodeResult, len(nodes))
	active := 0

	for {
		progress := false
		for _, node := range ordered {
			state := nodes[node]
			if state.status != "pending" {
				continue
			}
			if ctx.Err() != nil {
				state.status = StatusCancelled
				state.record = cancelledTaskRecord(node, wf.Tasks[node].Uses, runID)
				nodes[node] = state
				e.emitProgress(workflowProgressRecord(wf, runID, trigger, started, nodes, StatusRunning))
				progress = true
				continue
			}
			ready, blocked, upstreamFailed := nodeReadiness(node, wf, nodes)
			if blocked {
				if upstreamFailed {
					state.status = nodeStatusUpstreamFailed
					state.record = upstreamFailedTaskRecord(node, wf.Tasks[node].Uses, runID)
				} else {
					state.status = StatusSkipped
					state.record = skippedTaskRecord(node, wf.Tasks[node].Uses, runID)
				}
				nodes[node] = state
				e.emitProgress(workflowProgressRecord(wf, runID, trigger, started, nodes, StatusRunning))
				progress = true
				continue
			}
			if !ready {
				continue
			}
			nodeSpec := wf.Tasks[node]
			task, ok := e.taskFor(wf.Project, nodeSpec.Uses)
			if !ok {
				state.status = StatusFailed
				state.record = failedTaskRecord(node, nodeSpec.Uses, runID, fmt.Errorf("task %s/%s not found", wf.Project, nodeSpec.Uses))
				nodes[node] = state
				e.emitProgress(workflowProgressRecord(wf, runID, trigger, started, nodes, StatusRunning))
				progress = true
				continue
			}
			task = applyNodePolicy(task, nodeSpec)
			state.status = StatusRunning
			taskRunID := scheduler.NewRunID()
			state.record = runningTaskRecord(node, task, taskRunID, runID, time.Now(), nodeSpec.AllowFailure)
			nodes[node] = state
			e.emitProgress(workflowProgressRecord(wf, runID, trigger, started, nodes, StatusRunning))
			active++
			progress = true
			go func(node string, task config.EffectiveTask, taskRunID string, allowFailure bool) {
				result, attempts := e.executeTask(ctx, task, Invocation{Project: wf.Project, Workflow: wf.Name, Node: node, RunID: taskRunID, Trigger: trigger, ParentRunID: runID})
				now := time.Now()
				begin := now
				if len(attempts) > 0 {
					begin = attempts[0].Started
				}
				taskRecord := makeTaskRecord(node, task, taskRunID, runID, begin, now, result, attempts, taskStatus(ctx, result), allowFailure)
				e.setTaskLatest(wf.Project+"/"+task.Name, taskRecord)
				results <- nodeResult{node: node, record: taskRecord}
			}(node, task, taskRunID, nodeSpec.AllowFailure)
		}

		if active == 0 {
			pending := false
			for _, node := range ordered {
				if nodes[node].status == "pending" {
					pending = true
					break
				}
			}
			if !pending || !progress {
				break
			}
			continue
		}

		result := <-results
		state := nodes[result.node]
		state.status = result.record.Status
		state.record = result.record
		nodes[result.node] = state
		e.emitProgress(workflowProgressRecord(wf, runID, trigger, started, nodes, StatusRunning))
		active--
	}

	records := make([]scheduler.TaskRecord, 0, len(ordered))
	status := StatusSuccess
	exitCode := 0
	errorText := ""
	for _, node := range ordered {
		state := nodes[node]
		if state.status == "pending" {
			state.status = StatusCancelled
			state.record = cancelledTaskRecord(node, wf.Tasks[node].Uses, runID)
		}
		records = append(records, state.record)
		switch state.status {
		case StatusCancelled:
			status = StatusCancelled
			if exitCode == 0 {
				exitCode = state.record.ExitCode
			}
			if errorText == "" {
				errorText = state.record.Error
			}
		case StatusFailed:
			if wf.Tasks[node].AllowFailure {
				continue
			}
			if status != StatusCancelled {
				status = StatusFailed
			}
			if exitCode == 0 {
				exitCode = state.record.ExitCode
			}
			if errorText == "" {
				errorText = state.record.Error
			}
		}
	}
	if status == StatusSuccess && ctx.Err() != nil {
		status = StatusCancelled
		exitCode = 130
	}
	finished := time.Now()
	return scheduler.Record{
		RunID: runID, Project: wf.Project, Name: wf.Name, TargetType: "workflow", Target: wf.Name, Trigger: trigger,
		Status: status, Started: started, Finished: finished, ExitCode: exitCode, Error: errorText, Tasks: records,
	}
}

func (e *Executor) executeTask(ctx context.Context, task config.EffectiveTask, invocation Invocation) (scheduler.ExecutionResult, []scheduler.Attempt) {
	retries := task.RetryCount
	if retries < 0 {
		retries = 0
	}
	attempts := make([]scheduler.Attempt, 0, retries+1)
	var result scheduler.ExecutionResult
	for number := 1; ; number++ {
		if number > 1 && !waitForRetry(ctx, task.RetryDelay) {
			return result, attempts
		}
		if !e.acquireTask(ctx, task.Project+"/"+task.Name, task.Concurrency == "allow") {
			return scheduler.ExecutionResult{ExitCode: 130, Err: ctx.Err()}, attempts
		}
		attemptStarted := time.Now()
		attemptInvocation := invocation
		attemptInvocation.AttemptNumber = number
		result = e.runner(ctx, task, attemptInvocation)
		attemptFinished := time.Now()
		e.releaseTask(task.Project + "/" + task.Name)
		attempt := scheduler.Attempt{Number: number, Started: attemptStarted, Finished: attemptFinished, DurationSeconds: nonNegativeSeconds(attemptFinished.Sub(attemptStarted)), ExitCode: result.ExitCode, Stderr: result.Stderr}
		if result.Err != nil {
			attempt.Error = result.Err.Error()
		}
		attempts = append(attempts, attempt)
		if result.Err == nil && result.ExitCode == 0 {
			return result, attempts
		}
		if ctx.Err() != nil || number > retries {
			return result, attempts
		}
	}
}

func (e *Executor) acquireTask(ctx context.Context, key string, allow bool) bool {
	for {
		e.mu.Lock()
		if allow || e.taskRunning[key] == 0 {
			e.taskRunning[key]++
			e.mu.Unlock()
			return true
		}
		waiter := e.taskWaiters[key]
		if waiter == nil {
			waiter = make(chan struct{})
			e.taskWaiters[key] = waiter
		}
		e.mu.Unlock()
		select {
		case <-ctx.Done():
			return false
		case <-waiter:
		}
	}
}

func (e *Executor) releaseTask(key string) {
	e.mu.Lock()
	if e.taskRunning[key] > 0 {
		e.taskRunning[key]--
	}
	if waiter := e.taskWaiters[key]; waiter != nil {
		delete(e.taskWaiters, key)
		close(waiter)
	}
	e.mu.Unlock()
}

func (e *Executor) taskFor(project, name string) (config.EffectiveTask, bool) {
	e.mu.Lock()
	defer e.mu.Unlock()
	task, ok := e.tasks[project+"/"+name]
	return task, ok
}

func (e *Executor) emitProgress(record scheduler.Record) {
	e.mu.Lock()
	sink := e.progress
	e.mu.Unlock()
	if sink != nil {
		sink(record)
	}
}

func (e *Executor) setTaskLatest(key string, record scheduler.TaskRecord) {
	e.mu.Lock()
	e.taskLatest[key] = record
	e.mu.Unlock()
}

func nodeReadiness(node string, wf config.EffectiveWorkflow, states map[string]nodeState) (ready, blocked, upstreamFailed bool) {
	for _, dependency := range wf.Tasks[node].Needs {
		state := states[dependency].status
		switch state {
		case StatusFailed:
			if wf.Tasks[dependency].AllowFailure {
				continue
			}
			return false, true, true
		case nodeStatusUpstreamFailed:
			return false, true, true
		case StatusSkipped, StatusCancelled:
			return false, true, false
		case "pending", StatusRunning:
			return false, false, false
		}
	}
	return true, false, false
}

func applyNodePolicy(task config.EffectiveTask, node config.EffectiveWorkflowTask) config.EffectiveTask {
	if !node.PolicyResolved {
		return task
	}
	task.Timeout = node.Timeout
	task.RetryCount = node.RetryCount
	task.RetryDelay = node.RetryDelay
	return task
}

func sortedWorkflowNodes(wf config.EffectiveWorkflow) []string {
	result := make([]string, 0, len(wf.Tasks))
	for node := range wf.Tasks {
		result = append(result, node)
	}
	sort.Strings(result)
	return result
}

func makeTaskRecord(node string, task config.EffectiveTask, taskRunID, parentRunID string, started, finished time.Time, result scheduler.ExecutionResult, attempts []scheduler.Attempt, status string, allowFailure bool) scheduler.TaskRecord {
	metadata := scheduler.SafeTaskMetadata(task.Command, task.Args, task.WorkingDir, task.DeclaredEnv)
	record := scheduler.TaskRecord{
		RunID: taskRunID, ParentRunID: parentRunID,
		Node: node, Task: task.Name, Command: metadata.Command, Args: metadata.Args,
		WorkingDir: metadata.WorkingDir, EnvKeys: metadata.EnvKeys, ArgsRedacted: metadata.ArgsRedacted,
		Status: status, Started: started, Finished: finished,
		Timeout: task.Timeout, RetryCount: task.RetryCount, RetryDelay: task.RetryDelay,
		AllowFailure: allowFailure, PolicyResolved: true,
		DurationSeconds: nonNegativeSeconds(finished.Sub(started)), ExitCode: result.ExitCode,
		Stderr: result.Stderr, StdoutPath: result.StdoutPath, StderrPath: result.StderrPath, Attempts: attempts,
		Artifacts: append([]scheduler.Artifact(nil), result.Artifacts...),
	}
	if result.Err != nil {
		record.Error = result.Err.Error()
	}
	return record
}

func runningTaskRecord(node string, task config.EffectiveTask, taskRunID, parentRunID string, started time.Time, allowFailure bool) scheduler.TaskRecord {
	metadata := scheduler.SafeTaskMetadata(task.Command, task.Args, task.WorkingDir, task.DeclaredEnv)
	return scheduler.TaskRecord{
		RunID: taskRunID, ParentRunID: parentRunID, Node: node, Task: task.Name,
		Command: metadata.Command, Args: metadata.Args, WorkingDir: metadata.WorkingDir,
		EnvKeys: metadata.EnvKeys, ArgsRedacted: metadata.ArgsRedacted,
		Status: StatusRunning, Started: started,
		Timeout: task.Timeout, RetryCount: task.RetryCount, RetryDelay: task.RetryDelay,
		AllowFailure: allowFailure, PolicyResolved: true,
	}
}

func workflowProgressRecord(wf config.EffectiveWorkflow, runID string, trigger scheduler.TriggerRef, started time.Time, nodes map[string]nodeState, status string) scheduler.Record {
	ordered := sortedWorkflowNodes(wf)
	record := scheduler.Record{
		RunID: runID, Project: wf.Project, Name: wf.Name, TargetType: "workflow", Target: wf.Name,
		Trigger: trigger, Status: status, Started: started, Tasks: make([]scheduler.TaskRecord, 0, len(ordered)),
	}
	for _, node := range ordered {
		if nodes[node].record.RunID != "" {
			record.Tasks = append(record.Tasks, nodes[node].record)
		}
	}
	return record
}

func taskStatus(ctx context.Context, result scheduler.ExecutionResult) string {
	if ctx.Err() != nil {
		return StatusCancelled
	}
	if result.Err != nil || result.ExitCode != 0 {
		return StatusFailed
	}
	return StatusSuccess
}

func skippedTaskRecord(node, task, parentRunID string) scheduler.TaskRecord {
	now := time.Now()
	return scheduler.TaskRecord{RunID: scheduler.NewRunID(), ParentRunID: parentRunID, Node: node, Task: task, Status: StatusSkipped, Started: now, Finished: now, ExitCode: 0}
}

func upstreamFailedTaskRecord(node, task, parentRunID string) scheduler.TaskRecord {
	now := time.Now()
	return scheduler.TaskRecord{RunID: scheduler.NewRunID(), ParentRunID: parentRunID, Node: node, Task: task,
		Status: StatusSkipped, SkipReason: nodeStatusUpstreamFailed, Started: now, Finished: now, ExitCode: 0,
		Error: "upstream node failed"}
}

func cancelledTaskRecord(node, task, parentRunID string) scheduler.TaskRecord {
	now := time.Now()
	return scheduler.TaskRecord{RunID: scheduler.NewRunID(), ParentRunID: parentRunID, Node: node, Task: task, Status: StatusCancelled, Started: now, Finished: now, ExitCode: 130, Error: context.Canceled.Error()}
}

func failedTaskRecord(node, task, parentRunID string, err error) scheduler.TaskRecord {
	now := time.Now()
	return scheduler.TaskRecord{RunID: scheduler.NewRunID(), ParentRunID: parentRunID, Node: node, Task: task, Status: StatusFailed, Started: now, Finished: now, ExitCode: 1, Error: err.Error()}
}

func failedExecution(err error) scheduler.ExecutionResult {
	return scheduler.ExecutionResult{ExitCode: 1, Err: err}
}

func errorFromRecord(record scheduler.Record) error {
	if record.Error != "" {
		return errors.New(record.Error)
	}
	if record.ExitCode != 0 {
		return fmt.Errorf("execution exited with code %d", record.ExitCode)
	}
	return nil
}

func waitForRetry(ctx context.Context, delay time.Duration) bool {
	if delay <= 0 {
		select {
		case <-ctx.Done():
			return false
		default:
			return true
		}
	}
	timer := time.NewTimer(delay)
	defer timer.Stop()
	select {
	case <-timer.C:
		return true
	case <-ctx.Done():
		return false
	}
}

func nonNegativeSeconds(value time.Duration) float64 {
	if value < 0 {
		return 0
	}
	return value.Seconds()
}

func timePtr(value time.Time) *time.Time { return &value }
func floatPtr(value float64) *float64    { return &value }

func recordDuration(record scheduler.Record) *float64 {
	if record.Started.IsZero() || record.Finished.IsZero() {
		return nil
	}
	return floatPtr(nonNegativeSeconds(record.Finished.Sub(record.Started)))
}
