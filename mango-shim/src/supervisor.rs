use crate::platform;
use crate::protocol::{
    HelloParams, HelloResponse, PROTOCOL_VERSION, Request, Response, StopParams, failure, success,
};
use crate::state::{
    Bootstrap, ExitRecord, StatePaths, Status, load_status, now_string, write_json,
};
use serde_json::Value;
use std::fs::{self, File, OpenOptions};
use std::io;
use std::path::Path;
use std::process::{Child, ExitStatus};
use std::sync::{
    Arc, Mutex,
    atomic::{AtomicBool, Ordering},
};
use std::thread;
use std::time::{Duration, Instant, SystemTime};

const STATE_RUNNING: &str = "running";
const STATE_STOPPED: &str = "stopped";
const STATE_STARTING: &str = "starting";
const STATE_STOPPING: &str = "stopping";
const STATE_EXITED: &str = "exited";
const STATE_BACKING_OFF: &str = "backing_off";
const STATE_CRASH_LOOP: &str = "crash_loop";
const STATE_ORPHANED: &str = "orphaned";

struct Runtime {
    status: Status,
    child: Option<Child>,
    tree: Option<platform::TreeHandle>,
    child_started: Option<Instant>,
    failures: Vec<Instant>,
}

pub struct Supervisor {
    paths: StatePaths,
    bootstrap: Bootstrap,
    _lock: File,
    runtime: Mutex<Runtime>,
    stop_server: Arc<AtomicBool>,
}

impl Supervisor {
    pub fn new(paths: StatePaths, bootstrap: Bootstrap) -> io::Result<Arc<Self>> {
        platform::prepare_subreaper()?;
        fs::create_dir_all(&paths.dir)?;
        set_private_directory(&paths.dir)?;
        let lock = crate::state::open_lock(&paths.lock)?;

        let previous = load_status(&paths)?;
        let mut status =
            previous.unwrap_or_else(|| Status::initial(&bootstrap, std::process::id()));
        status.schema_version = crate::state::STATE_SCHEMA_VERSION;
        status.protocol_version = bootstrap.protocol_version;
        status.service_key = bootstrap.service_key.clone();
        status.instance_id = bootstrap.instance_id.clone();
        status.incarnation = bootstrap.incarnation.clone();
        status.config_fingerprint = bootstrap.config_fingerprint.clone();
        status.shim_pid = std::process::id();
        status.stdout_path = bootstrap.stdout_path.clone();
        status.stderr_path = bootstrap.stderr_path.clone();

        let orphaned = matches!(
            status.state.as_str(),
            STATE_RUNNING | STATE_STARTING | STATE_STOPPING | STATE_BACKING_OFF
        ) && status
            .service_pid
            .map(|pid| platform::process_is_alive(pid, status.process_start_token.as_deref()))
            .unwrap_or(false);
        if orphaned {
            status.state = STATE_ORPHANED.to_string();
            status.last_error = Some(
                "previous shim instance is gone while its service process is still alive"
                    .to_string(),
            );
            status.next_restart_at = None;
        } else {
            status.service_pid = None;
            status.process_start_token = None;
            status.started_at = None;
            if status.state == STATE_ORPHANED || status.desired_state == "running" {
                status.state = STATE_STOPPED.to_string();
            }
        }
        status.updated_at = now_string();
        let supervisor = Arc::new(Self {
            paths,
            bootstrap,
            _lock: lock,
            runtime: Mutex::new(Runtime {
                status,
                child: None,
                tree: None,
                child_started: None,
                failures: Vec::new(),
            }),
            stop_server: Arc::new(AtomicBool::new(false)),
        });
        supervisor.persist_status()?;
        Ok(supervisor)
    }

    pub fn start_monitor(self: &Arc<Self>) {
        let supervisor = Arc::clone(self);
        thread::spawn(move || supervisor.monitor_loop());
    }

    pub fn stop_flag(&self) -> Arc<AtomicBool> {
        Arc::clone(&self.stop_server)
    }

    pub fn paths(&self) -> &StatePaths {
        &self.paths
    }

    pub fn handle(&self, request: &Request) -> Response<Value> {
        match request.method.as_str() {
            "hello" => self.hello(request),
            "status" => self.status(request),
            "start" => self.start(request),
            "stop" => self.stop(request, false),
            "restart" => self.restart(request),
            "shutdown" => self.stop(request, true),
            method => failure::<Value>(
                request,
                "BAD_REQUEST",
                format!("unsupported method {method:?}"),
            ),
        }
    }

    pub fn finish(&self) -> io::Result<()> {
        self.stop_server.store(true, Ordering::Release);
        let mut runtime = self.runtime.lock().map_err(lock_error)?;
        if runtime.child.is_some() {
            stop_runtime(
                &mut runtime,
                &self.bootstrap,
                Duration::from_millis(self.bootstrap.stop_timeout_ms),
            );
        }
        if let Some(exit) = runtime.status.last_exit.as_ref() {
            let _ = write_json(&self.paths.exit, exit);
        }
        runtime.status.shim_pid = 0;
        runtime.status.updated_at = now_string();
        write_json(&self.paths.status, &runtime.status)?;
        let _ = fs::remove_file(&self.paths.endpoint);
        let _ = fs::remove_file(&self.paths.shim_pid);
        Ok(())
    }

    fn hello(&self, request: &Request) -> Response<Value> {
        let params =
            serde_json::from_value::<HelloParams>(request.params.clone()).unwrap_or(HelloParams {
                instance_id: None,
                service_key: None,
                config_fingerprint: None,
            });
        let runtime = match self.runtime.lock() {
            Ok(value) => value,
            Err(error) => return failure(request, "INTERNAL", error.to_string()),
        };
        if let Some(expected) = params.instance_id.as_deref()
            && expected != runtime.status.instance_id
        {
            return failure(request, "INSTANCE_MISMATCH", "instance id does not match");
        }
        if let Some(expected) = params.service_key.as_deref()
            && expected != runtime.status.service_key
        {
            return failure(request, "INSTANCE_MISMATCH", "service key does not match");
        }
        if let Some(expected) = params.config_fingerprint.as_deref()
            && expected != runtime.status.config_fingerprint
        {
            return failure(
                request,
                "CONFIG_MISMATCH",
                "config fingerprint does not match",
            );
        }
        let data = HelloResponse {
            protocol_version: PROTOCOL_VERSION,
            capabilities: vec!["status", "start", "stop", "restart", "shutdown"],
            instance_id: runtime.status.instance_id.clone(),
            service_key: runtime.status.service_key.clone(),
            config_fingerprint: runtime.status.config_fingerprint.clone(),
        };
        match serde_json::to_value(data) {
            Ok(value) => success(request, value),
            Err(error) => failure(request, "INTERNAL", error.to_string()),
        }
    }

    fn status(&self, request: &Request) -> Response<Value> {
        let runtime = match self.runtime.lock() {
            Ok(value) => value,
            Err(error) => return failure(request, "INTERNAL", error.to_string()),
        };
        match serde_json::to_value(&runtime.status) {
            Ok(value) => success(request, value),
            Err(error) => failure(request, "INTERNAL", error.to_string()),
        }
    }

    fn start(&self, request: &Request) -> Response<Value> {
        let mut runtime = match self.runtime.lock() {
            Ok(value) => value,
            Err(error) => return failure(request, "INTERNAL", error.to_string()),
        };
        if runtime.status.state == STATE_ORPHANED {
            return failure(
                request,
                "ORPHANED",
                runtime
                    .status
                    .last_error
                    .clone()
                    .unwrap_or_else(|| "service process is orphaned".to_string()),
            );
        }
        runtime.status.desired_state = "running".to_string();
        runtime.status.last_error = None;
        if runtime.child.is_none()
            && let Err(error) = spawn_runtime(&mut runtime, &self.paths, &self.bootstrap)
        {
            runtime.status.state = "failed".to_string();
            runtime.status.last_error = Some(error.to_string());
            let _ = self.persist_status_locked(&runtime);
            return failure(request, "START_FAILED", error.to_string());
        }
        let _ = self.persist_status_locked(&runtime);
        self.response_status(request, &runtime.status)
    }

    fn stop(&self, request: &Request, shutdown: bool) -> Response<Value> {
        let params =
            serde_json::from_value::<StopParams>(request.params.clone()).unwrap_or(StopParams {
                timeout_ms: self.bootstrap.stop_timeout_ms,
            });
        let mut runtime = match self.runtime.lock() {
            Ok(value) => value,
            Err(error) => return failure(request, "INTERNAL", error.to_string()),
        };
        if runtime.status.state == STATE_ORPHANED && runtime.child.is_none() {
            if shutdown {
                self.stop_server.store(true, Ordering::Release);
            }
            return failure(
                request,
                "ORPHANED",
                runtime
                    .status
                    .last_error
                    .clone()
                    .unwrap_or_else(|| "service process is orphaned".to_string()),
            );
        }
        runtime.status.desired_state = "stopped".to_string();
        runtime.status.state = STATE_STOPPING.to_string();
        runtime.status.updated_at = now_string();
        let _ = self.persist_status_locked(&runtime);
        stop_runtime(
            &mut runtime,
            &self.bootstrap,
            Duration::from_millis(params.timeout_ms.max(1)),
        );
        if let Some(exit) = runtime.status.last_exit.as_ref() {
            let _ = write_json(&self.paths.exit, exit);
        }
        if shutdown {
            self.stop_server.store(true, Ordering::Release);
        }
        let _ = self.persist_status_locked(&runtime);
        self.response_status(request, &runtime.status)
    }

    fn restart(&self, request: &Request) -> Response<Value> {
        let mut runtime = match self.runtime.lock() {
            Ok(value) => value,
            Err(error) => return failure(request, "INTERNAL", error.to_string()),
        };
        if runtime.status.state == STATE_ORPHANED && runtime.child.is_none() {
            return failure(
                request,
                "ORPHANED",
                runtime
                    .status
                    .last_error
                    .clone()
                    .unwrap_or_else(|| "service process is orphaned".to_string()),
            );
        }
        runtime.status.desired_state = "running".to_string();
        runtime.status.state = STATE_STOPPING.to_string();
        stop_runtime(
            &mut runtime,
            &self.bootstrap,
            Duration::from_millis(self.bootstrap.stop_timeout_ms),
        );
        if let Some(exit) = runtime.status.last_exit.as_ref() {
            let _ = write_json(&self.paths.exit, exit);
        }
        runtime.status.desired_state = "running".to_string();
        if let Err(error) = spawn_runtime(&mut runtime, &self.paths, &self.bootstrap) {
            runtime.status.state = "failed".to_string();
            runtime.status.last_error = Some(error.to_string());
            let _ = self.persist_status_locked(&runtime);
            return failure(request, "START_FAILED", error.to_string());
        }
        let _ = self.persist_status_locked(&runtime);
        self.response_status(request, &runtime.status)
    }

    fn response_status(&self, request: &Request, status: &Status) -> Response<Value> {
        match serde_json::to_value(status) {
            Ok(value) => success(request, value),
            Err(error) => failure(request, "INTERNAL", error.to_string()),
        }
    }

    fn monitor_loop(self: Arc<Self>) {
        loop {
            if self.stop_server.load(Ordering::Acquire) {
                break;
            }
            let mut runtime = match self.runtime.lock() {
                Ok(value) => value,
                Err(_) => break,
            };
            let mut changed = false;
            if let Some(child) = runtime.child.as_mut() {
                match child.try_wait() {
                    Ok(Some(status)) => {
                        let tree = runtime.tree.take();
                        runtime.child.take();
                        runtime.child_started = None;
                        runtime.status.job_object = None;
                        let mut descendants_cleaned = true;
                        if let Some(tree) = tree.as_ref() {
                            if platform::terminate(tree, false).is_err() {
                                descendants_cleaned = false;
                            }
                            thread::sleep(Duration::from_millis(50));
                            if platform::terminate(tree, true).is_err() {
                                descendants_cleaned = false;
                            }
                            platform::cleanup_tree(tree);
                        }
                        record_exit(&mut runtime, &self.bootstrap, status, descendants_cleaned);
                        if let Some(exit) = runtime.status.last_exit.as_ref() {
                            let _ = write_json(&self.paths.exit, exit);
                        }
                        changed = true;
                    }
                    Ok(None) => {
                        if let Some(started) = runtime.child_started
                            && self.bootstrap.stable_after_ms > 0
                            && started.elapsed()
                                >= Duration::from_millis(self.bootstrap.stable_after_ms)
                            && (!runtime.failures.is_empty() || runtime.status.restart_count != 0)
                        {
                            runtime.failures.clear();
                            runtime.status.restart_count = 0;
                            runtime.status.next_restart_at = None;
                            changed = true;
                        }
                    }
                    Err(error) => {
                        runtime.status.last_error = Some(error.to_string());
                        changed = true;
                    }
                }
            } else if runtime.status.desired_state == "running"
                && runtime.status.state != STATE_ORPHANED
            {
                let due = runtime
                    .status
                    .next_restart_at
                    .as_deref()
                    .and_then(parse_millis_timestamp)
                    .map(|value| SystemTime::now() >= value)
                    .unwrap_or(true);
                if due {
                    if let Err(error) = spawn_runtime(&mut runtime, &self.paths, &self.bootstrap) {
                        runtime.status.state = "failed".to_string();
                        runtime.status.last_error = Some(error.to_string());
                    }
                    changed = true;
                }
            }
            if changed {
                runtime.status.updated_at = now_string();
                let _ = self.persist_status_locked(&runtime);
            }
            drop(runtime);
            thread::sleep(Duration::from_millis(50));
        }
    }

    fn persist_status(&self) -> io::Result<()> {
        let runtime = self.runtime.lock().map_err(lock_error)?;
        self.persist_status_locked(&runtime)
    }

    fn persist_status_locked(&self, runtime: &Runtime) -> io::Result<()> {
        write_json(&self.paths.status, &runtime.status)
    }
}

fn spawn_runtime(
    runtime: &mut Runtime,
    paths: &StatePaths,
    bootstrap: &Bootstrap,
) -> io::Result<()> {
    prepare_log_path(&bootstrap.stdout_path)?;
    prepare_log_path(&bootstrap.stderr_path)?;
    rotate_log(
        &bootstrap.stdout_path,
        bootstrap.log_max_size,
        bootstrap.log_max_files,
    )?;
    rotate_log(
        &bootstrap.stderr_path,
        bootstrap.log_max_size,
        bootstrap.log_max_files,
    )?;
    let stdout = open_log(&bootstrap.stdout_path)?;
    let stderr = open_log(&bootstrap.stderr_path)?;
    let spawned = platform::spawn(bootstrap, stdout, stderr)?;
    runtime.child = Some(spawned.child);
    runtime.tree = Some(spawned.tree);
    runtime.child_started = Some(Instant::now());
    runtime.status.state = STATE_RUNNING.to_string();
    runtime.status.service_pid = Some(spawned.pid);
    runtime.status.process_start_token = Some(spawned.process_start_token);
    runtime.status.job_object = spawned.job_object;
    runtime.status.started_at = Some(now_string());
    runtime.status.updated_at = now_string();
    runtime.status.next_restart_at = None;
    runtime.status.last_error = None;
    if let Err(error) = write_json(&paths.status, &runtime.status) {
        stop_runtime(
            runtime,
            bootstrap,
            Duration::from_millis(bootstrap.stop_timeout_ms),
        );
        return Err(error);
    }
    Ok(())
}

fn open_log(path: &str) -> io::Result<File> {
    let mut options = OpenOptions::new();
    options.create(true).append(true);
    #[cfg(unix)]
    {
        use std::os::unix::fs::OpenOptionsExt;
        options.mode(0o600);
    }
    options.open(path)
}

fn prepare_log_path(path: &str) -> io::Result<()> {
    if let Some(parent) = Path::new(path).parent() {
        fs::create_dir_all(parent)?;
        set_private_directory(parent)?;
    }
    Ok(())
}

fn stop_runtime(runtime: &mut Runtime, bootstrap: &Bootstrap, timeout: Duration) {
    runtime.status.desired_state = "stopped".to_string();
    runtime.status.state = STATE_STOPPING.to_string();
    let mut exit_status = None;
    let mut descendants_cleaned = true;
    if let Some(tree) = runtime.tree.as_ref()
        && platform::terminate(tree, false).is_err()
    {
        descendants_cleaned = false;
    }
    let deadline = Instant::now() + timeout;
    if let Some(child) = runtime.child.as_mut() {
        loop {
            match child.try_wait() {
                Ok(Some(status)) => {
                    exit_status = Some(status);
                    break;
                }
                Ok(None) if Instant::now() < deadline => {
                    thread::sleep(Duration::from_millis(20));
                }
                Ok(None) => {
                    if let Some(tree) = runtime.tree.as_ref()
                        && platform::terminate(tree, true).is_err()
                    {
                        descendants_cleaned = false;
                    }
                    exit_status = child.wait().ok();
                    break;
                }
                Err(_) => {
                    if child.kill().is_err() {
                        descendants_cleaned = false;
                    }
                    exit_status = child.wait().ok();
                    break;
                }
            }
        }
    }
    if let Some(tree) = runtime.tree.as_ref() {
        if platform::terminate(tree, true).is_err() {
            descendants_cleaned = false;
        }
        platform::cleanup_tree(tree);
    }
    if let Some(status) = exit_status {
        record_exit(runtime, bootstrap, status, descendants_cleaned);
        if let Some(exit) = runtime.status.last_exit.as_mut() {
            exit.intentional = true;
        }
    }
    runtime.child.take();
    runtime.tree.take();
    runtime.status.job_object = None;
    runtime.child_started = None;
    runtime.status.service_pid = None;
    runtime.status.process_start_token = None;
    runtime.status.started_at = None;
    runtime.status.next_restart_at = None;
    runtime.status.state = STATE_STOPPED.to_string();
    runtime.status.updated_at = now_string();
}

fn record_exit(
    runtime: &mut Runtime,
    bootstrap: &Bootstrap,
    status: ExitStatus,
    descendants_cleaned: bool,
) {
    let code = status.code();
    let signal = exit_signal(&status);
    let success = status.success();
    let sequence = runtime
        .status
        .last_exit
        .as_ref()
        .map(|value| value.sequence + 1)
        .unwrap_or(1);
    runtime.status.last_exit = Some(ExitRecord {
        sequence,
        exit_code: code,
        signal,
        started_at: runtime.status.started_at.clone(),
        ended_at: Some(now_string()),
        intentional: false,
        descendants_cleaned,
        error: None,
    });
    runtime.status.service_pid = None;
    runtime.status.process_start_token = None;
    runtime.status.started_at = None;
    let should_restart =
        bootstrap.restart == "always" || (bootstrap.restart == "on-failure" && !success);
    if !should_restart || runtime.status.desired_state != "running" {
        runtime.status.state = STATE_EXITED.to_string();
        runtime.status.next_restart_at = None;
        return;
    }

    let now = Instant::now();
    let window = Duration::from_millis(bootstrap.restart_window_ms);
    runtime
        .failures
        .retain(|value| now.duration_since(*value) <= window);
    runtime.failures.push(now);
    runtime.status.restart_count = runtime.status.restart_count.saturating_add(1);
    if bootstrap.max_restarts > 0 && runtime.failures.len() as u32 > bootstrap.max_restarts {
        runtime.status.state = STATE_CRASH_LOOP.to_string();
        runtime.status.last_error = Some("restart limit exceeded".to_string());
        runtime.status.next_restart_at = None;
        return;
    }
    let shift = runtime.failures.len().saturating_sub(1).min(6) as u32;
    let backoff = Duration::from_secs(1u64 << shift).min(Duration::from_secs(60));
    runtime.status.state = STATE_BACKING_OFF.to_string();
    runtime.status.next_restart_at = Some(format_millis_timestamp(SystemTime::now() + backoff));
}

#[cfg(unix)]
fn exit_signal(status: &ExitStatus) -> Option<i32> {
    use std::os::unix::process::ExitStatusExt;
    status.signal()
}

#[cfg(not(unix))]
fn exit_signal(_status: &ExitStatus) -> Option<i32> {
    None
}

fn rotate_log(path: &str, max_size: u64, max_files: u32) -> io::Result<()> {
    if max_size == 0 || max_files == 0 {
        return Ok(());
    }
    let path = Path::new(path);
    let metadata = match fs::metadata(path) {
        Ok(value) => value,
        Err(error) if error.kind() == io::ErrorKind::NotFound => return Ok(()),
        Err(error) => return Err(error),
    };
    if metadata.len() < max_size {
        return Ok(());
    }
    for index in (1..max_files).rev() {
        let old = path.with_extension(format!(
            "{}.{}",
            path.extension()
                .and_then(|value| value.to_str())
                .unwrap_or("log"),
            index
        ));
        let new = path.with_extension(format!(
            "{}.{}",
            path.extension()
                .and_then(|value| value.to_str())
                .unwrap_or("log"),
            index + 1
        ));
        if new.exists() {
            let _ = fs::remove_file(&new);
        }
        if old.exists() {
            fs::rename(old, new)?;
        }
    }
    let first = path.with_extension(format!(
        "{}.1",
        path.extension()
            .and_then(|value| value.to_str())
            .unwrap_or("log")
    ));
    if first.exists() {
        let _ = fs::remove_file(&first);
    }
    fs::rename(path, first)?;
    Ok(())
}

#[cfg_attr(not(unix), allow(unused_variables))]
fn set_private_directory(path: &Path) -> io::Result<()> {
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        fs::set_permissions(path, fs::Permissions::from_mode(0o700))?;
    }
    Ok(())
}

fn lock_error<T>(_error: std::sync::PoisonError<T>) -> io::Error {
    io::Error::other("supervisor state lock poisoned")
}

fn format_millis_timestamp(value: SystemTime) -> String {
    value
        .duration_since(SystemTime::UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis()
        .to_string()
}

fn parse_millis_timestamp(value: &str) -> Option<SystemTime> {
    let millis = value.parse::<u128>().ok()?;
    let seconds = millis / 1000;
    let nanos = ((millis % 1000) * 1_000_000) as u32;
    Some(SystemTime::UNIX_EPOCH + Duration::new(seconds as u64, nanos))
}
