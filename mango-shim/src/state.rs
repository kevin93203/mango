use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;
use std::fs::{self, File, OpenOptions};
use std::io;
use std::path::{Path, PathBuf};
use std::time::{SystemTime, UNIX_EPOCH};

pub const STATE_SCHEMA_VERSION: u32 = 2;
pub const BOOTSTRAP_SCHEMA_VERSION: u32 = 2;

#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct SecretReference {
    pub provider: String,
    pub name: String,
}

#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct Identity {
    pub uid: u32,
    pub gid: u32,
}

#[derive(Debug, Clone, Deserialize, Serialize, Default)]
pub struct ResourceLimits {
    #[serde(default)]
    pub process_limit: u32,
    #[serde(default)]
    pub memory_bytes: u64,
    #[serde(default)]
    pub cpu_percent: u32,
}

#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct Bootstrap {
    pub schema_version: u32,
    pub protocol_version: u32,
    pub service_key: String,
    pub instance_id: String,
    pub incarnation: String,
    pub config_fingerprint: String,
    pub command: String,
    #[serde(default)]
    pub args: Vec<String>,
    #[serde(default)]
    pub working_dir: String,
    #[serde(default)]
    pub env: BTreeMap<String, String>,
    #[serde(default)]
    pub secret_refs: BTreeMap<String, SecretReference>,
    #[serde(default)]
    pub run_as: Option<Identity>,
    #[serde(default)]
    pub resources: Option<ResourceLimits>,
    #[serde(default)]
    pub autostart: bool,
    #[serde(default = "default_restart")]
    pub restart: String,
    #[serde(default = "default_stop_timeout")]
    pub stop_timeout_ms: u64,
    #[serde(default = "default_max_restarts")]
    pub max_restarts: u32,
    #[serde(default = "default_restart_window")]
    pub restart_window_ms: u64,
    #[serde(default = "default_stable_after")]
    pub stable_after_ms: u64,
    pub stdout_path: String,
    pub stderr_path: String,
    #[serde(default = "default_log_max_size")]
    pub log_max_size: u64,
    #[serde(default = "default_log_max_files")]
    pub log_max_files: u32,
}

fn default_restart() -> String {
    "never".to_string()
}

fn default_stop_timeout() -> u64 {
    10_000
}

fn default_max_restarts() -> u32 {
    10
}

fn default_restart_window() -> u64 {
    300_000
}

fn default_stable_after() -> u64 {
    60_000
}

fn default_log_max_size() -> u64 {
    100 * 1024 * 1024
}

fn default_log_max_files() -> u32 {
    10
}

#[derive(Debug, Clone, Default, Deserialize, Serialize)]
pub struct ExitRecord {
    pub sequence: u64,
    pub exit_code: Option<i32>,
    pub signal: Option<i32>,
    pub started_at: Option<String>,
    pub ended_at: Option<String>,
    pub intentional: bool,
    pub descendants_cleaned: bool,
    pub error: Option<String>,
}

#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct Status {
    pub schema_version: u32,
    pub protocol_version: u32,
    pub service_key: String,
    pub instance_id: String,
    pub incarnation: String,
    pub config_fingerprint: String,
    pub state: String,
    pub desired_state: String,
    pub shim_pid: u32,
    pub service_pid: Option<u32>,
    pub process_start_token: Option<String>,
    pub started_at: Option<String>,
    pub updated_at: String,
    pub restart_count: u32,
    pub next_restart_at: Option<String>,
    pub last_exit: Option<ExitRecord>,
    pub last_error: Option<String>,
    pub stdout_path: String,
    pub stderr_path: String,
    #[serde(default)]
    pub job_object: Option<String>,
}

impl Status {
    pub fn initial(bootstrap: &Bootstrap, shim_pid: u32) -> Self {
        Self {
            schema_version: STATE_SCHEMA_VERSION,
            protocol_version: bootstrap.protocol_version,
            service_key: bootstrap.service_key.clone(),
            instance_id: bootstrap.instance_id.clone(),
            incarnation: bootstrap.incarnation.clone(),
            config_fingerprint: bootstrap.config_fingerprint.clone(),
            state: "stopped".to_string(),
            desired_state: if bootstrap.autostart {
                "running".to_string()
            } else {
                "stopped".to_string()
            },
            shim_pid,
            service_pid: None,
            process_start_token: None,
            started_at: None,
            updated_at: now_string(),
            restart_count: 0,
            next_restart_at: None,
            last_exit: None,
            last_error: None,
            stdout_path: bootstrap.stdout_path.clone(),
            stderr_path: bootstrap.stderr_path.clone(),
            job_object: None,
        }
    }
}

pub struct StatePaths {
    pub dir: PathBuf,
    pub bootstrap: PathBuf,
    pub status: PathBuf,
    pub exit: PathBuf,
    pub endpoint: PathBuf,
    pub shim_pid: PathBuf,
    pub lock: PathBuf,
}

impl StatePaths {
    pub fn new(dir: impl Into<PathBuf>) -> Self {
        let dir = dir.into();
        Self {
            bootstrap: dir.join("bootstrap.json"),
            status: dir.join("status.json"),
            exit: dir.join("exit.json"),
            endpoint: dir.join("endpoint"),
            shim_pid: dir.join("shim.pid"),
            lock: dir.join("instance.lock"),
            dir,
        }
    }
}

pub fn load_bootstrap(paths: &StatePaths) -> io::Result<Bootstrap> {
    let data = fs::read(&paths.bootstrap)?;
    serde_json::from_slice(&data).map_err(io::Error::other)
}

pub fn load_status(paths: &StatePaths) -> io::Result<Option<Status>> {
    match fs::read(&paths.status) {
        Ok(data) => serde_json::from_slice(&data)
            .map(Some)
            .map_err(io::Error::other),
        Err(error) if error.kind() == io::ErrorKind::NotFound => Ok(None),
        Err(error) => Err(error),
    }
}

pub fn write_json<T: Serialize>(path: &Path, value: &T) -> io::Result<()> {
    let parent = path
        .parent()
        .ok_or_else(|| io::Error::new(io::ErrorKind::InvalidInput, "state path has no parent"))?;
    fs::create_dir_all(parent)?;
    let name = path
        .file_name()
        .and_then(|value| value.to_str())
        .unwrap_or("state.json");
    let temporary = parent.join(format!(".{name}.{}.tmp", std::process::id()));
    let data = serde_json::to_vec_pretty(value).map_err(io::Error::other)?;
    {
        let mut file = File::create(&temporary)?;
        set_private_permissions(&file)?;
        use std::io::Write;
        file.write_all(&data)?;
        file.write_all(b"\n")?;
        file.sync_all()?;
    }
    replace_file(&temporary, path)?;
    Ok(())
}

pub fn write_text(path: &Path, value: &str) -> io::Result<()> {
    let parent = path
        .parent()
        .ok_or_else(|| io::Error::new(io::ErrorKind::InvalidInput, "state path has no parent"))?;
    fs::create_dir_all(parent)?;
    let temporary = parent.join(format!(
        ".{}.{}.tmp",
        path.file_name().and_then(|v| v.to_str()).unwrap_or("state"),
        std::process::id()
    ));
    {
        let mut file = File::create(&temporary)?;
        set_private_permissions(&file)?;
        use std::io::Write;
        file.write_all(value.as_bytes())?;
        file.write_all(b"\n")?;
        file.sync_all()?;
    }
    replace_file(&temporary, path)?;
    Ok(())
}

pub fn open_lock(path: &Path) -> io::Result<File> {
    if let Some(parent) = path.parent() {
        fs::create_dir_all(parent)?;
    }
    let file = OpenOptions::new()
        .create(true)
        .read(true)
        .write(true)
        .truncate(false)
        .open(path)?;
    fs2::FileExt::try_lock_exclusive(&file)?;
    set_private_permissions(&file)?;
    Ok(file)
}

pub fn now_string() -> String {
    let duration = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default();
    format!("{}.{:09}Z", duration.as_secs(), duration.subsec_nanos())
}

#[cfg_attr(not(unix), allow(unused_variables))]
fn set_private_permissions(file: &File) -> io::Result<()> {
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        file.set_permissions(fs::Permissions::from_mode(0o600))?;
    }
    Ok(())
}

#[cfg(not(windows))]
fn replace_file(temporary: &Path, destination: &Path) -> io::Result<()> {
    fs::rename(temporary, destination)
}

#[cfg(windows)]
fn replace_file(temporary: &Path, destination: &Path) -> io::Result<()> {
    use std::ffi::OsStr;
    use std::iter::once;
    use std::os::windows::ffi::OsStrExt;
    use windows_sys::Win32::Storage::FileSystem::{
        MOVEFILE_REPLACE_EXISTING, MOVEFILE_WRITE_THROUGH, MoveFileExW,
    };

    let temporary: Vec<u16> = OsStr::new(temporary).encode_wide().chain(once(0)).collect();
    let destination: Vec<u16> = OsStr::new(destination)
        .encode_wide()
        .chain(once(0))
        .collect();
    let result = unsafe {
        MoveFileExW(
            temporary.as_ptr(),
            destination.as_ptr(),
            MOVEFILE_REPLACE_EXISTING | MOVEFILE_WRITE_THROUGH,
        )
    };
    if result == 0 {
        return Err(io::Error::last_os_error());
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::{SystemTime, UNIX_EPOCH};

    #[test]
    fn atomic_json_write_replaces_existing_file() {
        let suffix = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("clock")
            .as_nanos();
        let root = std::env::temp_dir().join(format!("mango-shim-state-{suffix}"));
        fs::create_dir_all(&root).expect("create test directory");
        let path = root.join("status.json");
        write_json(&path, &serde_json::json!({"state": "old"})).expect("first write");
        write_json(&path, &serde_json::json!({"state": "new"})).expect("replacement write");
        let value: serde_json::Value =
            serde_json::from_slice(&fs::read(&path).expect("read")).expect("decode");
        assert_eq!(value["state"], "new");
        assert!(
            !root
                .join(format!(".status.json.{}.tmp", std::process::id()))
                .exists()
        );
        let _ = fs::remove_dir_all(root);
    }

    #[test]
    fn instance_lock_prevents_duplicate_supervisors() {
        let suffix = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("clock")
            .as_nanos();
        let root = std::env::temp_dir().join(format!("mango-shim-lock-{suffix}"));
        let lock_path = root.join("instance.lock");
        let first = open_lock(&lock_path).expect("first lock");
        assert!(open_lock(&lock_path).is_err());
        drop(first);
        let second = open_lock(&lock_path).expect("lock after release");
        drop(second);
        let _ = fs::remove_dir_all(root);
    }
}
