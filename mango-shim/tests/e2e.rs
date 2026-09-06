#![cfg(unix)]

use serde_json::{Value, json};
use std::fs;
use std::io::{Read, Write};
use std::os::unix::net::UnixStream;
use std::path::PathBuf;
use std::process::Command;
use std::thread;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

#[test]
fn root_exit_cleans_process_group_and_writes_exit_state() {
    let root = temporary_directory();
    let state_dir = root.join("state");
    let endpoint = state_dir.join("endpoint.sock");
    let child_pid_path = root.join("child.pid");
    let stdout = root.join("stdout.log");
    let stderr = root.join("stderr.log");
    fs::create_dir_all(&state_dir).expect("state directory");
    let bootstrap = json!({
        "schema_version": 1,
        "protocol_version": 1,
        "service_key": "test/service",
        "instance_id": "test/service",
        "incarnation": "test-incarnation",
        "config_fingerprint": "test-fingerprint",
        "command": "/bin/sh",
        "args": ["-c", format!("sleep 30 & child=$!; echo $child > {}; exit 7", child_pid_path.display())],
        "working_dir": root,
        "env": {},
        "autostart": true,
        "restart": "never",
        "stop_timeout_ms": 1000,
        "max_restarts": 10,
        "restart_window_ms": 300000,
        "stable_after_ms": 60000,
        "stdout_path": stdout,
        "stderr_path": stderr,
        "log_max_size": 1048576,
        "log_max_files": 2
    });
    fs::write(
        state_dir.join("bootstrap.json"),
        serde_json::to_vec_pretty(&bootstrap).unwrap(),
    )
    .expect("bootstrap");

    let mut shim = Command::new(env!("CARGO_BIN_EXE_mango-shim"))
        .args([
            "run",
            "--state-dir",
            state_dir.to_str().unwrap(),
            "--endpoint",
            endpoint.to_str().unwrap(),
        ])
        .spawn()
        .expect("spawn shim");

    wait_for_socket(&endpoint);
    let status = wait_for_status(&endpoint, |value| value["state"] == "exited");
    assert_eq!(status["last_exit"]["exit_code"], 7);
    assert_eq!(status["last_exit"]["descendants_cleaned"], true);

    let child_pid = wait_for_child_pid(&child_pid_path);
    for _ in 0..40 {
        if !process_exists(child_pid) {
            break;
        }
        thread::sleep(Duration::from_millis(50));
    }
    assert!(
        !process_exists(child_pid),
        "child process {child_pid} survived"
    );

    let shutdown = request(&endpoint, "shutdown", json!({"timeout_ms": 1000}));
    assert_eq!(shutdown["ok"], true);
    let result = shim.wait().expect("wait shim");
    assert!(result.success(), "shim exited with {result}");
}

#[test]
fn always_restart_uses_backoff_and_crash_loop_guard() {
    let root = temporary_directory();
    let state_dir = root.join("state");
    let endpoint = state_dir.join("endpoint.sock");
    fs::create_dir_all(&state_dir).expect("state directory");
    let bootstrap = json!({
        "schema_version": 1,
        "protocol_version": 1,
        "service_key": "test/restart",
        "instance_id": "test/restart",
        "incarnation": "test-incarnation",
        "config_fingerprint": "test-fingerprint",
        "command": "/bin/sh",
        "args": ["-c", "exit 7"],
        "working_dir": root,
        "env": {},
        "autostart": true,
        "restart": "always",
        "stop_timeout_ms": 1000,
        "max_restarts": 1,
        "restart_window_ms": 300000,
        "stable_after_ms": 60000,
        "stdout_path": root.join("stdout.log"),
        "stderr_path": root.join("stderr.log"),
        "log_max_size": 1048576,
        "log_max_files": 2
    });
    fs::write(
        state_dir.join("bootstrap.json"),
        serde_json::to_vec_pretty(&bootstrap).unwrap(),
    )
    .expect("bootstrap");

    let mut shim = Command::new(env!("CARGO_BIN_EXE_mango-shim"))
        .args([
            "run",
            "--state-dir",
            state_dir.to_str().unwrap(),
            "--endpoint",
            endpoint.to_str().unwrap(),
        ])
        .spawn()
        .expect("spawn shim");

    wait_for_socket(&endpoint);
    let status = wait_for_status(&endpoint, |value| value["state"] == "crash_loop");
    assert_eq!(status["restart_count"], 2);
    assert_eq!(status["last_exit"]["exit_code"], 7);
    let shutdown = request(&endpoint, "shutdown", json!({"timeout_ms": 1000}));
    assert_eq!(shutdown["ok"], true);
    let result = shim.wait().expect("wait shim");
    assert!(result.success(), "shim exited with {result}");
}

fn request(endpoint: &PathBuf, method: &str, params: Value) -> Value {
    let mut stream = UnixStream::connect(endpoint).expect("connect shim");
    let request = json!({
        "version": 1,
        "request_id": "test-request",
        "method": method,
        "params": params
    });
    stream
        .write_all(format!("{}\n", request).as_bytes())
        .expect("write request");
    let mut response = String::new();
    stream.read_to_string(&mut response).expect("read response");
    serde_json::from_str(response.trim()).expect("decode response")
}

fn try_request(endpoint: &PathBuf, method: &str, params: Value) -> Result<Value, ()> {
    let mut stream = UnixStream::connect(endpoint).map_err(|_| ())?;
    let request = json!({
        "version": 1,
        "request_id": "test-request",
        "method": method,
        "params": params
    });
    stream
        .write_all(format!("{}\n", request).as_bytes())
        .map_err(|_| ())?;
    let mut response = String::new();
    stream.read_to_string(&mut response).map_err(|_| ())?;
    serde_json::from_str(response.trim()).map_err(|_| ())
}

fn wait_for_status(endpoint: &PathBuf, predicate: impl Fn(&Value) -> bool) -> Value {
    for _ in 0..100 {
        if let Ok(value) = try_request(endpoint, "status", json!({}))
            && predicate(&value["data"])
        {
            return value["data"].clone();
        }
        thread::sleep(Duration::from_millis(50));
    }
    panic!("status predicate was not met");
}

fn wait_for_socket(endpoint: &PathBuf) {
    for _ in 0..100 {
        if UnixStream::connect(endpoint).is_ok() {
            return;
        }
        thread::sleep(Duration::from_millis(20));
    }
    panic!("shim endpoint did not become ready");
}

fn wait_for_child_pid(path: &PathBuf) -> i32 {
    for _ in 0..100 {
        if let Ok(value) = fs::read_to_string(path)
            && let Ok(pid) = value.trim().parse()
        {
            return pid;
        }
        thread::sleep(Duration::from_millis(20));
    }
    panic!("child pid was not written");
}

fn process_exists(pid: i32) -> bool {
    unsafe { libc::kill(pid, 0) == 0 }
}

fn temporary_directory() -> PathBuf {
    let suffix = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_nanos();
    let path = PathBuf::from("/tmp").join(format!("ms{}", suffix % 1_000_000_000));
    fs::create_dir_all(&path).expect("temporary directory");
    path
}
