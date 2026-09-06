use crate::state::Bootstrap;
use std::fs::File;
use std::io;
use std::os::unix::process::CommandExt;
use std::process::{Command, Stdio};
#[cfg(target_os = "linux")]
use std::thread;
#[cfg(target_os = "linux")]
use std::time::Duration;

pub struct TreeHandle {
    pub pgid: i32,
}

pub fn spawn_platform(
    bootstrap: &Bootstrap,
    stdout: File,
    stderr: File,
) -> io::Result<super::Spawned> {
    let mut command = Command::new(&bootstrap.command);
    command
        .args(&bootstrap.args)
        .current_dir(if bootstrap.working_dir.is_empty() {
            "."
        } else {
            &bootstrap.working_dir
        })
        .env_clear()
        .envs(&bootstrap.env)
        .stdin(Stdio::null())
        .stdout(Stdio::from(stdout))
        .stderr(Stdio::from(stderr));
    // A new session gives the service root its own process group and keeps
    // ordinary descendants inside the boundary used by terminate().
    unsafe {
        command.pre_exec(|| {
            if libc::setsid() == -1 {
                return Err(io::Error::last_os_error());
            }
            Ok(())
        });
    }
    let child = command.spawn()?;
    let pid = child.id();
    Ok(super::Spawned {
        child,
        tree: TreeHandle { pgid: pid as i32 },
        pid,
        process_start_token: process_start_token(pid),
        job_object: None,
    })
}

pub fn prepare_subreaper() -> io::Result<()> {
    #[cfg(target_os = "linux")]
    {
        let result = unsafe { libc::prctl(libc::PR_SET_CHILD_SUBREAPER, 1, 0, 0, 0) };
        if result != 0 {
            return Err(io::Error::last_os_error());
        }
    }
    Ok(())
}

pub fn terminate(tree: &TreeHandle, force: bool) -> io::Result<()> {
    let signal = if force { libc::SIGKILL } else { libc::SIGTERM };
    let result = unsafe { libc::kill(-tree.pgid, signal) };
    if result != 0 {
        let error = io::Error::last_os_error();
        if error.raw_os_error() == Some(libc::ESRCH) {
            return Ok(());
        }
        return Err(error);
    }
    Ok(())
}

#[cfg(target_os = "linux")]
fn drain_children() {
    loop {
        let result = unsafe { libc::waitpid(-1, std::ptr::null_mut(), libc::WNOHANG) };
        if result > 0 {
            continue;
        }
        if result < 0 && io::Error::last_os_error().raw_os_error() == Some(libc::EINTR) {
            continue;
        }
        break;
    }
}

pub fn cleanup_tree(_tree: &TreeHandle) {
    #[cfg(target_os = "linux")]
    for _ in 0..100 {
        let result = unsafe { libc::waitpid(-1, std::ptr::null_mut(), libc::WNOHANG) };
        if result > 0 {
            drain_children();
            continue;
        }
        if result == 0 {
            thread::sleep(Duration::from_millis(10));
            continue;
        }
        if io::Error::last_os_error().raw_os_error() == Some(libc::EINTR) {
            continue;
        }
        break;
    }
}

pub fn process_is_alive(pid: u32, token: Option<&str>) -> bool {
    if pid == 0 {
        return false;
    }
    let result = unsafe { libc::kill(pid as i32, 0) };
    if result != 0 {
        return false;
    }
    #[cfg(target_os = "linux")]
    if let Ok(data) = fs::read_to_string(format!("/proc/{pid}/stat")) {
        if let Some(end) = data.rfind(')') {
            let fields: Vec<&str> = data[end + 1..].split_whitespace().collect();
            if fields.first() == Some(&"Z") {
                return false;
            }
        }
    }
    match token {
        Some(expected) => process_start_token(pid) == expected,
        None => true,
    }
}

pub fn process_start_token(pid: u32) -> String {
    #[cfg(target_os = "linux")]
    {
        if let Ok(data) = fs::read_to_string(format!("/proc/{pid}/stat")) {
            if let Some(end) = data.rfind(')') {
                let fields: Vec<&str> = data[end + 1..].split_whitespace().collect();
                if fields.len() > 19 {
                    return fields[19].to_string();
                }
            }
        }
    }
    format!("pid:{pid}")
}
