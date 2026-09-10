use crate::state::Bootstrap;
#[cfg(target_os = "linux")]
use std::fs;
use std::io;
#[cfg(target_os = "linux")]
use std::os::unix::ffi::OsStrExt;
use std::os::unix::process::CommandExt;
use std::process::{Command, Stdio};
#[cfg(target_os = "linux")]
use std::thread;
#[cfg(target_os = "linux")]
use std::time::Duration;
#[cfg(target_os = "linux")]
use std::time::{SystemTime, UNIX_EPOCH};

pub struct TreeHandle {
    pub pgid: i32,
    #[cfg(target_os = "linux")]
    pub cgroup: Option<std::path::PathBuf>,
}

pub fn spawn_platform(bootstrap: &Bootstrap) -> io::Result<super::Spawned> {
    #[cfg(target_os = "macos")]
    if bootstrap.resources.is_some() {
        return Err(io::Error::new(
            io::ErrorKind::Unsupported,
            "resource policies are unsupported on macOS",
        ));
    }
    let mut command = Command::new(&bootstrap.command);
    let resolved = super::resolved_environment(bootstrap)?;
    let identity = bootstrap.run_as.clone();
    #[cfg(target_os = "linux")]
    let cgroup = prepare_cgroup(bootstrap)?;
    #[cfg(target_os = "linux")]
    let cgroup_procs = cgroup.as_ref().map(|path| {
        let mut value = path.join("cgroup.procs").as_os_str().as_bytes().to_vec();
        value.push(0);
        value
    });
    command
        .args(&bootstrap.args)
        .current_dir(if bootstrap.working_dir.is_empty() {
            "."
        } else {
            &bootstrap.working_dir
        })
        .env_clear()
        .envs(&resolved.values)
        .stdin(Stdio::null())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped());
    // A new session gives the service root its own process group and keeps
    // ordinary descendants inside the boundary used by terminate().
    unsafe {
        command.pre_exec(move || {
            if libc::setsid() == -1 {
                return Err(io::Error::last_os_error());
            }
            #[cfg(target_os = "linux")]
            if let Some(path) = &cgroup_procs {
                let fd = libc::open(path.as_ptr().cast(), libc::O_WRONLY | libc::O_CLOEXEC);
                if fd == -1 {
                    return Err(io::Error::last_os_error());
                }
                let mut digits = [0u8; 20];
                let mut value = libc::getpid() as u32;
                let mut length = 0usize;
                while value > 0 {
                    digits[length] = b'0' + (value % 10) as u8;
                    length += 1;
                    value /= 10;
                }
                for index in 0..(length / 2) {
                    digits.swap(index, length - index - 1);
                }
                let written = libc::write(fd, digits.as_ptr().cast(), length);
                libc::close(fd);
                if written != length as isize {
                    return Err(io::Error::last_os_error());
                }
            }
            if let Some(identity) = &identity {
                if libc::setgid(identity.gid) != 0 {
                    return Err(io::Error::last_os_error());
                }
                if libc::setuid(identity.uid) != 0 {
                    return Err(io::Error::last_os_error());
                }
            }
            Ok(())
        });
    }
    let mut child = match command.spawn() {
        Ok(child) => child,
        Err(error) => {
            #[cfg(target_os = "linux")]
            if let Some(path) = &cgroup {
                let _ = fs::remove_dir(path);
            }
            return Err(error);
        }
    };
    let pid = child.id();
    let stdout = child
        .stdout
        .take()
        .ok_or_else(|| io::Error::other("child stdout pipe was not created"))?;
    let stderr = child
        .stderr
        .take()
        .ok_or_else(|| io::Error::other("child stderr pipe was not created"))?;
    #[cfg(target_os = "linux")]
    let tree = TreeHandle {
        pgid: pid as i32,
        cgroup,
    };
    #[cfg(not(target_os = "linux"))]
    let tree = TreeHandle { pgid: pid as i32 };
    Ok(super::Spawned {
        stdout,
        stderr,
        secrets: resolved.secrets,
        child,
        tree,
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
    #[cfg(target_os = "linux")]
    if let Some(path) = &_tree.cgroup {
        if fs::remove_dir(path).is_err() {
            let _ = fs::write(path.join("cgroup.kill"), b"1");
            for _ in 0..10 {
                if fs::remove_dir(path).is_ok() {
                    break;
                }
                thread::sleep(Duration::from_millis(10));
            }
        }
    }
}

#[cfg(target_os = "linux")]
fn prepare_cgroup(bootstrap: &Bootstrap) -> io::Result<Option<std::path::PathBuf>> {
    let Some(policy) = bootstrap.resources.as_ref() else {
        return Ok(None);
    };
    let parent = std::path::PathBuf::from("/sys/fs/cgroup")
        .join(current_cgroup_path().trim_start_matches('/'));
    let timestamp = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_nanos();
    let path = parent.join(format!("mango-shim-{}-{timestamp}", std::process::id()));
    fs::create_dir(&path)?;
    let result = (|| {
        if policy.process_limit > 0 {
            fs::write(path.join("pids.max"), policy.process_limit.to_string())?;
        }
        if policy.memory_bytes > 0 {
            fs::write(path.join("memory.max"), policy.memory_bytes.to_string())?;
        }
        if policy.cpu_percent > 0 {
            fs::write(
                path.join("cpu.max"),
                format!("{} 100000", u64::from(policy.cpu_percent) * 1000),
            )?;
        }
        Ok::<(), io::Error>(())
    })();
    if let Err(error) = result {
        let _ = fs::remove_dir(&path);
        return Err(io::Error::new(
            error.kind(),
            format!("apply service cgroup: {error}"),
        ));
    }
    Ok(Some(path))
}

#[cfg(target_os = "linux")]
fn current_cgroup_path() -> String {
    if let Ok(data) = fs::read_to_string("/proc/self/cgroup") {
        for line in data.lines() {
            if let Some(path) = line.strip_prefix("0::") {
                return path.to_string();
            }
        }
    }
    String::new()
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
    if let Ok(data) = fs::read_to_string(format!("/proc/{pid}/stat"))
        && let Some(end) = data.rfind(')')
    {
        let fields: Vec<&str> = data[end + 1..].split_whitespace().collect();
        if fields.first() == Some(&"Z") {
            return false;
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
        if let Ok(data) = fs::read_to_string(format!("/proc/{pid}/stat"))
            && let Some(end) = data.rfind(')')
        {
            let fields: Vec<&str> = data[end + 1..].split_whitespace().collect();
            if fields.len() > 19 {
                return fields[19].to_string();
            }
        }
    }
    format!("pid:{pid}")
}
