use crate::state::Bootstrap;
use std::fs::File;
use std::io;
use std::os::windows::process::CommandExt;
use std::process::{Command, Stdio};
use std::ptr::null;
use windows_sys::Win32::Foundation::{CloseHandle, ERROR_ALREADY_EXISTS, GetLastError, HANDLE};
use windows_sys::Win32::System::JobObjects::{
    AssignProcessToJobObject, CreateJobObjectW, JOB_OBJECT_LIMIT_KILL_ON_JOB_CLOSE,
    JOBOBJECT_EXTENDED_LIMIT_INFORMATION, JobObjectExtendedLimitInformation,
    SetInformationJobObject, TerminateJobObject,
};
use windows_sys::Win32::System::Threading::{
    GetProcessTimes, OpenProcess, PROCESS_QUERY_LIMITED_INFORMATION, PROCESS_SET_QUOTA,
    PROCESS_TERMINATE,
};

const CREATE_NEW_PROCESS_GROUP: u32 = 0x0000_0200;
const CREATE_NO_WINDOW: u32 = 0x0800_0000;
const STILL_ACTIVE: u32 = 259;

pub struct TreeHandle {
    pub job: HANDLE,
}

unsafe impl Send for TreeHandle {}

impl Drop for TreeHandle {
    fn drop(&mut self) {
        if !self.job.is_null() {
            unsafe { CloseHandle(self.job) };
            self.job = std::ptr::null_mut();
        }
    }
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
        .stderr(Stdio::from(stderr))
        .creation_flags(CREATE_NEW_PROCESS_GROUP | CREATE_NO_WINDOW);
    let mut child = command.spawn()?;
    let pid = child.id();
    let job_name = job_object_name(bootstrap);
    let job_name_w = to_wide(&job_name);
    let mut job = unsafe { CreateJobObjectW(null(), job_name_w.as_ptr()) };
    if job.is_null() {
        let _ = child.kill();
        let _ = child.wait();
        return Err(io::Error::last_os_error());
    }
    if unsafe { GetLastError() } == ERROR_ALREADY_EXISTS {
        // The instance lock prevents another shim from owning this identity.
        // A named stale job is therefore safe to tear down before reuse.
        let _ = unsafe { TerminateJobObject(job, 1) };
        unsafe { CloseHandle(job) };
        job = unsafe { CreateJobObjectW(null(), job_name_w.as_ptr()) };
        if job.is_null() {
            let _ = child.kill();
            let _ = child.wait();
            return Err(io::Error::last_os_error());
        }
    }
    let mut info: JOBOBJECT_EXTENDED_LIMIT_INFORMATION = unsafe { std::mem::zeroed() };
    info.BasicLimitInformation.LimitFlags = JOB_OBJECT_LIMIT_KILL_ON_JOB_CLOSE;
    let set = unsafe {
        SetInformationJobObject(
            job,
            JobObjectExtendedLimitInformation,
            &mut info as *mut _ as *mut _,
            std::mem::size_of::<JOBOBJECT_EXTENDED_LIMIT_INFORMATION>() as u32,
        )
    };
    if set == 0 {
        unsafe { CloseHandle(job) };
        let _ = child.kill();
        let _ = child.wait();
        return Err(io::Error::last_os_error());
    }
    let process = unsafe {
        OpenProcess(
            PROCESS_SET_QUOTA | PROCESS_TERMINATE | PROCESS_QUERY_LIMITED_INFORMATION,
            0,
            pid,
        )
    };
    if process.is_null() {
        unsafe { CloseHandle(job) };
        let _ = child.kill();
        let _ = child.wait();
        return Err(io::Error::last_os_error());
    }
    let assigned = unsafe { AssignProcessToJobObject(job, process) };
    unsafe { CloseHandle(process) };
    if assigned == 0 {
        unsafe { CloseHandle(job) };
        let _ = child.kill();
        let _ = child.wait();
        return Err(io::Error::last_os_error());
    }
    Ok(super::Spawned {
        child,
        tree: TreeHandle { job },
        pid,
        process_start_token: process_start_token(pid),
        job_object: Some(job_name),
    })
}

fn job_object_name(bootstrap: &Bootstrap) -> String {
    let mut hash = 0xcbf29ce484222325u64;
    for byte in format!(
        "{}\0{}\0{}",
        bootstrap.service_key, bootstrap.instance_id, bootstrap.incarnation
    )
    .bytes()
    {
        hash ^= u64::from(byte);
        hash = hash.wrapping_mul(0x100000001b3);
    }
    format!("MangoShim-{hash:016x}")
}

fn to_wide(value: &str) -> Vec<u16> {
    use std::ffi::OsStr;
    use std::iter::once;
    use std::os::windows::ffi::OsStrExt;
    OsStr::new(value).encode_wide().chain(once(0)).collect()
}

pub fn prepare_subreaper() -> io::Result<()> {
    Ok(())
}

pub fn terminate(tree: &TreeHandle, _force: bool) -> io::Result<()> {
    if tree.job.is_null() {
        return Ok(());
    }
    let result = unsafe { TerminateJobObject(tree.job, 1) };
    if result == 0 {
        let error = unsafe { GetLastError() };
        if error == windows_sys::Win32::Foundation::ERROR_INVALID_HANDLE {
            return Ok(());
        }
        return Err(io::Error::from_raw_os_error(error as i32));
    }
    Ok(())
}

#[allow(dead_code)]
pub fn drain_children() {}

pub fn cleanup_tree(_tree: &TreeHandle) {}

pub fn process_is_alive(pid: u32, _token: Option<&str>) -> bool {
    if pid == 0 {
        return false;
    }
    let process = unsafe { OpenProcess(PROCESS_QUERY_LIMITED_INFORMATION, 0, pid) };
    if process.is_null() {
        return false;
    }
    let mut exit_code = 0;
    let result = unsafe {
        windows_sys::Win32::System::Threading::GetExitCodeProcess(process, &mut exit_code)
    };
    unsafe { CloseHandle(process) };
    result != 0 && exit_code == STILL_ACTIVE
}

pub fn process_start_token(pid: u32) -> String {
    let process = unsafe { OpenProcess(PROCESS_QUERY_LIMITED_INFORMATION, 0, pid) };
    if process.is_null() {
        return format!("pid:{pid}");
    }
    let mut creation = unsafe { std::mem::zeroed() };
    let mut exit = unsafe { std::mem::zeroed() };
    let mut kernel = unsafe { std::mem::zeroed() };
    let mut user = unsafe { std::mem::zeroed() };
    let token =
        if unsafe { GetProcessTimes(process, &mut creation, &mut exit, &mut kernel, &mut user) }
            != 0
        {
            format!("{}:{}", creation.dwHighDateTime, creation.dwLowDateTime)
        } else {
            format!("pid:{pid}")
        };
    unsafe { CloseHandle(process) };
    token
}
