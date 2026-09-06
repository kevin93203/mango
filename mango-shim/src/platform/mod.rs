use crate::state::Bootstrap;
use std::fs::File;
use std::io;
use std::process::{Child, ExitStatus};

#[cfg(unix)]
mod unix;
#[cfg(windows)]
mod windows;

#[cfg(unix)]
pub use unix::*;
#[cfg(windows)]
pub use windows::*;

pub struct Spawned {
    pub child: Child,
    pub tree: TreeHandle,
    pub pid: u32,
    pub process_start_token: String,
    pub job_object: Option<String>,
}

pub fn spawn(bootstrap: &Bootstrap, stdout: File, stderr: File) -> io::Result<Spawned> {
    spawn_platform(bootstrap, stdout, stderr)
}

#[allow(dead_code)]
pub fn status_success(status: &ExitStatus) -> bool {
    status.success()
}
