use super::{Handler, handle_connection};
use std::fs;
use std::io;
use std::os::unix::net::UnixListener;
use std::path::Path;
use std::sync::{
    Arc,
    atomic::{AtomicBool, Ordering},
};
use std::thread;
use std::time::Duration;

pub fn serve(endpoint: &str, stop: Arc<AtomicBool>, handler: Arc<Handler>) -> io::Result<()> {
    let path = Path::new(endpoint);
    if let Some(parent) = path.parent() {
        fs::create_dir_all(parent)?;
    }
    match fs::remove_file(path) {
        Ok(()) => {}
        Err(error) if error.kind() == io::ErrorKind::NotFound => {}
        Err(error) => return Err(error),
    }
    let listener = UnixListener::bind(path)
        .map_err(|error| io::Error::new(error.kind(), format!("bind {path:?}: {error}")))?;
    if let Err(error) = fs::set_permissions(path, fs::Permissions::from_mode(0o600))
        && !matches!(error.raw_os_error(), Some(libc::EPERM | libc::EACCES))
    {
        return Err(error);
    }
    listener.set_nonblocking(true)?;
    while !stop.load(Ordering::Acquire) {
        match listener.accept() {
            Ok((stream, _)) => {
                let handler = Arc::clone(&handler);
                thread::spawn(move || handle_connection(stream, &handler));
            }
            Err(error)
                if error.kind() == io::ErrorKind::WouldBlock
                    || matches!(error.raw_os_error(), Some(libc::EPERM | libc::EACCES)) =>
            {
                thread::sleep(Duration::from_millis(25));
            }
            Err(error) => return Err(error),
        }
    }
    let _ = fs::remove_file(path);
    Ok(())
}

use std::os::unix::fs::PermissionsExt;
