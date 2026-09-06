use super::{Handler, handle_connection};
use std::ffi::OsStr;
use std::fs::File;
use std::io;
use std::iter::once;
use std::os::windows::ffi::OsStrExt;
use std::os::windows::io::{FromRawHandle, RawHandle};
use std::ptr::null_mut;
use std::sync::{
    Arc,
    atomic::{AtomicBool, Ordering},
};
use std::thread;
use std::time::Duration;
use windows_sys::Win32::Foundation::{
    CloseHandle, ERROR_PIPE_CONNECTED, ERROR_PIPE_LISTENING, GetLastError, HANDLE,
    INVALID_HANDLE_VALUE, LocalFree,
};
use windows_sys::Win32::Security::Authorization::{
    ConvertStringSecurityDescriptorToSecurityDescriptorW, SDDL_REVISION_1,
};
use windows_sys::Win32::Security::PSECURITY_DESCRIPTOR;
use windows_sys::Win32::Security::SECURITY_ATTRIBUTES;
use windows_sys::Win32::Storage::FileSystem::PIPE_ACCESS_DUPLEX;
use windows_sys::Win32::System::Pipes::{
    ConnectNamedPipe, CreateNamedPipeW, DisconnectNamedPipe, PIPE_NOWAIT, PIPE_READMODE_BYTE,
    PIPE_TYPE_BYTE, PIPE_WAIT, SetNamedPipeHandleState,
};

pub fn serve(endpoint: &str, stop: Arc<AtomicBool>, handler: Arc<Handler>) -> io::Result<()> {
    while !stop.load(Ordering::Acquire) {
        let handle = create_pipe(endpoint)?;
        let connected = loop {
            let result = unsafe { ConnectNamedPipe(handle, null_mut()) };
            if result != 0 {
                break true;
            }
            let error = unsafe { GetLastError() };
            if error == ERROR_PIPE_CONNECTED {
                break true;
            }
            if error == ERROR_PIPE_LISTENING {
                if stop.load(Ordering::Acquire) {
                    unsafe { CloseHandle(handle) };
                    return Ok(());
                }
                thread::sleep(Duration::from_millis(25));
                continue;
            }
            unsafe { CloseHandle(handle) };
            return Err(io::Error::last_os_error());
        };
        if !connected {
            unsafe { CloseHandle(handle) };
            continue;
        }
        let mode = PIPE_READMODE_BYTE | PIPE_WAIT;
        let ok = unsafe { SetNamedPipeHandleState(handle, &mode, null_mut(), null_mut()) };
        if ok == 0 {
            unsafe { CloseHandle(handle) };
            return Err(io::Error::last_os_error());
        }
        let stream = unsafe { File::from_raw_handle(handle as RawHandle) };
        let handler = Arc::clone(&handler);
        thread::spawn(move || handle_connection(stream, &handler));
    }
    Ok(())
}

fn create_pipe(endpoint: &str) -> io::Result<HANDLE> {
    let mut descriptor: PSECURITY_DESCRIPTOR = null_mut();
    let security = to_wide("D:P(A;;GA;;;OW)");
    let converted = unsafe {
        ConvertStringSecurityDescriptorToSecurityDescriptorW(
            security.as_ptr(),
            SDDL_REVISION_1,
            &mut descriptor,
            null_mut(),
        )
    };
    if converted == 0 {
        return Err(io::Error::last_os_error());
    }
    let attributes = SECURITY_ATTRIBUTES {
        nLength: std::mem::size_of::<SECURITY_ATTRIBUTES>() as u32,
        lpSecurityDescriptor: descriptor as *mut _,
        bInheritHandle: 0,
    };
    let name = to_wide(endpoint);
    let handle = unsafe {
        CreateNamedPipeW(
            name.as_ptr(),
            PIPE_ACCESS_DUPLEX,
            PIPE_TYPE_BYTE | PIPE_READMODE_BYTE | PIPE_NOWAIT,
            16,
            64 * 1024,
            64 * 1024,
            0,
            &attributes,
        )
    };
    unsafe { LocalFree(descriptor) };
    if handle == INVALID_HANDLE_VALUE {
        return Err(io::Error::last_os_error());
    }
    Ok(handle)
}

fn to_wide(value: &str) -> Vec<u16> {
    OsStr::new(value).encode_wide().chain(once(0)).collect()
}

#[allow(dead_code)]
fn close_pipe(handle: HANDLE) {
    unsafe {
        let _ = DisconnectNamedPipe(handle);
        CloseHandle(handle);
    }
}
