mod ipc;
mod platform;
mod protocol;
mod state;
mod supervisor;

use crate::state::{BOOTSTRAP_SCHEMA_VERSION, StatePaths, load_bootstrap, write_text};
use crate::supervisor::Supervisor;
use std::env;
use std::io;
use std::path::PathBuf;
use std::sync::Arc;

fn main() {
    if let Err(error) = run() {
        eprintln!("mango-shim: {error}");
        std::process::exit(1);
    }
}

fn run() -> io::Result<()> {
    let arguments: Vec<String> = env::args().skip(1).collect();
    if arguments.len() == 1 && arguments[0] == "--version" {
        println!(
            "mango-shim {} (commit={}, build_date={})",
            build_version(),
            option_env!("MANGO_COMMIT").unwrap_or("unknown"),
            option_env!("MANGO_BUILD_DATE").unwrap_or("unknown")
        );
        return Ok(());
    }
    if arguments.first().map(String::as_str) != Some("run") {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            "usage: mango-shim run --state-dir DIR --endpoint ENDPOINT",
        ));
    }
    let state_dir = argument_value(&arguments, "--state-dir")?
        .map(PathBuf::from)
        .ok_or_else(|| io::Error::new(io::ErrorKind::InvalidInput, "--state-dir is required"))?;
    let endpoint = argument_value(&arguments, "--endpoint")?
        .map(PathBuf::from)
        .unwrap_or_else(|| state_dir.join("endpoint.sock"));
    let paths = StatePaths::new(&state_dir);
    let bootstrap = load_bootstrap(&paths)?;
    if bootstrap.schema_version != BOOTSTRAP_SCHEMA_VERSION {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            format!(
                "unsupported bootstrap schema version {}; requires version {}",
                bootstrap.schema_version, BOOTSTRAP_SCHEMA_VERSION
            ),
        ));
    }
    if bootstrap.protocol_version != protocol::PROTOCOL_VERSION {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            format!(
                "unsupported bootstrap protocol version {}",
                bootstrap.protocol_version
            ),
        ));
    }
    let supervisor = Supervisor::new(paths, bootstrap)?;
    // Acquire the instance lock before publishing endpoint/PID state. This
    // prevents a duplicate shim from overwriting the live shim's recovery
    // metadata before it is rejected by the lock.
    write_text(
        &supervisor.paths().endpoint,
        endpoint.to_string_lossy().as_ref(),
    )?;
    write_text(
        &supervisor.paths().shim_pid,
        &std::process::id().to_string(),
    )?;
    supervisor.start_monitor();
    let stop = supervisor.stop_flag();
    let handler_supervisor = Arc::clone(&supervisor);
    let handler: Arc<ipc::Handler> = Arc::new(move |request| handler_supervisor.handle(request));
    let serve_result = ipc::serve(&endpoint.to_string_lossy(), stop, handler)
        .map_err(|error| io::Error::new(error.kind(), format!("IPC server: {error}")));
    let finish_result = supervisor.finish();
    serve_result?;
    finish_result
}

fn build_version() -> &'static str {
    option_env!("MANGO_VERSION").unwrap_or(env!("CARGO_PKG_VERSION"))
}

fn argument_value(arguments: &[String], name: &str) -> io::Result<Option<String>> {
    for index in 0..arguments.len() {
        if arguments[index] == name {
            return arguments.get(index + 1).cloned().map(Some).ok_or_else(|| {
                io::Error::new(io::ErrorKind::InvalidInput, format!("{name} needs a value"))
            });
        }
    }
    Ok(None)
}
