use crate::state::Bootstrap;
use std::collections::BTreeMap;
use std::fs::File;
use std::io;
use std::process::Child;

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

pub fn resolved_environment(bootstrap: &Bootstrap) -> io::Result<BTreeMap<String, String>> {
    let mut result = BTreeMap::new();
    for (key, value) in &bootstrap.env {
        if let Some(reference) = value.strip_prefix("__MANGO_SECRET_REF__:") {
            let (provider, name) = reference.split_once(':').ok_or_else(|| {
                io::Error::new(
                    io::ErrorKind::InvalidInput,
                    "malformed Mango secret reference",
                )
            })?;
            let resolved = match provider {
                "from_env" => std::env::var(name).map_err(|_| {
                    io::Error::new(
                        io::ErrorKind::NotFound,
                        format!("secret environment variable {name} is not set"),
                    )
                })?,
                "from_file" => std::fs::read_to_string(name)
                    .map_err(|error| {
                        io::Error::new(error.kind(), format!("read secret file {name}: {error}"))
                    })?
                    .trim_end_matches(['\r', '\n'])
                    .to_string(),
                _ => {
                    return Err(io::Error::new(
                        io::ErrorKind::InvalidInput,
                        "unsupported Mango secret provider",
                    ));
                }
            };
            if resolved.is_empty() {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidData,
                    "resolved secret is empty",
                ));
            }
            result.insert(key.clone(), resolved);
        } else {
            result.insert(key.clone(), value.clone());
        }
    }
    Ok(result)
}
