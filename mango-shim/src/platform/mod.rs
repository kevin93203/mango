use crate::state::Bootstrap;
use std::collections::BTreeMap;
use std::io;
use std::process::{Child, ChildStderr, ChildStdout};

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
    pub stdout: ChildStdout,
    pub stderr: ChildStderr,
    pub secrets: Vec<Vec<u8>>,
    pub tree: TreeHandle,
    pub pid: u32,
    pub process_start_token: String,
    pub job_object: Option<String>,
}

pub fn spawn(bootstrap: &Bootstrap) -> io::Result<Spawned> {
    validate_policy(bootstrap)?;
    spawn_platform(bootstrap)
}

fn validate_policy(bootstrap: &Bootstrap) -> io::Result<()> {
    if let Some(policy) = bootstrap.resources.as_ref()
        && policy.cpu_percent > 100
    {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            "resources.cpu_percent must be between 0 and 100",
        ));
    }
    Ok(())
}

pub struct ResolvedEnvironment {
    pub values: BTreeMap<String, String>,
    pub secrets: Vec<Vec<u8>>,
}

pub fn resolved_environment(bootstrap: &Bootstrap) -> io::Result<ResolvedEnvironment> {
    let mut result = BTreeMap::new();
    let mut secrets = Vec::with_capacity(bootstrap.secret_refs.len());
    for (key, value) in &bootstrap.env {
        result.insert(key.clone(), value.clone());
    }
    for (key, reference) in &bootstrap.secret_refs {
        let resolved = match reference.provider.as_str() {
            "from_env" => {
                if !valid_environment_name(&reference.name) {
                    return Err(io::Error::new(
                        io::ErrorKind::InvalidInput,
                        format!("environment secret name {} is invalid", reference.name),
                    ));
                }
                std::env::var(&reference.name).map_err(|_| {
                    io::Error::new(
                        io::ErrorKind::NotFound,
                        format!("secret environment variable {} is not set", reference.name),
                    )
                })?
            }
            "from_file" => std::fs::read_to_string(&reference.name)
                .map_err(|error| {
                    io::Error::new(
                        error.kind(),
                        format!("read secret file {}: {error}", reference.name),
                    )
                })?
                .trim_end_matches(['\r', '\n'])
                .to_string(),
            _ => {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidInput,
                    format!("unsupported Mango secret provider {}", reference.provider),
                ));
            }
        };
        if resolved.is_empty() {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "resolved secret is empty",
            ));
        }
        secrets.push(resolved.as_bytes().to_vec());
        result.insert(key.clone(), resolved);
    }
    Ok(ResolvedEnvironment {
        values: result,
        secrets,
    })
}

fn valid_environment_name(value: &str) -> bool {
    let mut characters = value.chars();
    matches!(characters.next(), Some('_' | 'A'..='Z' | 'a'..='z'))
        && characters.all(|character| character == '_' || character.is_ascii_alphanumeric())
}

#[cfg(test)]
mod tests {
    use super::resolved_environment;
    use crate::state::Bootstrap;
    use serde_json::json;
    use std::fs;

    #[test]
    fn magic_prefix_is_preserved_as_a_plain_environment_value() {
        let bootstrap: Bootstrap = serde_json::from_value(json!({
            "schema_version": 2,
            "protocol_version": 3,
            "service_key": "test/service",
            "instance_id": "test/service",
            "incarnation": "test",
            "config_fingerprint": "fingerprint",
            "command": "fixture",
            "env": {"VALUE": "__MANGO_SECRET_REF__:from_env:LEGAL"},
            "stdout_path": "stdout.log",
            "stderr_path": "stderr.log"
        }))
        .expect("bootstrap should decode");
        let resolved = resolved_environment(&bootstrap).expect("environment should resolve");
        assert_eq!(
            resolved.values["VALUE"],
            "__MANGO_SECRET_REF__:from_env:LEGAL"
        );
        assert!(resolved.secrets.is_empty());
    }

    #[test]
    fn file_secret_is_trimmed_and_returned_only_to_the_runtime() {
        let path = std::env::temp_dir().join(format!("mango-secret-{}", std::process::id()));
        fs::write(&path, b"top-secret\r\n").expect("secret file");
        let bootstrap: Bootstrap = serde_json::from_value(json!({
            "schema_version": 2,
            "protocol_version": 3,
            "service_key": "test/service",
            "instance_id": "test/service",
            "incarnation": "test",
            "config_fingerprint": "fingerprint",
            "command": "fixture",
            "secret_refs": {"TOKEN": {"provider": "from_file", "name": path}},
            "stdout_path": "stdout.log",
            "stderr_path": "stderr.log"
        }))
        .expect("bootstrap should decode");
        let resolved = resolved_environment(&bootstrap).expect("file secret should resolve");
        assert_eq!(resolved.values["TOKEN"], "top-secret");
        assert_eq!(resolved.secrets, vec![b"top-secret".to_vec()]);
        let _ = fs::remove_file(path);
    }
}
