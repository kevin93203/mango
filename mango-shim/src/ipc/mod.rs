use crate::protocol::{PROTOCOL_VERSION, Request, Response, failure};
use serde::Serialize;
use serde_json::Value;
use std::io::{BufRead, BufReader, Write};
use std::sync::{Arc, atomic::AtomicBool};

#[cfg(unix)]
mod unix;
#[cfg(windows)]
mod windows;

#[cfg(unix)]
pub use unix::serve;
#[cfg(windows)]
pub use windows::serve;

pub type Handler = dyn Fn(&Request) -> Response<Value> + Send + Sync + 'static;

pub fn handle_connection<S: ReadWrite>(mut stream: S, handler: &Arc<Handler>) {
    let mut line = String::new();
    let mut reader = BufReader::new(&mut stream);
    let result = reader.read_line(&mut line);
    drop(reader);
    let response = match result {
        Ok(0) => return,
        Ok(_) => match serde_json::from_str::<Request>(&line) {
            Ok(request) if request.version == 0 || request.version == PROTOCOL_VERSION => {
                handler(&request)
            }
            Ok(request) => failure::<Value>(
                &request,
                "UNSUPPORTED_VERSION",
                format!("unsupported protocol version {}", request.version),
            ),
            Err(error) => Response {
                version: PROTOCOL_VERSION,
                request_id: String::new(),
                ok: false,
                data: None,
                error: Some(crate::protocol::ErrorBody {
                    code: "BAD_REQUEST".to_string(),
                    message: error.to_string(),
                }),
            },
        },
        Err(error) => Response {
            version: PROTOCOL_VERSION,
            request_id: String::new(),
            ok: false,
            data: None,
            error: Some(crate::protocol::ErrorBody {
                code: "BAD_REQUEST".to_string(),
                message: error.to_string(),
            }),
        },
    };
    let _ = serde_json::to_writer(&mut stream, &response);
    let _ = stream.write_all(b"\n");
    let _ = stream.flush();
}

pub trait ReadWrite: std::io::Read + Write + Send + 'static {}
impl<T: std::io::Read + Write + Send + 'static> ReadWrite for T {}

#[allow(dead_code)]
fn _keep_serialize_bound<T: Serialize>(_value: &T) {}

#[allow(dead_code)]
fn _keep_atomic_bound(_value: &Arc<AtomicBool>) {}
