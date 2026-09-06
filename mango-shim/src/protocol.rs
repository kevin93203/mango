use serde::{Deserialize, Serialize};
use serde_json::Value;

pub const PROTOCOL_VERSION: u32 = 1;

#[derive(Debug, Deserialize)]
pub struct Request {
    pub version: u32,
    #[serde(rename = "request_id")]
    pub request_id: String,
    pub method: String,
    #[serde(default)]
    pub params: Value,
}

#[derive(Debug, Serialize)]
pub struct Response<T: Serialize> {
    pub version: u32,
    #[serde(rename = "request_id")]
    pub request_id: String,
    pub ok: bool,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub data: Option<T>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub error: Option<ErrorBody>,
}

#[derive(Debug, Serialize)]
pub struct ErrorBody {
    pub code: String,
    pub message: String,
}

pub fn success<T: Serialize>(request: &Request, data: T) -> Response<T> {
    Response {
        version: PROTOCOL_VERSION,
        request_id: request.request_id.clone(),
        ok: true,
        data: Some(data),
        error: None,
    }
}

pub fn failure<T: Serialize>(
    request: &Request,
    code: impl Into<String>,
    message: impl Into<String>,
) -> Response<T> {
    Response {
        version: PROTOCOL_VERSION,
        request_id: request.request_id.clone(),
        ok: false,
        data: None,
        error: Some(ErrorBody {
            code: code.into(),
            message: message.into(),
        }),
    }
}

#[derive(Debug, Deserialize)]
pub struct HelloParams {
    pub instance_id: Option<String>,
    pub service_key: Option<String>,
    pub config_fingerprint: Option<String>,
}

#[derive(Debug, Deserialize)]
pub struct StopParams {
    #[serde(default = "default_timeout")]
    pub timeout_ms: u64,
}

fn default_timeout() -> u64 {
    10_000
}

#[derive(Debug, Serialize)]
pub struct HelloResponse {
    pub protocol_version: u32,
    pub capabilities: Vec<&'static str>,
    pub instance_id: String,
    pub service_key: String,
    pub config_fingerprint: String,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn request_accepts_unknown_fields_and_defaults_params() {
        let request: Request = serde_json::from_str(
            r#"{"version":1,"request_id":"r1","method":"status","extra":true}"#,
        )
        .expect("request should decode");
        assert_eq!(request.request_id, "r1");
        assert_eq!(request.method, "status");
        assert!(request.params.is_null());
    }

    #[test]
    fn failure_contains_protocol_error_code() {
        let request: Request =
            serde_json::from_str(r#"{"version":1,"request_id":"r2","method":"hello","params":{}}"#)
                .expect("request should decode");
        let response = failure::<Value>(&request, "CONFIG_MISMATCH", "fingerprint differs");
        assert!(!response.ok);
        assert_eq!(response.error.expect("error body").code, "CONFIG_MISMATCH");
    }
}
