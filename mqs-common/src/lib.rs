#![feature(unboxed_closures, fn_traits)]

#[macro_use]
extern crate log;
#[macro_use]
extern crate serde_derive;
#[macro_use]
extern crate tokio;

use hyper::{
    body::{Buf, HttpBody},
    header::HeaderName,
    Body,
};

pub mod logger;
pub mod multipart;
pub mod router;
pub mod status;

pub const DEFAULT_CONTENT_TYPE: &'static str = "application/octet-stream";
pub const TRACE_ID_HEADER: TraceIdHeader = TraceIdHeader {};

pub struct TraceIdHeader {}

impl TraceIdHeader {
    pub fn name(&self) -> HeaderName {
        HeaderName::from_static("x-trace-id")
    }

    pub fn upper(&self) -> String {
        self.name().as_str().to_uppercase()
    }
}

#[derive(Serialize, Deserialize, Debug)]
pub struct QueueConfig {
    pub redrive_policy:        Option<QueueRedrivePolicy>,
    pub retention_timeout:     i64,
    pub visibility_timeout:    i64,
    pub message_delay:         i64,
    pub message_deduplication: bool,
}

#[derive(Serialize, Deserialize, Debug)]
pub struct QueueDescriptionOutput {
    pub name:                  String,
    pub redrive_policy:        Option<QueueRedrivePolicy>,
    pub retention_timeout:     i64,
    pub visibility_timeout:    i64,
    pub message_delay:         i64,
    pub message_deduplication: bool,
    pub status:                QueueStatus,
}

#[derive(Serialize, Deserialize, Debug)]
pub struct QueueRedrivePolicy {
    pub max_receives:      i32,
    pub dead_letter_queue: String,
}

#[derive(Serialize, Deserialize, Debug)]
pub struct QueueStatus {
    pub messages:           i64,
    pub visible_messages:   i64,
    pub oldest_message_age: i64,
}

#[derive(Serialize, Deserialize, Debug)]
pub struct QueueConfigOutput {
    pub name:                  String,
    pub redrive_policy:        Option<QueueRedrivePolicy>,
    pub retention_timeout:     i64,
    pub visibility_timeout:    i64,
    pub message_delay:         i64,
    pub message_deduplication: bool,
}

impl QueueConfigOutput {
    pub fn into_description(
        self,
        messages: i64,
        visible_messages: i64,
        oldest_message_age: i64,
    ) -> QueueDescriptionOutput {
        QueueDescriptionOutput {
            name:                  self.name,
            redrive_policy:        self.redrive_policy,
            retention_timeout:     self.retention_timeout,
            visibility_timeout:    self.visibility_timeout,
            message_delay:         self.message_delay,
            message_deduplication: self.message_deduplication,
            status:                QueueStatus {
                messages,
                visible_messages,
                oldest_message_age,
            },
        }
    }
}

#[derive(Serialize, Deserialize, Debug)]
pub struct QueuesResponse {
    pub queues: Vec<QueueConfigOutput>,
    pub total:  i64,
}

pub async fn read_body(body: &mut Body, max_size: Option<usize>) -> Result<Option<Vec<u8>>, hyper::error::Error> {
    let mut chunks = Vec::new();
    let mut total_length = 0;

    while let Some(chunk) = body.data().await {
        let bytes = chunk?;
        total_length += bytes.len();
        if let Some(max_length) = max_size {
            if total_length > max_length {
                return Ok(None);
            }
        }
        chunks.push(bytes);
    }

    let mut result = Vec::with_capacity(total_length);

    for chunk in chunks {
        result.extend_from_slice(chunk.bytes());
    }

    Ok(Some(result))
}

pub mod test {
    use hyper::Body;
    use tokio::runtime::{Builder, Runtime};

    pub fn make_runtime() -> Runtime {
        Builder::new().enable_all().basic_scheduler().build().unwrap()
    }

    pub fn read_body(body: &mut Body) -> Vec<u8> {
        let mut rt = Builder::new().enable_all().basic_scheduler().build().unwrap();

        rt.block_on(async { crate::read_body(body, None).await.unwrap().unwrap() })
    }
}
