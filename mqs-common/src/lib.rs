#![feature(unboxed_closures, fn_traits)]
#![warn(
    missing_docs,
    rust_2018_idioms,
    future_incompatible,
    missing_copy_implementations,
    trivial_numeric_casts,
    unsafe_code,
    unused,
    unused_qualifications,
    variant_size_differences
)]
#![cfg_attr(test, deny(warnings))]

//! Common definitions shared between an mqs server and client.

#[macro_use]
extern crate log;
#[macro_use]
extern crate serde_derive;
#[macro_use]
extern crate tokio;

use hyper::{body::HttpBody, header::HeaderName, Body, HeaderMap};
use uuid::Uuid;

/// Utilities to connect to the database.
#[cfg(feature = "server")]
pub mod connection;
/// Logging utils for mqs applications.
pub mod logger;
/// Encoding and decoding of multipart/mixed messages.
pub mod multipart;
/// Request routing and handling.
pub mod router;
/// Run a server with the given handler method.
#[cfg(feature = "server")]
pub mod server;
mod status;
mod time;

pub use crate::time::*;
pub use status::*;

/// Content type used if the client does not specify one.
pub const DEFAULT_CONTENT_TYPE: &str = "application/octet-stream";

/// Header containing the message id.
#[derive(Clone, Copy)]
pub struct MessageIdHeader {}

impl MessageIdHeader {
    /// Get the name of the header containing the message id.
    ///
    /// ```
    /// use hyper::header::HeaderName;
    /// use mqs_common::MessageIdHeader;
    ///
    /// assert_eq!(
    ///     HeaderName::from_static("x-mqs-message-id"),
    ///     MessageIdHeader::name()
    /// );
    /// ```
    #[must_use]
    pub const fn name() -> HeaderName {
        HeaderName::from_static("x-mqs-message-id")
    }

    /// Get the message id from the headers or an empty string if no id was set.
    ///
    /// ```
    /// use http::HeaderValue;
    /// use hyper::HeaderMap;
    /// use mqs_common::MessageIdHeader;
    ///
    /// let mut headers = HeaderMap::new();
    /// assert_eq!(MessageIdHeader::get(&headers), "".to_string());
    /// headers.insert(
    ///     MessageIdHeader::name(),
    ///     HeaderValue::from_static("my value"),
    /// );
    /// assert_eq!(MessageIdHeader::get(&headers), "my value".to_string());
    /// ```
    #[must_use]
    pub fn get(headers: &HeaderMap) -> String {
        get_header(headers, Self::name()).map_or_else(String::new, ToString::to_string)
    }
}

/// Header containing the number of times a message was already received.
#[derive(Clone, Copy)]
pub struct MessageReceivesHeader {}

impl MessageReceivesHeader {
    /// Get the name of the header containing the number of message receives.
    ///
    /// ```
    /// use hyper::header::HeaderName;
    /// use mqs_common::MessageReceivesHeader;
    ///
    /// assert_eq!(
    ///     HeaderName::from_static("x-mqs-message-receives"),
    ///     MessageReceivesHeader::name()
    /// );
    /// ```
    #[must_use]
    pub const fn name() -> HeaderName {
        HeaderName::from_static("x-mqs-message-receives")
    }

    /// Get the number of times a message was already received.
    /// Returns 0 in case the header is missing or contains an invalid value.
    ///
    /// ```
    /// use http::HeaderValue;
    /// use hyper::HeaderMap;
    /// use mqs_common::MessageReceivesHeader;
    ///
    /// let mut headers = HeaderMap::new();
    /// assert_eq!(MessageReceivesHeader::get(&headers), 0);
    /// headers.insert(
    ///     MessageReceivesHeader::name(),
    ///     HeaderValue::from_static("not a number"),
    /// );
    /// assert_eq!(MessageReceivesHeader::get(&headers), 0);
    /// headers.insert(
    ///     MessageReceivesHeader::name(),
    ///     HeaderValue::from_static("42"),
    /// );
    /// assert_eq!(MessageReceivesHeader::get(&headers), 42);
    /// ```
    #[must_use]
    pub fn get(headers: &HeaderMap) -> i32 {
        get_header(headers, Self::name()).map_or_else(|| 0, |s| s.parse().unwrap_or(0))
    }
}

/// Header containing the time the message was published at.
#[derive(Clone, Copy)]
pub struct PublishedAtHeader {}

impl PublishedAtHeader {
    /// Get the name of the header containing the time the message was published at.
    ///
    /// ```
    /// use hyper::header::HeaderName;
    /// use mqs_common::PublishedAtHeader;
    ///
    /// assert_eq!(
    ///     HeaderName::from_static("x-mqs-message-published-at"),
    ///     PublishedAtHeader::name()
    /// );
    /// ```
    #[must_use]
    pub const fn name() -> HeaderName {
        HeaderName::from_static("x-mqs-message-published-at")
    }

    /// Get the time a message was originally published at.
    /// Returns 1970-01-01T00:00:00Z in case the header is missing or contains an invalid value.
    ///
    /// ```
    /// use http::HeaderValue;
    /// use hyper::HeaderMap;
    /// use mqs_common::{PublishedAtHeader, UtcTime};
    ///
    /// let mut headers = HeaderMap::new();
    /// assert_eq!(
    ///     PublishedAtHeader::get(&headers),
    ///     PublishedAtHeader::default()
    /// );
    /// headers.insert(
    ///     PublishedAtHeader::name(),
    ///     HeaderValue::from_static("today is not a valid date"),
    /// );
    /// assert_eq!(
    ///     PublishedAtHeader::get(&headers),
    ///     PublishedAtHeader::default()
    /// );
    /// headers.insert(
    ///     PublishedAtHeader::name(),
    ///     HeaderValue::from_static("1984-04-04T00:00:00Z"),
    /// );
    /// let expected = UtcTime::from_timestamp(449884800);
    /// assert_eq!(PublishedAtHeader::get(&headers), expected);
    /// ```
    #[must_use]
    pub fn get(headers: &HeaderMap) -> UtcTime {
        get_header(headers, Self::name()).map_or_else(Self::default, |s| {
            UtcTime::parse_from_rfc3339(s).unwrap_or_else(|_| Self::default())
        })
    }

    /// The default value if we can't find or parse the header.
    /// Returns the unix epoch at 1970-01-01T00:00:00Z.
    ///
    /// ```
    /// use mqs_common::PublishedAtHeader;
    ///
    /// assert_eq!(
    ///     "1970-01-01T00:00:00Z",
    ///     PublishedAtHeader::default().to_rfc3339()
    /// );
    /// ```
    #[must_use]
    pub fn default() -> UtcTime {
        UtcTime::from_timestamp(0)
    }
}

/// Header containing the time the message got visible again.
#[derive(Clone, Copy)]
pub struct VisibleAtHeader {}

impl VisibleAtHeader {
    /// Get the name of the header containing the time the message got visible again.
    ///
    /// ```
    /// use hyper::header::HeaderName;
    /// use mqs_common::VisibleAtHeader;
    ///
    /// assert_eq!(
    ///     HeaderName::from_static("x-mqs-message-visible-at"),
    ///     VisibleAtHeader::name()
    /// );
    /// ```
    #[must_use]
    pub const fn name() -> HeaderName {
        HeaderName::from_static("x-mqs-message-visible-at")
    }

    /// Get the time a message got visible again.
    /// Returns 1970-01-01T00:00:00Z in case the header is missing or contains an invalid value.
    ///
    /// ```
    /// use http::HeaderValue;
    /// use hyper::HeaderMap;
    /// use mqs_common::{UtcTime, VisibleAtHeader};
    ///
    /// let mut headers = HeaderMap::new();
    /// assert_eq!(VisibleAtHeader::get(&headers), VisibleAtHeader::default());
    /// headers.insert(
    ///     VisibleAtHeader::name(),
    ///     HeaderValue::from_static("today is not a valid date"),
    /// );
    /// assert_eq!(VisibleAtHeader::get(&headers), VisibleAtHeader::default());
    /// headers.insert(
    ///     VisibleAtHeader::name(),
    ///     HeaderValue::from_static("1984-04-04T00:00:00Z"),
    /// );
    /// let expected = UtcTime::from_timestamp(449884800);
    /// assert_eq!(VisibleAtHeader::get(&headers), expected);
    /// ```
    #[must_use]
    pub fn get(headers: &HeaderMap) -> UtcTime {
        get_header(headers, Self::name()).map_or_else(Self::default, |s| {
            UtcTime::parse_from_rfc3339(s).unwrap_or_else(|_| Self::default())
        })
    }

    /// The default value if we can't find or parse the header.
    /// Returns the unix epoch at 1970-01-01T00:00:00Z.
    ///
    /// ```
    /// use mqs_common::VisibleAtHeader;
    ///
    /// assert_eq!(
    ///     "1970-01-01T00:00:00Z",
    ///     VisibleAtHeader::default().to_rfc3339()
    /// );
    /// ```
    #[must_use]
    pub fn default() -> UtcTime {
        UtcTime::from_timestamp(0)
    }
}

/// Header containing the trace id.
#[derive(Clone, Copy)]
pub struct TraceIdHeader {}

impl TraceIdHeader {
    /// Get the name of the header containing the trace id.
    ///
    /// ```
    /// use hyper::header::HeaderName;
    /// use mqs_common::TraceIdHeader;
    ///
    /// assert_eq!(HeaderName::from_static("x-trace-id"), TraceIdHeader::name());
    /// ```
    #[must_use]
    pub const fn name() -> HeaderName {
        HeaderName::from_static("x-trace-id")
    }

    /// Get a string representation of the header containing the trace id.
    ///
    /// ```
    /// use mqs_common::TraceIdHeader;
    ///
    /// assert_eq!("X-TRACE-ID", &TraceIdHeader::upper());
    /// ```
    #[must_use]
    pub fn upper() -> String {
        Self::name().as_str().to_uppercase()
    }

    /// Get the trace id header value.
    ///
    /// ```
    /// use hyper::{header::HeaderValue, HeaderMap};
    /// use mqs_common::TraceIdHeader;
    ///
    /// let mut headers = HeaderMap::new();
    /// assert_eq!(TraceIdHeader::get(&headers), None);
    /// headers.insert(
    ///     TraceIdHeader::name(),
    ///     HeaderValue::from_static("2e372a3a-9dff-4c61-8678-753bbdf4295e"),
    /// );
    /// assert_eq!(
    ///     TraceIdHeader::get(&headers),
    ///     Some("2e372a3a-9dff-4c61-8678-753bbdf4295e".parse().unwrap())
    /// );
    /// ```
    #[must_use]
    pub fn get(headers: &HeaderMap) -> Option<Uuid> {
        get_header(headers, Self::name()).map_or_else(|| None, |s| Uuid::parse_str(s).map_or_else(|_| None, Some))
    }
}

/// Get a single header and convert it to a string.
///
/// ```
/// use hyper::{
///     header::{HeaderValue, CONTENT_TYPE},
///     HeaderMap,
/// };
/// use mqs_common::get_header;
///
/// let mut headers = HeaderMap::new();
/// assert_eq!(get_header(&headers, CONTENT_TYPE), None);
/// headers.insert(CONTENT_TYPE, HeaderValue::from_static("text/plain"));
/// assert_eq!(get_header(&headers, CONTENT_TYPE), Some("text/plain"));
/// ```
pub fn get_header(headers: &HeaderMap, header: HeaderName) -> Option<&str> {
    headers
        .get(header)
        .map_or_else(|| None, |v| v.to_str().map_or_else(|_| None, Some))
}

/// Queue configuration send to the server by the client.
#[derive(Serialize, Deserialize, Debug, PartialEq, Eq, PartialOrd, Ord, Clone)]
pub struct QueueConfig {
    /// Redrive policy of the queue.
    pub redrive_policy:        Option<QueueRedrivePolicy>,
    /// Number of seconds until a message will no longer be returned.
    pub retention_timeout:     i64,
    /// Number of seconds a message will be hidden after it was received.
    pub visibility_timeout:    i64,
    /// Number of seconds a message will be hidden after it was published.
    pub message_delay:         i64,
    /// Whether duplicate messages in a queue will be dropped.
    pub message_deduplication: bool,
}

/// Queue description returned from the server.
#[derive(Serialize, Deserialize, Debug, PartialEq, Eq, PartialOrd, Ord)]
pub struct QueueDescriptionOutput {
    /// Name of the queue.
    pub name:                  String,
    /// Redrive policy of the queue.
    pub redrive_policy:        Option<QueueRedrivePolicy>,
    /// Number of seconds a message will be hidden after it was received.
    pub retention_timeout:     i64,
    /// Number of seconds a message will be hidden after it was received.
    pub visibility_timeout:    i64,
    /// Number of seconds a message will be hidden after it was published.
    pub message_delay:         i64,
    /// Whether duplicate messages in a queue will be dropped.
    pub message_deduplication: bool,
    /// Information about messages currently in the queue.
    pub status:                QueueStatus,
}

/// Redrive policy of a queue.
#[derive(Serialize, Deserialize, Debug, PartialEq, Eq, PartialOrd, Ord, Clone)]
pub struct QueueRedrivePolicy {
    /// Number of receives after which a message will be moved to the dead letter queue.
    pub max_receives:      i32,
    /// Name of the dead letter queue.
    pub dead_letter_queue: String,
}

/// Description of the current status of a queue.
#[derive(Serialize, Deserialize, Debug, PartialEq, Eq, PartialOrd, Ord, Clone, Copy)]
pub struct QueueStatus {
    /// Number of messages currently in the queue.
    pub messages:           i64,
    /// Number of messages which can currently be received in the queue.
    pub visible_messages:   i64,
    /// Age in seconds of the oldest message in the queue.
    pub oldest_message_age: u64,
}

/// Queue configuration as returned by the server.
#[derive(Serialize, Deserialize, PartialEq, Eq, PartialOrd, Ord, Debug)]
pub struct QueueConfigOutput {
    /// Name of the queue.
    pub name:                  String,
    /// Redrive policy of the queue.
    pub redrive_policy:        Option<QueueRedrivePolicy>,
    /// Number of seconds a message will be hidden after it was received.
    pub retention_timeout:     i64,
    /// Number of seconds a message will be hidden after it was received.
    pub visibility_timeout:    i64,
    /// Number of seconds a message will be hidden after it was published.
    pub message_delay:         i64,
    /// Whether duplicate messages in a queue will be dropped.
    pub message_deduplication: bool,
}

// Hack to get clippy to shut up about about possible constant functions for into_description.
// See: https://github.com/rust-lang/rust-clippy/issues/4979
impl Drop for QueueConfigOutput {
    fn drop(&mut self) {}
}

// replace the referenced value with the default value and return the referenced value.
fn extract<T: Default>(a: &mut T) -> T {
    let mut b = T::default();
    std::mem::swap(a, &mut b);

    b
}

impl QueueConfigOutput {
    /// Convert a `QueueConfigOutput` into a `QueueDescriptionOutput`. We can't use `From` for this
    /// as we need to provide some additional parameters.
    ///
    /// ```
    /// use mqs_common::{QueueConfigOutput, QueueDescriptionOutput, QueueRedrivePolicy, QueueStatus};
    ///
    /// let output = QueueConfigOutput {
    ///     name:                  "queue".to_string(),
    ///     redrive_policy:        Some(QueueRedrivePolicy {
    ///         max_receives:      5,
    ///         dead_letter_queue: "queue-dead".to_string(),
    ///     }),
    ///     retention_timeout:     3600,
    ///     visibility_timeout:    30,
    ///     message_delay:         0,
    ///     message_deduplication: true,
    /// };
    /// let description = output.into_description(10, 3, 50);
    /// assert_eq!(description, QueueDescriptionOutput {
    ///     name:                  "queue".to_string(),
    ///     redrive_policy:        Some(QueueRedrivePolicy {
    ///         max_receives:      5,
    ///         dead_letter_queue: "queue-dead".to_string(),
    ///     }),
    ///     retention_timeout:     3600,
    ///     visibility_timeout:    30,
    ///     message_delay:         0,
    ///     message_deduplication: true,
    ///     status:                QueueStatus {
    ///         messages:           10,
    ///         visible_messages:   3,
    ///         oldest_message_age: 50,
    ///     },
    /// });
    /// ```
    #[must_use]
    pub fn into_description(
        mut self,
        messages: i64,
        visible_messages: i64,
        oldest_message_age: u64,
    ) -> QueueDescriptionOutput {
        QueueDescriptionOutput {
            name:                  extract(&mut self.name),
            redrive_policy:        extract(&mut self.redrive_policy),
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

/// Response for a queue list request.
#[derive(Serialize, Deserialize, Debug)]
pub struct QueuesResponse {
    /// List of queues, might be less than `total` if limit or offset was set.
    pub queues: Vec<QueueConfigOutput>,
    /// Total number of queues known to the server.
    pub total:  i64,
}

/// Read a request or response body into a vector. If `max_size` is set, no more than this number of bytes will be read.
/// If more bytes would need to be read, `None` is returned insted of the body.
///
/// ```
/// use hyper::{Body, Error};
/// use mqs_common::read_body;
///
/// async fn example(body: &mut Body) -> Result<(), Error> {
///     match read_body(body, Some(1024)).await? {
///         None => println!("More than 1024 bytes were received"),
///         Some(data) => println!("{} / 1024 bytes were read", data.len()),
///     }
///     Ok(())
/// }
/// ```
///
/// # Errors
///
/// If reading any chunk returns an error.
pub async fn read_body(body: &mut Body, max_size: Option<usize>) -> Result<Option<Vec<u8>>, hyper::Error> {
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
        result.extend_from_slice(chunk.as_ref());
    }

    Ok(Some(result))
}

/// Test utilities for client and server parts as well as some tests for this module.
pub mod test {
    #[cfg(test)]
    use http::header::HeaderName;
    use hyper::Body;
    #[cfg(test)]
    use hyper::HeaderMap;
    use tokio::runtime::{Builder, Runtime};

    #[cfg(test)]
    use crate::{
        get_header,
        QueueConfigOutput,
        QueueDescriptionOutput,
        QueueRedrivePolicy,
        QueueStatus,
        TraceIdHeader,
    };

    /// Create a new tokio runtime to use in tests.
    ///
    /// ```
    /// use mqs_common::test::make_runtime;
    ///
    /// let mut done = false;
    /// make_runtime().block_on(async {
    ///     done = true;
    /// });
    /// assert!(done);
    /// ```
    #[must_use]
    pub fn make_runtime() -> Runtime {
        Builder::new_current_thread().enable_all().build().unwrap()
    }

    /// Read some body returned in a test. The body is read synchronously, so don't use this
    /// outside of test code.
    ///
    /// ```
    /// use hyper::Body;
    /// use mqs_common::test::read_body;
    ///
    /// let mut body = Body::from("some body");
    /// let read = read_body(&mut body);
    /// assert_eq!(read.as_slice(), b"some body");
    /// ```
    pub fn read_body(body: &mut Body) -> Vec<u8> {
        make_runtime().block_on(async { crate::read_body(body, None).await.unwrap().unwrap() })
    }

    #[test]
    async fn read_big_body() {
        let read = crate::read_body(&mut Body::from("this is too large".to_string()), Some(5))
            .await
            .unwrap();
        assert_eq!(read, None);
        let read = crate::read_body(&mut Body::from("this is ok".to_string()), Some(50))
            .await
            .unwrap();
        assert!(read.is_some());
        assert_eq!(read.unwrap().as_slice(), b"this is ok");
    }

    #[test]
    async fn into_description() {
        let output = QueueConfigOutput {
            name:                  "queue".to_string(),
            redrive_policy:        Some(QueueRedrivePolicy {
                max_receives:      5,
                dead_letter_queue: "queue-dead".to_string(),
            }),
            retention_timeout:     3600,
            visibility_timeout:    30,
            message_delay:         0,
            message_deduplication: true,
        };
        let description = output.into_description(10, 3, 50);
        assert_eq!(description, QueueDescriptionOutput {
            name:                  "queue".to_string(),
            redrive_policy:        Some(QueueRedrivePolicy {
                max_receives:      5,
                dead_letter_queue: "queue-dead".to_string(),
            }),
            retention_timeout:     3600,
            visibility_timeout:    30,
            message_delay:         0,
            message_deduplication: true,
            status:                QueueStatus {
                messages:           10,
                visible_messages:   3,
                oldest_message_age: 50,
            },
        });
    }

    #[test]
    async fn get_headers() {
        use hyper::header::{HeaderValue, CONTENT_TYPE};

        let mut headers = HeaderMap::new();
        assert_eq!(get_header(&headers, CONTENT_TYPE), None);
        headers.insert(CONTENT_TYPE, HeaderValue::from_static("text/plain"));
        assert_eq!(get_header(&headers, CONTENT_TYPE), Some("text/plain"));
    }

    #[test]
    async fn test_trace_id_header() {
        use hyper::header::HeaderValue;

        assert_eq!(HeaderName::from_static("x-trace-id"), TraceIdHeader::name());
        assert_eq!("X-TRACE-ID", &TraceIdHeader::upper());

        let mut headers = HeaderMap::new();
        assert_eq!(TraceIdHeader::get(&headers), None);
        headers.insert(
            TraceIdHeader::name(),
            HeaderValue::from_static("2e372a3a-9dff-4c61-8678-753bbdf4295e"),
        );
        assert_eq!(
            TraceIdHeader::get(&headers),
            Some("2e372a3a-9dff-4c61-8678-753bbdf4295e".parse().unwrap())
        );
    }
}
