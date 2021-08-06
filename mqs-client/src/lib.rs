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

//! Client library for mqs servers
//!
//! # Overview
//!
//! The main type of this crate is `Service`. A service is a configured client to talk directly with
//! an mqs server, manage queues, or publish, receive, and delete messages.
//!
//! ## Example
//!
//! You have to use tokio or a similar library as this library makes extensive use of futures to
//! run the different methods provided by `Service`. To see if for example an mqs server is running
//! on some host, you could write the following:
//!
//! ```
//! use mqs_client::Service;
//! use tokio::runtime::Builder;
//!
//! let service = Service::new("https://mqs.example.com:7843");
//!
//! let mut rt = Builder::new_multi_thread().enable_all().build().unwrap();
//! let success = rt.block_on(async { service.check_health().await });
//! assert!(!success.is_ok());
//! ```

use hyper::{
    client::{Client, HttpConnector},
    header::{HeaderName, HeaderValue, CONNECTION, CONTENT_ENCODING, CONTENT_TYPE},
    Body,
    HeaderMap,
    Method,
    Request,
    Response,
    StatusCode,
};
use mqs_common::{
    multipart,
    read_body,
    QueueConfig,
    QueueDescriptionOutput,
    QueuesResponse,
    Status::ServiceUnavailable,
    TraceIdHeader,
    DEFAULT_CONTENT_TYPE,
};
use serde::{de::DeserializeOwned, Serialize};
use std::{
    error::Error,
    fmt::{Display, Formatter},
};
use uuid::Uuid;

/// If something goes wrong, we return an instance of `ClientError` to tell you what exactly failed
/// during the operation.
#[derive(Debug)]
pub enum ClientError {
    /// Hyper returned some error
    HyperError(hyper::Error),
    /// An invalid URI was provided
    InvalidUri(hyper::http::uri::InvalidUri),
    /// An I/O error occurred.
    IoError(std::io::Error),
    /// A JSON response failed to parse.
    ParseError(serde_json::error::Error),
    /// A value could not converted to a header value because it contained invalid characters.
    InvalidHeaderValue(hyper::header::InvalidHeaderValue),
    /// The server returned an invalid multipart response.
    MultipartParseError(multipart::InvalidMultipart),
    /// The server returned an error status code.
    ServiceError(u16),
    /// The response returned by the server was larger than what the client was configured to accept.
    TooLargeResponse,
    /// The server returned an invalid health check response.
    HealthCheckError,
}

impl Display for ClientError {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "{:?}", self)
    }
}

impl Error for ClientError {}

impl From<hyper::Error> for ClientError {
    fn from(error: hyper::Error) -> Self {
        Self::HyperError(error)
    }
}

impl From<hyper::http::uri::InvalidUri> for ClientError {
    fn from(error: hyper::http::uri::InvalidUri) -> Self {
        Self::InvalidUri(error)
    }
}

impl From<std::io::Error> for ClientError {
    fn from(error: std::io::Error) -> Self {
        Self::IoError(error)
    }
}

impl From<serde_json::error::Error> for ClientError {
    fn from(error: serde_json::error::Error) -> Self {
        Self::ParseError(error)
    }
}

impl From<hyper::header::InvalidHeaderValue> for ClientError {
    fn from(error: hyper::header::InvalidHeaderValue) -> Self {
        Self::InvalidHeaderValue(error)
    }
}

impl From<multipart::InvalidMultipart> for ClientError {
    fn from(error: multipart::InvalidMultipart) -> Self {
        Self::MultipartParseError(error)
    }
}

/// A `Service` allows you to speak to a single mqs server.
pub struct Service {
    client:        Client<HttpConnector>,
    host:          String,
    max_body_size: Option<usize>,
}

/// A `PublishableMessage` contains all information a message can contain.
#[derive(Clone)]
pub struct PublishableMessage<'a> {
    /// Content type of the message.
    pub content_type:     &'a str,
    /// Content encoding of the message.
    pub content_encoding: Option<&'a str>,
    /// Trace id of the message. You can use this to attach a unique identifier to a request and
    /// later recover this identifier upon message consumption.
    pub trace_id:         Option<Uuid>,
    /// Encoded body of the message.
    pub message:          Vec<u8>,
}

impl<'a> PublishableMessage<'a> {
    fn encode(self) -> (HeaderMap, Vec<u8>) {
        let mut headers = HeaderMap::new();

        if let Ok(content_type) = HeaderValue::from_str(self.content_type) {
            headers.insert(CONTENT_TYPE, content_type);
        }

        if let Some(content_encoding) = self.content_encoding {
            if let Ok(content_encoding) = HeaderValue::from_str(content_encoding) {
                headers.insert(CONTENT_ENCODING, content_encoding);
            }
        }

        if let Some(trace_id) = self.trace_id {
            if let Ok(trace_id) = HeaderValue::from_str(&trace_id.to_string()) {
                headers.insert(TraceIdHeader::name(), trace_id);
            }
        }

        (headers, self.message)
    }
}

/// A `MessageResponse` contains the same information as a `PublishableMessage` plus the id of the message.
#[derive(Debug)]
pub struct MessageResponse {
    /// Id of the message. Needed to later delete the message so it will not be received again later.
    pub message_id:       String,
    /// Content type of the message.
    pub content_type:     String,
    /// Content encoding of the message.
    pub content_encoding: Option<String>,
    /// Trace id of the message.
    pub trace_id:         Option<Uuid>,
    /// Encoded body of the message.
    pub content:          Vec<u8>,
}

impl Service {
    const DEFAULT_MAX_BODY_SIZE: usize = 5 * 1024 * 1024;

    /// Create a new instance.
    ///
    /// ```
    /// use mqs_client::Service;
    ///
    /// let _service = Service::new("https://mqs.example.com:7843");
    /// ```
    #[must_use]
    pub fn new(host: &str) -> Self {
        Self {
            client:        Client::new(),
            host:          host.to_string(),
            max_body_size: Some(Self::DEFAULT_MAX_BODY_SIZE),
        }
    }

    /// Configure the maximum body size we are prepared to accept. Should the server return a bigger
    /// response, we return an error and drop the response instead of reading the whole response into
    /// memory.
    ///
    /// ```
    /// use mqs_client::Service;
    ///
    /// let mut service = Service::new("https://mqs.example.com:7843");
    /// // allow at most 64 KiB
    /// service.set_max_body_size(Some(1024 * 64));
    /// // allow unlimited responses
    /// service.set_max_body_size(None);
    /// ```
    pub fn set_max_body_size(&mut self, max_body_size: Option<usize>) -> &mut Self {
        self.max_body_size = max_body_size;
        self
    }

    fn new_request(
        method: Method,
        uri: &str,
        trace_id: Option<Uuid>,
        body: Body,
    ) -> Result<Request<Body>, hyper::http::uri::InvalidUri> {
        let mut req = Request::new(body);
        *req.uri_mut() = uri.parse()?;
        *req.method_mut() = method;
        req.headers_mut()
            .insert(CONNECTION, HeaderValue::from_static("keep-alive"));
        if let Some(trace_id) = trace_id {
            if let Ok(value) = HeaderValue::from_str(&trace_id.to_string()) {
                req.headers_mut().insert(TraceIdHeader::name(), value);
            }
        }
        Ok(req)
    }

    async fn parse_response_maybe<T: DeserializeOwned>(
        &self,
        mut response: Response<Body>,
        success_status: u16,
        error_status: u16,
    ) -> Result<Option<T>, ClientError> {
        let status = response.status().as_u16();
        if status == success_status {
            if let Some(body) = read_body(response.body_mut(), self.max_body_size).await? {
                let value = serde_json::from_slice(body.as_slice())?;
                Ok(Some(value))
            } else {
                Err(ClientError::TooLargeResponse)
            }
        } else if status == error_status {
            Ok(None)
        } else {
            Err(ClientError::ServiceError(status))
        }
    }

    async fn request<E: Send, F: Sync + Send + Fn() -> Result<Request<Body>, E>>(
        &self,
        builder: F,
    ) -> Result<Response<Body>, ClientError>
    where
        ClientError: From<E>,
    {
        loop {
            let res = self.client.request(builder()?).await?;
            if res.status() != StatusCode::from(ServiceUnavailable) {
                return Ok(res);
            }
        }
    }

    async fn json_request<T: Serialize + Sync>(
        &self,
        method: Method,
        uri: &str,
        trace_id: Option<Uuid>,
        request: &T,
    ) -> Result<Response<Body>, ClientError> {
        self.request(|| {
            let message = serde_json::to_string(request)?;
            let mut req = Self::new_request(method.clone(), uri, trace_id, Body::from(message))?;
            req.headers_mut()
                .insert(CONTENT_TYPE, HeaderValue::from_static("application/json"));
            Ok::<_, ClientError>(req)
        })
        .await
    }

    /// Create a new queue with the given name and configuration.
    ///
    /// ```
    /// use mqs_client::{ClientError, Service};
    /// use mqs_common::{QueueConfig, QueueRedrivePolicy};
    ///
    /// // create a new queue named "new-queue". The queue will not delay new messages,
    /// // will hide messages for 30 seconds after they are received, delete messages older
    /// // than 1 hour and send messages to the queue "my-queue-dead" after 3 receives.
    /// async fn example(service: &Service) -> Result<Option<QueueConfig>, ClientError> {
    ///     service
    ///         .create_queue("new-queue", None, &QueueConfig {
    ///             redrive_policy:        Some(QueueRedrivePolicy {
    ///                 dead_letter_queue: "my-queue-dead".to_string(),
    ///                 max_receives:      3,
    ///             }),
    ///             retention_timeout:     3600,
    ///             visibility_timeout:    30,
    ///             message_delay:         0,
    ///             message_deduplication: true,
    ///         })
    ///         .await
    /// }
    /// ```
    ///
    /// # Errors
    ///
    /// Returns an error if the request fails or the server returns an invalid response.
    pub async fn create_queue(
        &self,
        queue_name: &str,
        trace_id: Option<Uuid>,
        config: &QueueConfig,
    ) -> Result<Option<QueueConfig>, ClientError> {
        let uri = format!("{}/queues/{}", &self.host, queue_name);
        let response = self.json_request(Method::PUT, &uri, trace_id, config).await?;
        self.parse_response_maybe(response, 201, 409).await
    }

    /// Update the configuration of a queue.
    ///
    /// ```
    /// use mqs_client::{ClientError, Service};
    /// use mqs_common::{QueueConfig, QueueRedrivePolicy};
    ///
    /// // update an existing queue named "existing-queue". The queue will not delay new messages,
    /// // will hide messages for 30 seconds after they are received, delete messages older
    /// // than 1 hour and send messages to the queue "my-queue-dead" after 3 receives.
    /// async fn example(service: &Service) -> Result<Option<QueueConfig>, ClientError> {
    ///     service
    ///         .update_queue("existing-queue", None, &QueueConfig {
    ///             redrive_policy:        Some(QueueRedrivePolicy {
    ///                 dead_letter_queue: "my-queue-dead".to_string(),
    ///                 max_receives:      3,
    ///             }),
    ///             retention_timeout:     3600,
    ///             visibility_timeout:    30,
    ///             message_delay:         0,
    ///             message_deduplication: true,
    ///         })
    ///         .await
    /// }
    /// ```
    ///
    /// # Errors
    ///
    /// Returns an error if the request fails or the server returns an invalid response.
    pub async fn update_queue(
        &self,
        queue_name: &str,
        trace_id: Option<Uuid>,
        config: &QueueConfig,
    ) -> Result<Option<QueueConfig>, ClientError> {
        let uri = format!("{}/queues/{}", &self.host, queue_name);
        let response = self.json_request(Method::POST, &uri, trace_id, config).await?;
        self.parse_response_maybe(response, 200, 404).await
    }

    /// Delete an existing queue. If the queue did exist, the configuration of the queue is returned, otherwise
    /// `None` is returned. All messages currently stored in the queue are also deleted.
    ///
    /// ```
    /// use mqs_client::{ClientError, Service};
    /// use mqs_common::QueueConfig;
    ///
    /// async fn example(service: &Service) -> Result<Option<QueueConfig>, ClientError> {
    ///     service.delete_queue("existing-queue", None).await
    /// }
    /// ```
    ///
    /// # Errors
    ///
    /// Returns an error if the request fails or the server returns an invalid response.
    pub async fn delete_queue(
        &self,
        queue_name: &str,
        trace_id: Option<Uuid>,
    ) -> Result<Option<QueueConfig>, ClientError> {
        let uri = format!("{}/queues/{}", &self.host, queue_name);
        let response = self
            .request(|| Self::new_request(Method::DELETE, &uri, trace_id, Body::default()))
            .await?;
        self.parse_response_maybe(response, 200, 404).await
    }

    /// Retrieve a list of all queues.
    ///
    /// ```
    /// use mqs_client::{ClientError, Service};
    /// use uuid::Uuid;
    ///
    /// // Delete all queues found on a server (including all messages).
    /// async fn delete_all_queues(service: &Service) -> Result<(), ClientError> {
    ///     // a new trace_id for this operation, we will group all operations under this id
    ///     // to easier see what belongs together in logs
    ///     let trace_id = Uuid::new_v4();
    ///
    ///     loop {
    ///         let queues = service.get_queues(Some(trace_id), None, Some(10)).await?;
    ///         for queue in queues.queues {
    ///             service.delete_queue(&queue.name, Some(trace_id)).await?;
    ///         }
    ///     }
    ///
    ///     Ok(())
    /// }
    /// ```
    ///
    /// # Errors
    ///
    /// Returns an error if the request fails or the server returns an invalid response.
    pub async fn get_queues(
        &self,
        trace_id: Option<Uuid>,
        offset: Option<usize>,
        limit: Option<usize>,
    ) -> Result<QueuesResponse, ClientError> {
        let uri = match (offset, limit) {
            (Some(offset), Some(limit)) => format!("{}/queues?offset={}&limit={}", &self.host, offset, limit),
            (Some(offset), None) => format!("{}/queues?offset={}", &self.host, offset),
            (None, Some(limit)) => format!("{}/queues?limit={}", &self.host, limit),
            (None, None) => format!("{}/queues", &self.host),
        };
        let mut response = self
            .request(|| Self::new_request(Method::GET, &uri, trace_id, Body::default()))
            .await?;
        match response.status().as_u16() {
            200 => {
                if let Some(body) = read_body(response.body_mut(), self.max_body_size).await? {
                    let value = serde_json::from_slice(body.as_slice())?;
                    Ok(value)
                } else {
                    Err(ClientError::TooLargeResponse)
                }
            },
            status => Err(ClientError::ServiceError(status)),
        }
    }

    /// Get information about a single queue.
    ///
    /// ```
    /// use mqs_client::{ClientError, Service};
    ///
    /// async fn queue_exists(service: &Service, queue_name: &str) -> Result<bool, ClientError> {
    ///     let description = service.describe_queue(queue_name, None).await?;
    ///
    ///     Ok(description.is_some())
    /// }
    ///
    /// async fn has_redrive_policy(service: &Service, queue_name: &str) -> Result<bool, ClientError> {
    ///     let description = service.describe_queue(queue_name, None).await?;
    ///
    ///     Ok(description.map_or_else(|| false, |description| description.redrive_policy.is_some()))
    /// }
    /// ```
    ///
    /// # Errors
    ///
    /// Returns an error if the request fails or the server returns an invalid response.
    pub async fn describe_queue(
        &self,
        queue_name: &str,
        trace_id: Option<Uuid>,
    ) -> Result<Option<QueueDescriptionOutput>, ClientError> {
        let uri = format!("{}/queues/{}", &self.host, queue_name);
        let response = self
            .request(|| Self::new_request(Method::GET, &uri, trace_id, Body::default()))
            .await?;
        self.parse_response_maybe(response, 200, 404).await
    }

    /// Receive a single message from a queue.
    ///
    /// ```
    /// use mqs_client::{ClientError, Service};
    ///
    /// async fn consume_one<F: FnOnce(String, Option<String>, Vec<u8>)>(
    ///     service: &Service,
    ///     queue_name: &str,
    ///     callback: F,
    /// ) -> Result<bool, ClientError> {
    ///     match service.get_message(queue_name, None).await? {
    ///         None => Ok(false),
    ///         Some(msg) => {
    ///             callback(msg.content_type, msg.content_encoding, msg.content);
    ///             service.delete_message(msg.trace_id, &msg.message_id).await?;
    ///             Ok(true)
    ///         },
    ///     }
    /// }
    /// ```
    ///
    /// # Errors
    ///
    /// Returns an error if the request fails or the server returns an invalid status.
    pub async fn get_message(
        &self,
        queue_name: &str,
        timeout: Option<u16>,
    ) -> Result<Option<MessageResponse>, ClientError> {
        let mut messages = self.get_messages(queue_name, 1, timeout).await?;
        Ok(messages.pop())
    }

    fn parse_message<F: FnOnce() -> Result<Vec<u8>, ClientError>>(
        headers: &HeaderMap,
        get_body: F,
    ) -> Result<MessageResponse, ClientError> {
        let message_id = headers
            .get("X-MQS-MESSAGE-ID")
            .map_or_else(|| "", |h| h.to_str().unwrap_or(""))
            .to_string();
        let content_type = headers
            .get(CONTENT_TYPE)
            .map_or_else(|| DEFAULT_CONTENT_TYPE, |h| h.to_str().unwrap_or(DEFAULT_CONTENT_TYPE))
            .to_string();
        let content_encoding = headers
            .get(CONTENT_ENCODING)
            .map_or_else(|| None, |h| h.to_str().map_or_else(|_| None, |s| Some(s.to_string())));
        let trace_id = TraceIdHeader::get(headers);
        let content = get_body()?;
        Ok(MessageResponse {
            message_id,
            content_type,
            content_encoding,
            trace_id,
            content,
        })
    }

    /// Receive one or more message from a queue.
    ///
    /// For example, to retrieve up to 20 messages, waiting up to 10 seconds,
    /// the following function could be used:
    ///
    /// ```
    /// use mqs_client::{ClientError, Service};
    ///
    /// async fn consume_multiple<F: Fn(String, Option<String>, Vec<u8>)>(
    ///     service: &Service,
    ///     queue_name: &str,
    ///     callback: F,
    /// ) -> Result<usize, ClientError> {
    ///     let mut count = 0;
    ///     for msg in service.get_messages(queue_name, 20, Some(10)).await? {
    ///         callback(msg.content_type, msg.content_encoding, msg.content);
    ///         service.delete_message(msg.trace_id, &msg.message_id).await?;
    ///         count += 1;
    ///     }
    ///
    ///     Ok(count)
    /// }
    /// ```
    ///
    /// # Errors
    ///
    /// Returns an error if the request fails or the server returns an invalid status.
    pub async fn get_messages(
        &self,
        queue_name: &str,
        limit: u16,
        timeout: Option<u16>,
    ) -> Result<Vec<MessageResponse>, ClientError> {
        if limit == 0 {
            return Ok(Vec::new());
        }

        let uri = format!("{}/messages/{}", &self.host, queue_name);
        let mut response = self
            .request(|| {
                let mut req = Self::new_request(Method::GET, &uri, None, Body::default())?;
                if let Ok(value) = HeaderValue::from_str(&format!("{}", limit)) {
                    req.headers_mut()
                        .insert(HeaderName::from_static("x-mqs-max-messages"), value);
                }
                if let Some(timeout) = timeout {
                    if let Ok(value) = HeaderValue::from_str(&format!("{}", timeout)) {
                        req.headers_mut()
                            .insert(HeaderName::from_static("x-mqs-max-wait-time"), value);
                    }
                }
                Ok::<_, ClientError>(req)
            })
            .await?;
        match response.status().as_u16() {
            200 => {
                let content_type = response
                    .headers()
                    .get(CONTENT_TYPE)
                    .map_or_else(|| DEFAULT_CONTENT_TYPE, |h| h.to_str().unwrap_or(DEFAULT_CONTENT_TYPE))
                    .to_string();
                if let Some(body) = read_body(response.body_mut(), self.max_body_size).await? {
                    if let Some(boundary) = multipart::is_multipart(&content_type) {
                        let chunks = multipart::parse(boundary.as_bytes(), body.as_slice())?;
                        let mut messages = Vec::with_capacity(chunks.len());
                        for (headers, message) in chunks {
                            messages.push(Self::parse_message(&headers, || Ok(message.to_vec()))?);
                        }
                        Ok(messages)
                    } else {
                        let message = Self::parse_message(response.headers(), || Ok(body))?;
                        Ok(vec![message])
                    }
                } else {
                    Err(ClientError::TooLargeResponse)
                }
            },
            204 => Ok(Vec::new()),
            status => Err(ClientError::ServiceError(status)),
        }
    }

    /// Publish a single message to a queue.
    ///
    /// ```
    /// use mqs_client::{ClientError, PublishableMessage, Service};
    ///
    /// async fn example(service: &Service) -> Result<bool, ClientError> {
    ///     let message = PublishableMessage {
    ///         trace_id:         None,
    ///         content_encoding: None,
    ///         content_type:     "application/json; encoding=utf-8",
    ///         message:          b"{}".to_vec(),
    ///     };
    ///
    ///     service.publish_message("my-queue", message).await
    /// }
    /// ```
    ///
    /// # Errors
    ///
    /// Returns an error if the request fails or the server returns an invalid status.
    pub async fn publish_message(
        &self,
        queue_name: &str,
        message: PublishableMessage<'_>,
    ) -> Result<bool, ClientError> {
        let uri = format!("{}/messages/{}", &self.host, queue_name);
        let response = self
            .request(|| {
                let (headers, body) = message.clone().encode();
                let mut req = Self::new_request(Method::POST, &uri, None, Body::from(body))?;
                for (key, value) in headers {
                    // we never get the same header twice from PublishableMessage::encode, so we
                    // can just ignore that case
                    if let Some(key) = key {
                        req.headers_mut().insert(key, value);
                    }
                }
                Ok::<_, ClientError>(req)
            })
            .await?;
        match response.status().as_u16() {
            200 => Ok(false),
            201 => Ok(true),
            status => Err(ClientError::ServiceError(status)),
        }
    }

    /// Publish a set of messages to a queue.
    ///
    /// ```
    /// use mqs_client::{ClientError, PublishableMessage, Service};
    /// use uuid::Uuid;
    ///
    /// async fn example(service: &Service) -> Result<bool, ClientError> {
    ///     let trace_id = Uuid::new_v4();
    ///     let mut messages = Vec::with_capacity(10);
    ///     for i in 0..messages.capacity() {
    ///         messages.push(PublishableMessage {
    ///             trace_id:         Some(trace_id),
    ///             content_type:     "text/plain",
    ///             content_encoding: None,
    ///             message:          format!("Message {}", i).into_bytes(),
    ///         });
    ///     }
    ///
    ///     service.publish_messages("my-queue", &messages).await
    /// }
    /// ```
    ///
    /// # Errors
    ///
    /// Returns an error if the request fails or the server returns an invalid status.
    pub async fn publish_messages(
        &self,
        queue_name: &str,
        messages: &[PublishableMessage<'_>],
    ) -> Result<bool, ClientError> {
        let uri = format!("{}/messages/{}", &self.host, queue_name);
        let response = self
            .request(|| {
                let (boundary, body) = multipart::encode(messages.iter().map(|msg| msg.clone().encode()));
                let mut req = Self::new_request(Method::POST, &uri, None, Body::from(body))?;
                req.headers_mut().insert(
                    CONTENT_TYPE,
                    HeaderValue::from_str(&format!("multipart/mixed; boundary={}", boundary))?,
                );
                Ok::<_, ClientError>(req)
            })
            .await?;
        match response.status().as_u16() {
            200 => Ok(false),
            201 => Ok(true),
            status => Err(ClientError::ServiceError(status)),
        }
    }

    /// Delete a single message. The message is identified by its id and not by the queue in which it
    /// currently resides (a redrive policy will move a message as soon as it hits the maximum receive
    /// count. It will then be invisible in the new queue until the visibility timeout expires. Thus
    /// it is possible for messages to be in different queues directly after they have been received
    /// from some queue).
    ///
    /// ```
    /// use mqs_client::{ClientError, Service};
    ///
    /// async fn consume_all<F: Fn(String, Option<String>, Vec<u8>)>(
    ///     service: &Service,
    ///     queue_name: &str,
    ///     callback: F,
    /// ) -> Result<(), ClientError> {
    ///     loop {
    ///         let messages = service.get_messages(queue_name, 10, Some(20)).await?;
    ///         for msg in messages {
    ///             callback(msg.content_type, msg.content_encoding, msg.content);
    ///             service.delete_message(msg.trace_id, &msg.message_id).await?;
    ///         }
    ///     }
    /// }
    /// ```
    ///
    /// # Errors
    ///
    /// Returns an error if the request fails or the server returns an invalid status.
    pub async fn delete_message(&self, trace_id: Option<Uuid>, message_id: &str) -> Result<bool, ClientError> {
        let uri = format!("{}/messages/{}", &self.host, message_id);
        let response = self
            .request(|| Self::new_request(Method::DELETE, &uri, trace_id, Body::default()))
            .await?;
        match response.status().as_u16() {
            200 => Ok(true),
            404 => Ok(false),
            status => Err(ClientError::ServiceError(status)),
        }
    }

    /// Evaluate the health of a service. Returns true if the service is healthy, false if it is not
    /// healthy, `HealthCheckError` if the service responded with an invalid status.
    ///
    /// ```
    /// use mqs_client::Service;
    /// use tokio::runtime::Builder;
    ///
    /// let service = Service::new("https://mqs.example.com:7843");
    ///
    /// let mut rt = Builder::new_multi_thread().enable_all().build().unwrap();
    /// let success = rt.block_on(async { service.check_health().await });
    /// assert!(!success.is_ok());
    /// ```
    ///
    /// # Errors
    ///
    /// Returns an error if the request fails, the server returns a status different from 200, or a
    /// response different from "green" or "red".
    pub async fn check_health(&self) -> Result<bool, ClientError> {
        let uri = format!("{}/health", &self.host);
        let mut response = self
            .request(|| Self::new_request(Method::GET, &uri, None, Body::default()))
            .await?;
        let body = match response.status().as_u16() {
            200 => Ok(read_body(response.body_mut(), self.max_body_size).await?),
            status => Err(ClientError::ServiceError(status)),
        }?;
        body.map_or(Err(ClientError::TooLargeResponse), |body| {
            if body.as_slice().eq(b"green") {
                Ok(true)
            } else if body.as_slice().eq(b"red") {
                Ok(false)
            } else {
                Err(ClientError::HealthCheckError)
            }
        })
    }
}

#[cfg(test)]
mod test {
    use super::*;
    use hyper::Uri;
    use mqs_common::test::make_runtime;
    use std::io::ErrorKind;

    #[test]
    fn encode_publishable_message() {
        let msg = PublishableMessage {
            trace_id:         None,
            content_encoding: None,
            content_type:     "type",
            message:          vec![1, 2, 3],
        };
        assert_eq!(
            msg.encode(),
            (
                {
                    let mut headers = HeaderMap::new();
                    headers.insert(CONTENT_TYPE, HeaderValue::from_static("type"));
                    headers
                },
                vec![1, 2, 3]
            )
        );
        let msg = PublishableMessage {
            trace_id:         Some(Uuid::parse_str("96a372de-2db0-405b-a49e-fbcddcabefdb").unwrap()),
            content_encoding: Some("encoding"),
            content_type:     "type",
            message:          vec![4, 5, 6],
        };
        assert_eq!(
            msg.encode(),
            (
                {
                    let mut headers = HeaderMap::new();
                    headers.insert(CONTENT_TYPE, HeaderValue::from_static("type"));
                    headers.insert(CONTENT_ENCODING, HeaderValue::from_static("encoding"));
                    headers.insert(
                        TraceIdHeader::name(),
                        HeaderValue::from_static("96a372de-2db0-405b-a49e-fbcddcabefdb"),
                    );
                    headers
                },
                vec![4, 5, 6]
            )
        );
    }

    #[test]
    fn test_errors() {
        // let invalid_method = Method::from_bytes(&[]).unwrap_err();
        let client = hyper::client::Client::new();
        let rt = make_runtime();
        let hyper_error = rt.block_on(async {
            client
                .get("http://localhost:60000/non-existent".parse().unwrap())
                .await
                .unwrap_err()
        });
        let hyper_error_string = format!("HyperError({:?})", &hyper_error);
        let err: ClientError = ClientError::from(hyper_error);
        assert_eq!(format!("{}", err), hyper_error_string);

        let invalid_uri_error = "".parse::<Uri>().unwrap_err();
        let err = ClientError::from(invalid_uri_error);
        assert_eq!(format!("{}", err), "InvalidUri(InvalidUri(Empty))");

        let std_err = std::io::Error::from(ErrorKind::NotConnected);
        let err = ClientError::from(std_err);
        assert_eq!(format!("{}", err), "IoError(Kind(NotConnected))");

        let serde_error = serde_json::from_str::<String>("").unwrap_err();
        let err = ClientError::from(serde_error);
        assert_eq!(
            format!("{}", err),
            "ParseError(Error(\"EOF while parsing a value\", line: 1, column: 0))"
        );

        let invalid_header_error = HeaderValue::from_str("\0").unwrap_err();
        let err = ClientError::from(invalid_header_error);
        assert_eq!(format!("{}", err), "InvalidHeaderValue(InvalidHeaderValue)");

        let parse_error = multipart::InvalidMultipart::Chunk;
        let err = ClientError::from(parse_error);
        assert_eq!(format!("{}", err), "MultipartParseError(Chunk)");
    }

    #[test]
    fn set_max_body_size() {
        let mut service = Service::new("http://localhost:7843");
        service.set_max_body_size(None);
        assert_eq!(service.max_body_size, None);
        service.set_max_body_size(Some(64 * 1024));
        assert_eq!(service.max_body_size, Some(64 * 1024));
    }
}
