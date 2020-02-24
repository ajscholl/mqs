use hyper::{Client, Body, Request, Method, Response, HeaderMap};
use hyper::client::HttpConnector;
use hyper::body::{HttpBody, Buf};
use hyper::header::{HeaderValue, HeaderName, CONNECTION, CONTENT_TYPE, CONTENT_ENCODING};
use std::error::Error;
use std::fmt::{Display, Formatter};
use serde::Deserialize;

use crate::multipart;
use crate::routes::queues::{QueuesResponse, QueueDescription, QueueConfig};
use crate::routes::messages::DEFAULT_CONTENT_TYPE;

#[derive(Debug)]
pub enum ClientError {
    HyperError(hyper::Error),
    InvalidUri(hyper::http::uri::InvalidUri),
    IoError(std::io::Error),
    ParseError(serde_json::error::Error),
    InvalidHeaderValue(hyper::header::InvalidHeaderValue),
    MultipartParseError(multipart::ParseError),
    ServiceError(u16),
}

impl Display for ClientError {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "{:?}", self)
    }
}

impl Error for ClientError {}

impl From<hyper::Error> for ClientError {
    fn from(error: hyper::Error) -> Self {
        ClientError::HyperError(error)
    }
}

impl From<hyper::http::uri::InvalidUri> for ClientError {
    fn from(error: hyper::http::uri::InvalidUri) -> Self {
        ClientError::InvalidUri(error)
    }
}

impl From<std::io::Error> for ClientError {
    fn from(error: std::io::Error) -> Self {
        ClientError::IoError(error)
    }
}

impl From<serde_json::error::Error> for ClientError {
    fn from(error: serde_json::error::Error) -> Self {
        ClientError::ParseError(error)
    }
}

impl From<hyper::header::InvalidHeaderValue> for ClientError {
    fn from(error: hyper::header::InvalidHeaderValue) -> Self {
        ClientError::InvalidHeaderValue(error)
    }
}

impl From<multipart::ParseError> for ClientError {
    fn from(error: multipart::ParseError) -> Self {
        ClientError::MultipartParseError(error)
    }
}

pub struct Service {
    client: Client<HttpConnector>,
    host: String,
}

#[derive(Debug)]
pub struct MessageResponse {
    pub message_id: String,
    pub content_type: String,
    pub content_encoding: Option<String>,
    pub content: Vec<u8>,
}

impl Service {
    pub fn new(host: &str) -> Service {
        Service {
            client: Client::new(),
            host: host.to_string(),
        }
    }

    fn new_request(method: Method, uri: &str, body: Body) -> Result<Request<Body>, hyper::http::uri::InvalidUri> {
        let mut req = Request::new(body);
        *req.uri_mut() = uri.parse()?;
        *req.method_mut() = method;
        req.headers_mut().insert(CONNECTION, HeaderValue::from_static("keep-alive"));
        Ok(req)
    }

    pub async fn read_body(body: &mut Body) -> Result<Vec<u8>, hyper::error::Error> {
        let mut chunks = Vec::new();
        let mut total_length = 0;

        while let Some(chunk) = body.data().await {
            let bytes = chunk?;
            total_length += bytes.len();
            chunks.push(bytes);
        }

        let mut result = Vec::with_capacity(total_length);

        for chunk in chunks {
            result.extend_from_slice(chunk.bytes());
        }

        Ok(result)
    }

    async fn parse_response_maybe<'a, T: Deserialize<'a>>(mut response: Response<Body>, body: &'a mut Vec<u8>, success_status: u16, error_status: u16) -> Result<Option<T>, ClientError> {
        let status = response.status().as_u16();
        if status == success_status {
            *body = Self::read_body(response.body_mut()).await?;
            let value = serde_json::from_slice(body.as_slice())?;
            Ok(Some(value))
        } else if status == error_status {
            Ok(None)
        } else {
            Err(ClientError::ServiceError(status))
        }
    }

    pub async fn create_queue(&self, queue_name: &str, config: &QueueConfig) -> Result<Option<QueueConfig>, ClientError> {
        let uri = format!("{}/queues/{}", &self.host, queue_name);
        let message = serde_json::to_string(config)?;
        let mut req = Self::new_request(Method::PUT, &uri, Body::from(message))?;
        req.headers_mut().insert(CONTENT_TYPE, HeaderValue::from_static("application/json"));
        let response = self.client.request(req).await?;
        let mut body = Vec::new();
        Self::parse_response_maybe(response, &mut body, 201, 409).await
    }

    pub async fn update_queue(&self, queue_name: &str, config: &QueueConfig) -> Result<Option<QueueConfig>, ClientError> {
        let uri = format!("{}/queues/{}", &self.host, queue_name);
        let message = serde_json::to_string(config)?;
        let mut req = Self::new_request(Method::POST, &uri, Body::from(message))?;
        req.headers_mut().insert(CONTENT_TYPE, HeaderValue::from_static("application/json"));
        let response = self.client.request(req).await?;
        let mut body = Vec::new();
        Self::parse_response_maybe(response, &mut body, 200, 404).await
    }

    pub async fn delete_queue(&self, queue_name: &str) -> Result<Option<QueueConfig>, ClientError> {
        let uri = format!("{}/queues/{}", &self.host, queue_name);
        let req = Self::new_request(Method::DELETE, &uri, Body::default())?;
        let response = self.client.request(req).await?;
        let mut body = Vec::new();
        Self::parse_response_maybe(response, &mut body, 200, 404).await
    }

    pub async fn get_queues(&self, offset: Option<usize>, limit: Option<usize>) -> Result<QueuesResponse, ClientError> {
        let uri = match (offset, limit) {
            (Some(offset), Some(limit)) => format!("{}/queues?offset={}&limit={}", &self.host, offset, limit),
            (Some(offset), None) => format!("{}/queues?offset={}", &self.host, offset),
            (None, Some(limit)) => format!("{}/queues?limit={}", &self.host, limit),
            (None, None) => format!("{}/queues", &self.host),
        };
        let req = Self::new_request(Method::GET, &uri, Body::default())?;
        let mut response = self.client.request(req).await?;
        match response.status().as_u16() {
            200 => {
                let body = Self::read_body(response.body_mut()).await?;
                let value = serde_json::from_slice(body.as_slice())?;
                Ok(value)
            },
            status => Err(ClientError::ServiceError(status)),
        }
    }

    pub async fn describe_queue(&self, queue_name: &str) -> Result<Option<QueueDescription>, ClientError> {
        let uri = format!("{}/queues/{}", &self.host, queue_name);
        let req = Self::new_request(Method::GET, &uri, Body::default())?;
        let response = self.client.request(req).await?;
        let mut body = Vec::new();
        Self::parse_response_maybe(response, &mut body, 200, 404).await
    }

    pub async fn get_message(&self, queue_name: &str) -> Result<Option<MessageResponse>, ClientError> {
        let mut messages = self.get_messages(queue_name, 1).await?;
        Ok(messages.pop())
    }

    fn parse_message<F: FnOnce() -> Result<Vec<u8>, ClientError>>(headers: &HeaderMap, get_body: F) -> Result<MessageResponse, ClientError> {
        let message_id = headers.get("X-MQS-MESSAGE-ID").map_or_else(|| "", |h| {
            h.to_str().unwrap_or("")
        }).to_string();
        let content_type = headers.get(CONTENT_TYPE).map_or_else(|| DEFAULT_CONTENT_TYPE, |h| {
            h.to_str().unwrap_or(DEFAULT_CONTENT_TYPE)
        }).to_string();
        let content_encoding = headers.get(CONTENT_ENCODING).map_or_else(|| None, |h| {
            h.to_str().map_or_else(|_| None, |s| Some(s.to_string()))
        });
        let content = get_body()?;
        Ok(MessageResponse {
            message_id,
            content_type,
            content_encoding,
            content,
        })
    }

    pub async fn get_messages(&self, queue_name: &str, limit: u16) -> Result<Vec<MessageResponse>, ClientError> {
        if limit == 0 {
            return Ok(Vec::new());
        }

        let uri = format!("{}/messages/{}", &self.host, queue_name);
        let mut req = Self::new_request(Method::GET, &uri, Body::default())?;
        if let Ok(value) = HeaderValue::from_str(&format!("{}", limit)) {
            req.headers_mut().insert(HeaderName::from_static("x-mqs-max-messages"), value);
        }
        let mut response = self.client.request(req).await?;
        match response.status().as_u16() {
            200 => {
                let content_type = response.headers().get(CONTENT_TYPE).map_or_else(|| DEFAULT_CONTENT_TYPE, |h| {
                    h.to_str().unwrap_or(DEFAULT_CONTENT_TYPE)
                }).to_string();
                let body = Self::read_body(response.body_mut()).await?;
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
            },
            204 => Ok(Vec::new()),
            status => Err(ClientError::ServiceError(status)),
        }
    }

    pub async fn publish_message(&self, queue_name: &str, content_type: &str, content_encoding: Option<&str>, message: Vec<u8>) -> Result<bool, ClientError> {
        let uri = format!("{}/messages/{}", &self.host, queue_name);
        let mut req = Self::new_request(Method::POST, &uri, Body::from(message))?;
        if let Ok(content_type) = HeaderValue::from_str(content_type) {
            req.headers_mut().insert(CONTENT_TYPE, content_type);
        }
        if let Some(content_encoding) = content_encoding {
            if let Ok(content_encoding) = HeaderValue::from_str(content_encoding) {
                req.headers_mut().insert(CONTENT_ENCODING, content_encoding);
            }
        }
        let response = self.client.request(req).await?;
        match response.status().as_u16() {
            200 => Ok(false),
            201 => Ok(true),
            status => Err(ClientError::ServiceError(status)),
        }
    }

    pub async fn publish_messages(&self, queue_name: &str, messages: &Vec<(HeaderMap, Vec<u8>)>) -> Result<bool, ClientError> {
        let uri = format!("{}/messages/{}", &self.host, queue_name);
        let (boundary, body) = multipart::encode(messages);
        let mut req = Self::new_request(Method::POST, &uri, Body::from(body))?;
        req.headers_mut().insert(CONTENT_TYPE, HeaderValue::from_str(&format!("multipart/mixed; boundary={}", boundary))?);
        let response = self.client.request(req).await?;
        match response.status().as_u16() {
            200 => Ok(false),
            201 => Ok(true),
            status => Err(ClientError::ServiceError(status)),
        }
    }

    pub async fn delete_message(&self, message_id: &str) -> Result<bool, ClientError> {
        let uri = format!("{}/messages/{}", &self.host, message_id);
        let req = Self::new_request(Method::DELETE, &uri, Body::default())?;
        let response = self.client.request(req).await?;
        match response.status().as_u16() {
            200 => Ok(true),
            404 => Ok(false),
            status => Err(ClientError::ServiceError(status)),
        }
    }
}
