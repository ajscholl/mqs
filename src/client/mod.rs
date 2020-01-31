use hyper::{Client, Body, Request, Method, Response};
use hyper::client::HttpConnector;
use hyper::body::{HttpBody, Buf};
use hyper::header::HeaderValue;
use std::error::Error;
use std::fmt::{Display, Formatter};
use serde::Deserialize;

use crate::routes::queues::{QueuesResponse, QueueDescription, QueueConfig};

#[derive(Debug)]
pub enum ClientError {
    HyperError(hyper::Error),
    InvalidUri(hyper::http::uri::InvalidUri),
    IoError(std::io::Error),
    ParseError(serde_json::error::Error),
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

pub struct Service {
    client: Client<HttpConnector>,
    host: String,
}

#[derive(Debug)]
pub struct MessageResponse {
    pub message_id: String,
    pub content_type: String,
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
        Ok(req)
    }

    async fn read_body(body: &mut Body) -> Result<Vec<u8>, hyper::error::Error> {
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
        req.headers_mut().insert("Content-Type", HeaderValue::from_static("application/json"));
        let response = self.client.request(req).await?;
        let mut body = Vec::new();
        Self::parse_response_maybe(response, &mut body, 201, 409).await
    }

    pub async fn update_queue(&self, queue_name: &str, config: &QueueConfig) -> Result<Option<QueueConfig>, ClientError> {
        let uri = format!("{}/queues/{}", &self.host, queue_name);
        let message = serde_json::to_string(config)?;
        let mut req = Self::new_request(Method::POST, &uri, Body::from(message))?;
        req.headers_mut().insert("Content-Type", HeaderValue::from_static("application/json"));
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
        let uri = format!("{}/messages/{}", &self.host, queue_name);
       let req = Self::new_request(Method::GET, &uri, Body::default())?;
        let mut response = self.client.request(req).await?;
        match response.status().as_u16() {
            200 => {
                let message_id = response.headers().get("X-MQS-MESSAGE-ID").map_or_else(|| "".to_string(), |h| {
                    h.to_str().unwrap_or("").to_string()
                });
                let content_type = response.headers().get("Content-Type").map_or_else(|| "application/octet-stream".to_string(), |h| {
                    h.to_str().unwrap_or("application/octet-stream").to_string()
                });
                let content = Self::read_body(response.body_mut()).await?;
                Ok(Some(MessageResponse {
                    message_id,
                    content_type,
                    content,
                }))
            },
            204 => Ok(None),
            status => Err(ClientError::ServiceError(status)),
        }
    }

    pub async fn publish_message(&self, queue_name: &str, content_type: &str, message: Vec<u8>) -> Result<bool, ClientError> {
        let uri = format!("{}/messages/{}", &self.host, queue_name);
        let mut req = Self::new_request(Method::POST, &uri, Body::from(message))?;
        if let Ok(content_type) = HeaderValue::from_str(content_type) {
            req.headers_mut().insert("Content-Type", content_type);
        }
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
