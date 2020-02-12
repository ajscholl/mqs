use serde::Serialize;
use hyper::{Body, HeaderMap};
use hyper::header::{CONTENT_TYPE, HeaderValue, HeaderName};

use crate::models::message::Message;
use crate::multipart;
use crate::status::Status;

pub mod messages;
pub mod queues;

#[derive(Serialize, Debug)]
pub struct ErrorResponse<'a> {
    error: &'a str,
}

#[derive(Debug)]
pub enum MqsResponse {
    StatusResponse(Status),
    JsonResponse(Status, String),
    MessageResponse(Status, Vec<Message>),
}

impl MqsResponse {
    pub fn status(status: Status) -> Self {
        MqsResponse::StatusResponse(status)
    }

    pub fn error_static(error: &'static str) -> Self {
        Self::status_json(Status::BadRequest, &ErrorResponse { error })
    }

    pub fn error_owned(error: String) -> Self {
        Self::status_json(Status::BadRequest, &ErrorResponse { error: &error })
    }

    pub fn json<T: Serialize>(body: &T) -> Self {
        Self::status_json(Status::Ok, body)
    }

    pub fn status_json<T: Serialize>(status: Status, body: &T) -> Self {
        match serde_json::to_string(body) {
            Err(err) => {
                error!("Failed to serialize json response: {}", err);

                MqsResponse::StatusResponse(Status::InternalServerError)
            },
            Ok(json) => MqsResponse::JsonResponse(status, json),
        }
    }

    pub fn messages(messages: Vec<Message>) -> Self {
        MqsResponse::MessageResponse(Status::Ok, messages)
    }

    pub fn into_response(self) -> hyper::Response<Body> {
        match self {
            MqsResponse::StatusResponse(status) => {
                let mut res = hyper::Response::new(Body::default());
                *res.status_mut() = status.to_hyper();
                res
            },
            MqsResponse::JsonResponse(status, body) => {
                let mut res = hyper::Response::new(Body::from(body));
                *res.status_mut() = status.to_hyper();
                res.headers_mut().insert(CONTENT_TYPE, HeaderValue::from_static("application/json"));
                res
            },
            MqsResponse::MessageResponse(status, mut messages) => {
                if messages.len() == 1 {
                    let message = messages.pop().unwrap();

                    let mut res = hyper::Response::new(Body::from(message.payload));
                    *res.status_mut() = status.to_hyper();
                    res.headers_mut().insert(CONTENT_TYPE, HeaderValue::from_str(&message.content_type).unwrap());
                    res.headers_mut().insert("X-MQS-MESSAGE-ID", HeaderValue::from_str(&message.id.to_string()).unwrap());
                    return res;
                }

                let message_parts = messages.into_iter().map(|message| {
                    let mut headers = HeaderMap::new();
                    if let Ok(content_type) = HeaderValue::from_str(&message.content_type) {
                        headers.insert(HeaderName::from_static("content-type"), content_type);
                    }
                    if let Ok(id) = HeaderValue::from_str(&message.id.to_string()) {
                        headers.insert(HeaderName::from_static("x-mqs-message-id"), id);
                    }
                    (headers, message.payload.as_bytes().to_vec())
                }).collect();
                let (boundary, body) = multipart::encode(&message_parts);

                let mut res = hyper::Response::new(Body::from(body));
                *res.status_mut() = status.to_hyper();
                res.headers_mut().insert(CONTENT_TYPE, HeaderValue::from_str(&format!("multipart/mixed; boundary={}", &boundary)).unwrap());
                res
            },
        }
    }
}
