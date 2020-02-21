use serde::Serialize;
use hyper::{Body, HeaderMap};
use hyper::header::{CONTENT_TYPE, HeaderValue, HeaderName, CONTENT_ENCODING};

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

                    let mut res = hyper::Response::new(Body::default());
                    *res.status_mut() = status.to_hyper();
                    Self::add_message_headers(res.headers_mut(), &message);
                    *res.body_mut() = Body::from(message.payload);
                    return res;
                }

                let message_parts = messages.into_iter().map(|message| {
                    let mut headers = HeaderMap::new();
                    Self::add_message_headers(&mut headers, &message);
                    (headers, message.payload)
                }).collect();
                let (boundary, body) = multipart::encode(&message_parts);

                let mut res = hyper::Response::new(Body::from(body));
                *res.status_mut() = status.to_hyper();
                res.headers_mut().insert(CONTENT_TYPE, HeaderValue::from_str(&format!("multipart/mixed; boundary={}", &boundary)).unwrap());
                res
            },
        }
    }

    fn add_message_headers(headers: &mut HeaderMap, message: &Message) {
        if let Ok(value) = HeaderValue::from_str(&message.content_type) {
            headers.insert(CONTENT_TYPE, value);
        }
        if let Some(content_encoding) = &message.content_encoding {
            if let Ok(value) = HeaderValue::from_str(content_encoding) {
                headers.insert(CONTENT_ENCODING, value);
            }
        }
        if let Ok(value) = HeaderValue::from_str(&message.id.to_string()) {
            headers.insert(HeaderName::from_static("x-mqs-message-id"), value);
        }
    }
}
