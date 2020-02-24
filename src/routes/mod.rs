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

#[cfg(test)]
mod test {
    use super::*;
    use crate::client::Service;
    use tokio::runtime::Builder;
    use chrono::Utc;

    fn read_body(body: &mut Body) -> Vec<u8> {
        let mut rt = Builder::new()
            .enable_all()
            .basic_scheduler()
            .build()
            .unwrap();

        rt.block_on(async {
            Service::read_body(body).await.unwrap()
        })
    }

    #[test]
    fn status_response() {
        let mut response = MqsResponse::status(Status::Ok).into_response();
        assert_eq!(response.status().as_u16(), Status::Ok as u16);
        assert!(response.headers().is_empty());
        assert_eq!(read_body(response.body_mut()), Vec::<u8>::new());
    }

    #[test]
    fn json_response() {
        let json = "{\"error\":\"test\"}";
        let mut responses = [
            (Status::BadRequest, MqsResponse::error_static("test").into_response()),
            (Status::BadRequest, MqsResponse::error_owned("test".to_string()).into_response()),
            (Status::Ok, MqsResponse::json(&ErrorResponse { error: "test" }).into_response()),
        ];
        for (expected_status, response) in responses.iter_mut() {
            assert_eq!(response.status().as_u16(), *expected_status as u16);
            assert_eq!(response.headers().len(), 1);
            assert_eq!(response.headers().get(CONTENT_TYPE).unwrap(), HeaderValue::from_static("application/json"));
            assert_eq!(read_body(response.body_mut()).as_slice(), json.as_bytes());
        }
    }

    #[test]
    fn message_response_empty() {
        let mut response = MqsResponse::messages(Vec::new()).into_response();
        assert_eq!(response.status().as_u16(), Status::Ok as u16);
        assert_eq!(response.headers().len(), 1);
        let ct = response.headers().get(CONTENT_TYPE).unwrap().to_str().unwrap();
        assert!(ct.starts_with("multipart/mixed; boundary="));
        let boundary = ct["multipart/mixed; boundary=".len()..].to_string();
        assert_eq!(read_body(response.body_mut()).as_slice(), format!("--{}--", boundary).as_bytes());
    }

    fn mk_message(index: u8, encoding: Option<String>) -> Message {
        let now = Utc::now();
        Message {
            id: uuid::Uuid::from_uuid_bytes([10 + index, 20, 30, 40, 11, 21, 31, 41, 12, 22, 32, 43, 14, 24, 34, 44]),
            payload: vec![65, 66, 67],
            content_type: "text/plain".to_string(),
            content_encoding: encoding,
            hash: None,
            queue: "".to_string(),
            receives: 0,
            visible_since: now.naive_utc(),
            created_at: now.naive_utc(),
        }
    }

    fn message_response_single_with_encoding(encoding: Option<String>) {
        let mut response = MqsResponse::messages(vec![
            mk_message(0, encoding.clone()),
        ]).into_response();
        assert_eq!(response.status().as_u16(), Status::Ok as u16);
        assert_eq!(response.headers().len(), if encoding.is_some() { 3 } else { 2 });
        let ct = response.headers().get(CONTENT_TYPE).unwrap().to_str().unwrap();
        let message_id = response.headers().get(HeaderName::from_static("x-mqs-message-id")).unwrap().to_str().unwrap();
        assert_eq!(ct, "text/plain");
        assert_eq!(message_id, "0a141e28-0b15-1f29-0c16-202b0e18222c");
        if let Some(encoding) = encoding {
            let ce = response.headers().get(CONTENT_ENCODING).unwrap().to_str().unwrap();
            assert_eq!(ce, encoding.as_str());
        }
        assert_eq!(read_body(response.body_mut()).as_slice(), &[65u8, 66, 67][..]);
    }

    #[test]
    fn message_response_single() {
        message_response_single_with_encoding(None);
    }

    #[test]
    fn message_response_single_encoded() {
        message_response_single_with_encoding(Some("gzip".to_string()));
    }

    fn message_response_multiple_with_encoding(encoding: Option<String>) {
        let mut response = MqsResponse::messages(vec![
            mk_message(0, encoding.clone()),
            mk_message(1, encoding.clone()),
            mk_message(2, encoding.clone()),
        ]).into_response();
        assert_eq!(response.status().as_u16(), Status::Ok as u16);
        assert_eq!(response.headers().len(), 1);
        let ct = response.headers().get(CONTENT_TYPE).unwrap().to_str().unwrap();
        if let Some(boundary) = multipart::is_multipart(ct) {
            let encoding_header = if let Some(encoding) = encoding {
                format!("content-encoding: {}\r\n", encoding)
            } else {
                "".to_string()
            };
            assert_eq!(
                read_body(response.body_mut()).as_slice(),
                format!("{}\r\ncontent-type: text/plain\r\n{}x-mqs-message-id: {}\r\n\r\nABC\r\n{}\r\ncontent-type: text/plain\r\n{}x-mqs-message-id: {}\r\n\r\nABC\r\n{}\r\ncontent-type: text/plain\r\n{}x-mqs-message-id: {}\r\n\r\nABC\r\n{}--", boundary, encoding_header, "0a141e28-0b15-1f29-0c16-202b0e18222c", boundary, encoding_header, "0b141e28-0b15-1f29-0c16-202b0e18222c", boundary, encoding_header, "0c141e28-0b15-1f29-0c16-202b0e18222c", boundary).as_bytes(),
            );
        } else {
            assert!(false);
        }
    }

    #[test]
    fn message_response_multiple() {
        message_response_multiple_with_encoding(None);
    }

    #[test]
    fn message_response_multiple_encoded() {
        message_response_multiple_with_encoding(Some("gzip".to_string()));
    }
}
