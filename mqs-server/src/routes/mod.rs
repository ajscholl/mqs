use hyper::{
    header::{HeaderValue, CONTENT_ENCODING, CONTENT_TYPE},
    Body,
    HeaderMap,
};
use mqs_common::{
    multipart,
    MessageIdHeader,
    MessageReceivesHeader,
    PublishedAtHeader,
    Status,
    TraceIdHeader,
    VisibleAtHeader,
};
use serde::Serialize;

use crate::models::message::Message;

pub mod messages;
pub mod queues;

#[derive(Serialize, Debug, Clone, Copy)]
pub struct ErrorResponse<'a> {
    error: &'a str,
}

#[derive(Debug)]
pub enum MqsResponse {
    Status(Status),
    Json(Status, String),
    Message(Status, Vec<Message>),
}

impl MqsResponse {
    pub(crate) const fn status(status: Status) -> Self {
        Self::Status(status)
    }

    pub(crate) fn error_static(error: &'static str) -> Self {
        Self::status_json(Status::BadRequest, &ErrorResponse { error })
    }

    pub(crate) fn error_owned(error: &str) -> Self {
        Self::status_json(Status::BadRequest, &ErrorResponse { error })
    }

    pub(crate) fn json<T: Serialize>(body: &T) -> Self {
        Self::status_json(Status::Ok, body)
    }

    pub(crate) fn status_json<T: Serialize>(status: Status, body: &T) -> Self {
        match serde_json::to_string(body) {
            Err(err) => {
                error!("Failed to serialize json response: {}", err);

                Self::Status(Status::InternalServerError)
            },
            Ok(json) => Self::Json(status, json),
        }
    }

    pub(crate) fn messages(messages: Vec<Message>) -> Self {
        Self::Message(Status::Ok, messages)
    }

    pub(crate) fn into_response(self) -> hyper::Response<Body> {
        match self {
            Self::Status(status) => {
                let mut res = hyper::Response::new(Body::default());
                *res.status_mut() = status.into();
                res
            },
            Self::Json(status, body) => {
                let mut res = hyper::Response::new(Body::from(body));
                *res.status_mut() = status.into();
                res.headers_mut()
                    .insert(CONTENT_TYPE, HeaderValue::from_static("application/json"));
                res
            },
            Self::Message(status, mut messages) => {
                if messages.len() == 1 {
                    let message = messages.pop().unwrap();

                    let mut res = hyper::Response::new(Body::default());
                    *res.status_mut() = status.into();
                    Self::add_message_headers(res.headers_mut(), &message);
                    *res.body_mut() = Body::from(message.payload);
                    return res;
                }

                let message_parts = messages.into_iter().map(|message| {
                    let mut headers = HeaderMap::new();
                    Self::add_message_headers(&mut headers, &message);
                    (headers, message.payload)
                });
                let (boundary, body) = multipart::encode(message_parts);

                let mut res = hyper::Response::new(Body::from(body));
                *res.status_mut() = status.into();
                res.headers_mut().insert(
                    CONTENT_TYPE,
                    HeaderValue::from_str(&format!("multipart/mixed; boundary={}", &boundary)).unwrap(),
                );
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
        if let Some(trace_id) = message.trace_id {
            if let Ok(value) = HeaderValue::from_str(&trace_id.to_string()) {
                headers.insert(TraceIdHeader::name(), value);
            }
        }
        if let Ok(value) = HeaderValue::from_str(&message.id.to_string()) {
            headers.insert(MessageIdHeader::name(), value);
        }
        if let Ok(value) = HeaderValue::from_str(&format!("{}", message.receives)) {
            headers.insert(MessageReceivesHeader::name(), value);
        }
        if let Ok(value) = HeaderValue::from_str(&message.created_at.to_rfc3339()) {
            headers.insert(PublishedAtHeader::name(), value);
        }
        if let Ok(value) = HeaderValue::from_str(&message.visible_since.to_rfc3339()) {
            headers.insert(VisibleAtHeader::name(), value);
        }
    }
}

#[cfg(test)]
pub(crate) mod test {
    use super::*;
    use mqs_common::{test::read_body, UtcTime};

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
            (Status::BadRequest, MqsResponse::error_owned("test").into_response()),
            (
                Status::Ok,
                MqsResponse::json(&ErrorResponse { error: "test" }).into_response(),
            ),
        ];
        for (expected_status, response) in responses.iter_mut() {
            assert_eq!(response.status().as_u16(), *expected_status as u16);
            assert_eq!(response.headers().len(), 1);
            assert_eq!(
                response.headers().get(CONTENT_TYPE).unwrap(),
                HeaderValue::from_static("application/json")
            );
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
        assert_eq!(
            read_body(response.body_mut()).as_slice(),
            format!("--{}--", boundary).as_bytes()
        );
    }

    fn mk_message(index: u8, encoding: Option<String>) -> Message {
        let now = UtcTime::now();
        Message {
            id:               uuid::Uuid::from_bytes([
                10 + index,
                20,
                30,
                40,
                11,
                21,
                31,
                41,
                12,
                22,
                32,
                43,
                14,
                24,
                34,
                44,
            ]),
            payload:          vec![65, 66, 67],
            content_type:     "text/plain".to_string(),
            content_encoding: encoding,
            hash:             None,
            queue:            String::new(),
            receives:         index as i32 + 1,
            visible_since:    now,
            created_at:       now,
            trace_id:         None,
        }
    }

    fn message_response_single_with_encoding(encoding: Option<String>) {
        let mut response = MqsResponse::messages(vec![mk_message(0, encoding.clone())]).into_response();
        assert_eq!(response.status().as_u16(), Status::Ok as u16);
        assert_eq!(response.headers().len(), if encoding.is_some() { 6 } else { 5 });
        let ct = response.headers().get(CONTENT_TYPE).unwrap().to_str().unwrap();
        let message_id = MessageIdHeader::get(response.headers());
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
        let messages = vec![
            mk_message(0, encoding.clone()),
            mk_message(1, encoding.clone()),
            mk_message(2, encoding.clone()),
        ];
        let mut response = MqsResponse::messages(messages.clone()).into_response();
        assert_eq!(response.status().as_u16(), Status::Ok as u16);
        assert_eq!(response.headers().len(), 1);
        let ct = response.headers().get(CONTENT_TYPE).unwrap().to_str().unwrap();
        if let Some(boundary) = multipart::is_multipart(ct) {
            let encoding_header = if let Some(encoding) = encoding {
                format!("content-encoding: {}\r\n", encoding)
            } else {
                String::new()
            };
            assert_eq!(
                read_body(response.body_mut()).as_slice(),
                format!(
                    "{}\r\ncontent-type: text/plain\r\n{}x-mqs-message-id: {}\r\nx-mqs-message-receives: 1\r\nx-mqs-message-published-at: {}\r\nx-mqs-message-visible-at: {}\r\n\r\nABC\r\n\
                    {}\r\ncontent-type: text/plain\r\n{}x-mqs-message-id: {}\r\nx-mqs-message-receives: 2\r\nx-mqs-message-published-at: {}\r\nx-mqs-message-visible-at: {}\r\n\r\nABC\r\n\
                    {}\r\ncontent-type: text/plain\r\n{}x-mqs-message-id: {}\r\nx-mqs-message-receives: 3\r\nx-mqs-message-published-at: {}\r\nx-mqs-message-visible-at: {}\r\n\r\nABC\r\n{}--",
                    boundary,
                    encoding_header,
                    "0a141e28-0b15-1f29-0c16-202b0e18222c",
                    messages[0].created_at.to_rfc3339(),
                    messages[0].visible_since.to_rfc3339(),
                    boundary,
                    encoding_header,
                    "0b141e28-0b15-1f29-0c16-202b0e18222c",
                    messages[1].created_at.to_rfc3339(),
                    messages[1].visible_since.to_rfc3339(),
                    boundary,
                    encoding_header,
                    "0c141e28-0b15-1f29-0c16-202b0e18222c",
                    messages[2].created_at.to_rfc3339(),
                    messages[2].visible_since.to_rfc3339(),
                    boundary
                )
                .as_bytes(),
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
