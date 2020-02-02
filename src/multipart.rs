use uuid::Uuid;
use rocket::http::ContentType;
use hyper::HeaderMap;
use hyper::header::{HeaderName, HeaderValue, InvalidHeaderName, InvalidHeaderValue};
use std::fmt::{Display, Formatter};
use std::error::Error;

/// Encode data as a multipart/mixed document, return the boundary and the body
pub fn encode(messages: &Vec<(HeaderMap, Vec<u8>)>) -> (String, Vec<u8>) {
    let boundary = Uuid::new_v4().to_string();
    let full_boundary = format!("--{}", &boundary).into_bytes();
    let mut body = Vec::with_capacity(4096);

    for (headers, message) in messages {
        body.extend_from_slice(full_boundary.as_slice());
        body.extend_from_slice("\r\n".as_bytes());
        for (header_name, header_value) in headers {
            body.extend_from_slice(header_name.as_str().as_bytes());
            body.extend_from_slice(": ".as_bytes());
            body.extend_from_slice(header_value.as_bytes());
            body.extend_from_slice("\r\n".as_bytes());
        }
        body.extend_from_slice("\r\n".as_bytes());
        body.extend_from_slice(message.as_slice());
        body.extend_from_slice("\r\n".as_bytes());
    }
    body.extend_from_slice(full_boundary.as_slice());
    body.extend_from_slice("--".as_bytes());

    (boundary, body)
}

/// Return the boundary from "multipart/mixed; boundary=..."
pub fn is_multipart(content_type: &str) -> Option<String> {
    if let Ok(ct) = content_type.parse() {
        let ct: ContentType = ct;
        if !ct.top().eq("multipart") || !ct.sub().eq("mixed") {
            return None;
        }

        for (key, value) in ct.params() {
            if key == "boundary" {
                return Some(format!("--{}", value));
            }
        }
    }

    None
}

#[derive(Debug)]
pub enum ParseError {
    InvalidChunk,
    InvalidHeaderName,
    InvalidHeaderValue,
}


impl Display for ParseError {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "{:?}", self)
    }
}

impl Error for ParseError {}

impl From<InvalidHeaderName> for ParseError {
    fn from(_: InvalidHeaderName) -> Self {
        ParseError::InvalidHeaderName
    }
}

impl From<InvalidHeaderValue> for ParseError {
    fn from(_: InvalidHeaderValue) -> Self {
        ParseError::InvalidHeaderValue
    }
}

/// Split a message body at the boundaries and return a list of content-type/data pairs
pub fn parse<'a, 'b>(boundary: &'b str, body: &'a str) -> Result<Vec<(HeaderMap, &'a str)>, ParseError> {
    let mut result = Vec::new();

    let mut skipped_preamble = false;
    for mut document in body.split(&format!("\r\n{}", boundary)) {
        if !skipped_preamble {
            skipped_preamble = true;

            if !document.starts_with(boundary) {
                // if we start with the boundary, there was no preamble, but also no leading CRLF
                continue;
            } else {
                document = &document[boundary.len()..];
            }
        }

        if document.starts_with("--") {
            // last boundary, stop processing
            break;
        }

        // TODO: handle whitespace to ignore

        if !document.starts_with("\r\n") {
            // invalid chunk, signal bad request
            return Err(ParseError::InvalidChunk);
        }

        // remove initial CRLF
        let doc: &str = &document[2..];

        if doc.starts_with("\r\n") {
            // empty list of headers,
            result.push((HeaderMap::new(), &doc[2..]));
            continue;
        }

        let (header_text, body): (&str, &str) = {
            let mut it = doc.splitn(2, "\r\n\r\n");
            let header_text = it.next().unwrap_or("");
            let body = it.next().unwrap_or("");
            debug_assert!(it.next().is_none());
            (header_text, body)
        };

        let headers = {
            let mut headers = HeaderMap::new();
            for header in header_text.split("\r\n") {
                let mut it = header.splitn(2, ":");
                let name = it.next().unwrap_or("");
                let body = it.next().unwrap_or("");
                debug_assert!(it.next().is_none());
                headers.insert(HeaderName::from_bytes(name.as_bytes())?, HeaderValue::from_str(body.trim())?);
            }

            headers
        };

        result.push((headers, body));
    }

    Ok(result)
}

#[cfg(test)]
mod test {
    use super::*;

    fn get_input() -> Vec<(HeaderMap, Vec<u8>)> {
        vec![
            ({
                 let mut m = HeaderMap::new();
                 m.insert(HeaderName::from_static("content-type"), HeaderValue::from_static("data/type"));
                 m
             }, "This is my first message".as_bytes().to_vec()),
            ({
                 let mut m = HeaderMap::new();
                 m.insert(HeaderName::from_static("content-type"), HeaderValue::from_static("data/another-type"));
                 m
             }, "\r\nAnother message\r\nWith more than one line\r\n".as_bytes().to_vec()),
            ({
                 let mut m = HeaderMap::new();
                 m.insert(HeaderName::from_static("content-type"), HeaderValue::from_static("foo/bar"));
                 m
             }, "Last message, don't forget it".as_bytes().to_vec()),
        ]
    }

    #[test]
    fn encode_multipart() {
        let (boundary, body) = encode(&get_input());
        assert_eq!(std::str::from_utf8(body.as_slice()).unwrap(), format!("--{}\r\ncontent-type: data/type\r\n\r\nThis is my first message\r\n--{}\r\ncontent-type: data/another-type\r\n\r\n\r\nAnother message\r\nWith more than one line\r\n\r\n--{}\r\ncontent-type: foo/bar\r\n\r\nLast message, don\'t forget it\r\n--{}--", &boundary, &boundary, &boundary, &boundary));
    }

    #[test]
    fn is_multipart() {
        assert_eq!(None, super::is_multipart("text/plain"));
        assert_eq!(None, super::is_multipart("text/plain; boundary=abc"));
        assert_eq!(None, super::is_multipart("multipart/alternative; boundary=abc"));
        assert_eq!(Some("--abc".to_string()), super::is_multipart("multipart/mixed; boundary=abc"));
        assert_eq!(Some("--my boundary".to_string()), super::is_multipart("multipart/mixed; boundary=\"my boundary\""));
        assert_eq!(Some("--my boundary".to_string()), super::is_multipart("multipart/mixed; foo=abc; boundary=\"my boundary\""));
        assert_eq!(Some("--abc".to_string()), super::is_multipart("multipart/mixed; boundary=abc; foo=bar"));
    }

    #[test]
    fn parse_multipart() {
        let parsed = parse("--abc", "ignore this\r\n--abc\r\nContent-Type: text/plain\r\n\r\nThis is my text\r\n--abc\r\n\r\nThis has no content type\r\n\r\n--abc--this is ignored");
        assert!(parsed.is_ok());
        let parsed = parsed.unwrap();
        println!("{:?}", parsed);
        assert_eq!(parsed.len(), 2);
        assert_eq!(parsed[0].0, {
            let mut m = HeaderMap::new();
            m.insert(HeaderName::from_static("content-type"), HeaderValue::from_static("text/plain"));
            m
        });
        assert_eq!(parsed[0].1, "This is my text");
        assert_eq!(parsed[1].0, HeaderMap::new());
        assert_eq!(parsed[1].1, "This has no content type\r\n");
    }

    #[test]
    fn gen_and_parse() {
        let input = get_input();
        let (boundary, body) = encode(&input);
        assert!(!boundary.starts_with("--"));
        let body_string = std::str::from_utf8(body.as_slice());
        assert!(body_string.is_ok());
        let body_string = body_string.unwrap();
        assert_eq!(super::is_multipart(&format!("multipart/mixed; boundary={}", &boundary)), Some(format!("--{}", &boundary)));
        let parsed = parse(&format!("--{}", boundary), body_string);
        assert!(parsed.is_ok());
        let parsed = parsed.unwrap();
        assert_eq!(parsed, vec![
            (input[0].0.clone(), std::str::from_utf8(input[0].1.as_slice()).unwrap()),
            (input[1].0.clone(), std::str::from_utf8(input[1].1.as_slice()).unwrap()),
            (input[2].0.clone(), std::str::from_utf8(input[2].1.as_slice()).unwrap()),
        ]);
    }
}
