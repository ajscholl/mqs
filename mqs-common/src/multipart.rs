use hyper::{
    header::{HeaderName, HeaderValue, InvalidHeaderName, InvalidHeaderValue},
    HeaderMap,
};
use std::{
    error::Error,
    fmt::{Display, Formatter},
};
use uuid::Uuid;

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
    // TODO: a real parser would be nice...
    let (top, rest) = {
        let mut i = content_type.splitn(2, "/").into_iter();
        let top = i.next();
        let rest = i.next();
        debug_assert!(i.next().is_none());
        match (top, rest) {
            (Some(top), Some(rest)) => (top, rest),
            _ => return None,
        }
    };
    if top != "multipart" || !rest.starts_with("mixed;") {
        return None;
    }

    for param in rest.split(";").into_iter() {
        let (key, value) = {
            let mut i = param.splitn(2, "=").into_iter();
            let key = i.next();
            let value = i.next();
            debug_assert!(i.next().is_none());
            match (key, value) {
                (Some(key), Some(value)) => (key.trim(), {
                    let trimmed = value.trim();
                    if trimmed.starts_with("\"") && trimmed.ends_with("\"") {
                        &trimmed[1..trimmed.len() - 1]
                    } else {
                        trimmed
                    }
                }),
                _ => continue,
            }
        };
        if key == "boundary" {
            return Some(format!("--{}", value));
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
pub fn parse<'a, 'b>(boundary: &'b [u8], body: &'a [u8]) -> Result<Vec<(HeaderMap, &'a [u8])>, ParseError> {
    let mut result = Vec::new();

    let mut skipped_preamble = false;
    let separator = {
        let mut sep = Vec::with_capacity(2 + boundary.len());
        sep.push('\r' as u8);
        sep.push('\n' as u8);
        for b in boundary {
            sep.push(*b);
        }
        sep
    };
    for mut document in split(body, separator.as_slice()) {
        if !skipped_preamble {
            skipped_preamble = true;

            if !document.starts_with(boundary) {
                // if we start with the boundary, there was no preamble, but also no leading CRLF
                continue;
            } else {
                document = &document[boundary.len()..];
            }
        }

        if document.starts_with("--".as_bytes()) {
            // last boundary, stop processing
            break;
        }

        // TODO: handle whitespace to ignore

        if !document.starts_with("\r\n".as_bytes()) {
            // invalid chunk, signal bad request
            return Err(ParseError::InvalidChunk);
        }

        // remove initial CRLF
        let doc: &[u8] = &document[2..];

        if doc.starts_with("\r\n".as_bytes()) {
            // empty list of headers,
            result.push((HeaderMap::new(), &doc[2..]));
            continue;
        }

        if let Some((header_text, body)) = split_by(doc, "\r\n\r\n".as_bytes()) {
            let headers = {
                let mut headers = HeaderMap::new();
                for header in split(header_text, "\r\n".as_bytes()) {
                    if let Some((name, body)) = split_by(header, ":".as_bytes()) {
                        headers.insert(
                            HeaderName::from_bytes(name)?,
                            HeaderValue::from_bytes(trim_bytes(body))?,
                        );
                    }
                }

                headers
            };

            result.push((headers, body));
        }
    }

    Ok(result)
}

pub struct Split<'a, 'b> {
    data:     Option<&'a [u8]>,
    split_by: &'b [u8],
}

impl<'a, 'b> Iterator for Split<'a, 'b> {
    type Item = &'a [u8];

    fn next(&mut self) -> Option<Self::Item> {
        if let Some(data) = self.data {
            Some(match split_by(data, self.split_by) {
                None => {
                    self.data = None;
                    data
                },
                Some((elem, rest)) => {
                    self.data = Some(rest);
                    elem
                },
            })
        } else {
            None
        }
    }
}

pub fn split<'a, 'b>(data: &'a [u8], split_by: &'b [u8]) -> Split<'a, 'b> {
    Split {
        data: Some(data),
        split_by,
    }
}

pub fn split_by<'a, 'b>(data: &'a [u8], split_by: &'b [u8]) -> Option<(&'a [u8], &'a [u8])> {
    if data.len() < split_by.len() {
        return None;
    }

    let mut i = 0;
    let end = data.len() - split_by.len();

    while i < end {
        if data[i..].starts_with(split_by) {
            return Some((&data[..i], &data[i + split_by.len()..]));
        }

        i += 1;
    }

    None
}

pub fn trim_bytes(data: &[u8]) -> &[u8] {
    let mut start = 0;
    let mut end = data.len() - 1;

    while start < end && data[start] == ' ' as u8 {
        start += 1;
    }
    while end > start && data[end] == ' ' as u8 {
        end -= 1;
    }

    &data[start..end + 1]
}

#[cfg(test)]
mod test {
    use super::*;

    fn get_input() -> Vec<(HeaderMap, Vec<u8>)> {
        vec![
            (
                {
                    let mut m = HeaderMap::new();
                    m.insert(
                        HeaderName::from_static("content-type"),
                        HeaderValue::from_static("data/type"),
                    );
                    m
                },
                "This is my first message".as_bytes().to_vec(),
            ),
            (
                {
                    let mut m = HeaderMap::new();
                    m.insert(
                        HeaderName::from_static("content-type"),
                        HeaderValue::from_static("data/another-type"),
                    );
                    m
                },
                "\r\nAnother message\r\nWith more than one line\r\n".as_bytes().to_vec(),
            ),
            (
                {
                    let mut m = HeaderMap::new();
                    m.insert(
                        HeaderName::from_static("content-type"),
                        HeaderValue::from_static("foo/bar"),
                    );
                    m
                },
                "Last message, don't forget it".as_bytes().to_vec(),
            ),
        ]
    }

    #[test]
    fn split_by() {
        {
            let (a, b) = super::split_by("a:b".as_bytes(), ":".as_bytes()).unwrap();
            assert_eq!(a, "a".as_bytes());
            assert_eq!(b, "b".as_bytes());
        }
        {
            assert_eq!(super::split_by("".as_bytes(), ":".as_bytes()), None);
        }
        {
            let (a, b) = super::split_by("asd:b".as_bytes(), ":".as_bytes()).unwrap();
            assert_eq!(a, "asd".as_bytes());
            assert_eq!(b, "b".as_bytes());
        }
        {
            let (a, b) = super::split_by("asd:b:c".as_bytes(), ":".as_bytes()).unwrap();
            assert_eq!(a, "asd".as_bytes());
            assert_eq!(b, "b:c".as_bytes());
        }
        {
            let (a, b) = super::split_by("a:b::c".as_bytes(), "::".as_bytes()).unwrap();
            assert_eq!(a, "a:b".as_bytes());
            assert_eq!(b, "c".as_bytes());
        }
    }

    #[test]
    fn trim_bytes() {
        assert_eq!(super::trim_bytes("asd".as_bytes()), "asd".as_bytes());
        assert_eq!(super::trim_bytes("  asd".as_bytes()), "asd".as_bytes());
        assert_eq!(super::trim_bytes("asd ".as_bytes()), "asd".as_bytes());
        assert_eq!(super::trim_bytes("  asd ".as_bytes()), "asd".as_bytes());
        assert_eq!(super::trim_bytes("  asd   asd  ".as_bytes()), "asd   asd".as_bytes());
    }

    #[test]
    fn encode_multipart() {
        let (boundary, body) = encode(&get_input());
        assert_eq!(
            std::str::from_utf8(body.as_slice()).unwrap(),
            format!(
                "--{}\r\ncontent-type: data/type\r\n\r\nThis is my first message\r\n--{}\r\ncontent-type: data/another-type\r\n\r\n\r\nAnother message\r\nWith more than one line\r\n\r\n--{}\r\ncontent-type: foo/bar\r\n\r\nLast message, don\'t forget it\r\n--{}--",
                &boundary, &boundary, &boundary, &boundary
            )
        );
    }

    #[test]
    fn is_multipart() {
        assert_eq!(None, super::is_multipart("text/plain"));
        assert_eq!(None, super::is_multipart("text/plain; boundary=abc"));
        assert_eq!(None, super::is_multipart("multipart"));
        assert_eq!(None, super::is_multipart("multipart/alternative; boundary=abc"));
        assert_eq!(None, super::is_multipart("multipart/mixed"));
        assert_eq!(None, super::is_multipart("multipart/mixed; foo=bar"));
        assert_eq!(
            Some("--abc".to_string()),
            super::is_multipart("multipart/mixed; boundary=abc")
        );
        assert_eq!(
            Some("--my boundary".to_string()),
            super::is_multipart("multipart/mixed; boundary=\"my boundary\"")
        );
        assert_eq!(
            Some("--my boundary".to_string()),
            super::is_multipart("multipart/mixed; foo=abc; boundary=\"my boundary\"")
        );
        assert_eq!(
            Some("--abc".to_string()),
            super::is_multipart("multipart/mixed; boundary=abc; foo=bar")
        );
    }

    #[test]
    fn parse_multipart() {
        let parsed = parse(
            "--abc".as_bytes(),
            "ignore this\r\n--abc\r\nContent-Type: text/plain\r\n\r\nThis is my text\r\n--abc\r\n\r\nThis has no content type\r\n\r\n--abc--this is ignored"
                .as_bytes(),
        );
        assert!(parsed.is_ok());
        let parsed = parsed.unwrap();
        println!("{:?}", parsed);
        assert_eq!(parsed.len(), 2);
        assert_eq!(parsed[0].0, {
            let mut m = HeaderMap::new();
            m.insert(
                HeaderName::from_static("content-type"),
                HeaderValue::from_static("text/plain"),
            );
            m
        });
        assert_eq!(parsed[0].1, "This is my text".as_bytes());
        assert_eq!(parsed[1].0, HeaderMap::new());
        assert_eq!(parsed[1].1, "This has no content type\r\n".as_bytes());
    }

    #[test]
    fn gen_and_parse() {
        let input = get_input();
        let (boundary, body) = encode(&input);
        assert!(!boundary.starts_with("--"));
        let body_string = std::str::from_utf8(body.as_slice());
        assert!(body_string.is_ok());
        let body_string = body_string.unwrap();
        assert_eq!(
            super::is_multipart(&format!("multipart/mixed; boundary={}", &boundary)),
            Some(format!("--{}", &boundary))
        );
        let parsed = parse(format!("--{}", boundary).as_bytes(), body_string.as_bytes());
        assert!(parsed.is_ok());
        let parsed = parsed.unwrap();
        assert_eq!(parsed, vec![
            (input[0].0.clone(), input[0].1.as_slice()),
            (input[1].0.clone(), input[1].1.as_slice()),
            (input[2].0.clone(), input[2].1.as_slice()),
        ]);
    }
}