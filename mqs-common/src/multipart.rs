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
pub fn encode<I: Iterator<Item = (HeaderMap, Vec<u8>)>>(messages: I) -> (String, Vec<u8>) {
    let boundary = Uuid::new_v4().to_string();
    let full_boundary = format!("--{}", &boundary).into_bytes();
    let mut body = Vec::with_capacity(4096);

    for (headers, message) in messages {
        body.extend_from_slice(full_boundary.as_slice());
        body.extend_from_slice(b"\r\n");
        for (header_name, header_value) in &headers {
            body.extend_from_slice(header_name.as_str().as_bytes());
            body.extend_from_slice(b": ");
            body.extend_from_slice(header_value.as_bytes());
            body.extend_from_slice(b"\r\n");
        }
        body.extend_from_slice(b"\r\n");
        body.extend_from_slice(message.as_slice());
        body.extend_from_slice(b"\r\n");
    }
    body.extend_from_slice(full_boundary.as_slice());
    body.extend_from_slice(b"--");

    (boundary, body)
}

/// Return the boundary from "multipart/mixed; boundary=..."
///
/// ```
/// use mqs_common::multipart::is_multipart;
///
/// assert_eq!(
///     is_multipart("multipart/mixed; boundary=abc"),
///     Some("--abc".to_string())
/// );
/// assert_eq!(
///     is_multipart("multipart/mixed; boundary=\"abc def\""),
///     Some("--abc def".to_string())
/// );
/// assert_eq!(is_multipart("multipart/other; boundary=\"abc def\""), None);
/// assert_eq!(is_multipart("test/plain"), None);
/// ```
#[must_use]
pub fn is_multipart(content_type: &str) -> Option<String> {
    let (top, rest) = {
        let mut i = content_type.splitn(2, '/');
        let top = i.next();
        let rest = i.next();
        match (top, rest) {
            (Some(top), Some(rest)) => (top, rest),
            _ => return None,
        }
    };
    if top != "multipart" || !rest.starts_with("mixed;") {
        return None;
    }

    for param in rest.split(';') {
        let (key, value) = {
            let mut i = param.splitn(2, '=');
            let key = i.next();
            let value = i.next();
            match (key, value) {
                (Some(key), Some(value)) => (key.trim(), {
                    let trimmed = value.trim();
                    if trimmed.starts_with('"') && trimmed.ends_with('"') {
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

/// Error returned in case a slice is not a valid multipart document.
#[derive(Debug, Clone, Copy)]
pub enum InvalidMultipart {
    /// There was invalid data after a boundary.
    Chunk,
    /// An invalid header name was encountered in some chunk.
    HeaderName,
    /// An invalid header value was encountered in some chunk.
    HeaderValue,
}

impl Display for InvalidMultipart {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "{:?}", self)
    }
}

impl Error for InvalidMultipart {}

impl From<InvalidHeaderName> for InvalidMultipart {
    fn from(_: InvalidHeaderName) -> Self {
        Self::HeaderName
    }
}

impl From<InvalidHeaderValue> for InvalidMultipart {
    fn from(_: InvalidHeaderValue) -> Self {
        Self::HeaderValue
    }
}

/// Split a message body at the boundaries and return a list of content-type/data pairs
///
/// # Errors
///
/// If any part of the document fails to parse (invalid chunk, header name or header value).
pub fn parse<'a, 'b>(boundary: &'b [u8], body: &'a [u8]) -> Result<Vec<(HeaderMap, &'a [u8])>, InvalidMultipart> {
    let mut result = Vec::new();

    let mut skipped_preamble = false;
    for mut document in split(body, &Boundary { boundary }) {
        if !skipped_preamble {
            skipped_preamble = true;

            if document.starts_with(boundary) {
                document = &document[boundary.len()..];
            } else {
                continue;
            }
        }

        if document.starts_with(b"--") {
            // last boundary, stop processing
            break;
        }

        if !document.starts_with(b"\r\n") {
            // invalid chunk, signal bad request
            return Err(InvalidMultipart::Chunk);
        }

        // remove initial CRLF
        let doc: &[u8] = &document[2..];

        if doc.starts_with(b"\r\n") {
            // empty list of headers,
            result.push((HeaderMap::new(), &doc[2..]));
            continue;
        }

        if let Some((header_text, body)) = split_by(doc, b"\r\n\r\n".as_ref()) {
            let headers = {
                let mut headers = HeaderMap::new();
                for header in split(header_text, &HeaderValueSep {}) {
                    if let Some((name, body)) = split_by(header, &b':') {
                        headers.insert(HeaderName::from_bytes(name)?, to_header_value(body)?);
                    }
                }

                headers
            };

            result.push((headers, body));
        }
    }

    Ok(result)
}

struct Split<'a, 'b, M: Matcher + ?Sized> {
    data:     Option<&'a [u8]>,
    split_by: &'b M,
}

impl<'a, 'b, M: Matcher + ?Sized> Iterator for Split<'a, 'b, M> {
    type Item = &'a [u8];

    fn next(&mut self) -> Option<Self::Item> {
        self.data.map(|data| {
            if let Some((elem, rest)) = split_by(data, self.split_by) {
                self.data = Some(rest);
                elem
            } else {
                self.data = None;
                data
            }
        })
    }
}

const fn split<'a, 'b, M: Matcher + ?Sized>(data: &'a [u8], split_by: &'b M) -> Split<'a, 'b, M> {
    Split {
        data: Some(data),
        split_by,
    }
}

trait Matcher {
    fn min_len(&self) -> usize;

    fn does_match(&self, data: &[u8]) -> Option<usize>;
}

impl Matcher for u8 {
    fn min_len(&self) -> usize {
        1
    }

    fn does_match(&self, data: &[u8]) -> Option<usize> {
        if data.first() == Some(self) {
            Some(1)
        } else {
            None
        }
    }
}

impl Matcher for [u8] {
    fn min_len(&self) -> usize {
        self.len()
    }

    fn does_match(&self, data: &[u8]) -> Option<usize> {
        if data.starts_with(self) {
            Some(self.len())
        } else {
            None
        }
    }
}

struct HeaderValueSep {}

impl Matcher for HeaderValueSep {
    fn min_len(&self) -> usize {
        2
    }

    fn does_match(&self, data: &[u8]) -> Option<usize> {
        if data.starts_with(b"\r\n") && (data.len() <= 2 || (data[2] != 0x20 && data[2] != 0x09)) {
            Some(2)
        } else {
            None
        }
    }
}

fn to_header_value(value: &[u8]) -> Result<HeaderValue, InvalidHeaderValue> {
    let mut buffer = Vec::with_capacity(value.len());
    let mut i = 0;
    while i < value.len() {
        match skip_linear_whitespace(&value[i..]) {
            0 => {
                buffer.push(value[i]);
                i += 1;
            },
            skip => {
                buffer.push(b' ');
                i += skip;
            },
        }
    }

    HeaderValue::from_bytes(trim_bytes(buffer.as_slice()))
}

// split by CR LF + boundary + LWSP
struct Boundary<'a> {
    boundary: &'a [u8],
}

impl<'a> Matcher for Boundary<'a> {
    fn min_len(&self) -> usize {
        self.boundary.len() + 2
    }

    fn does_match(&self, data: &[u8]) -> Option<usize> {
        if !data.starts_with(b"\r\n") || !data[2..].starts_with(self.boundary) {
            return None;
        }

        Some(2 + self.boundary.len() + skip_linear_whitespace(&data[2 + self.boundary.len()..]))
    }
}

fn split_by<'a, 'b, M: Matcher + ?Sized>(data: &'a [u8], matcher: &'b M) -> Option<(&'a [u8], &'a [u8])> {
    if data.len() < matcher.min_len() {
        return None;
    }

    let mut i = 0;
    let end = data.len() - matcher.min_len();

    while i < end {
        match matcher.does_match(&data[i..]) {
            None => {
                i += 1;
            },
            Some(to_skip) => {
                return Some((&data[..i], &data[i + to_skip..]));
            },
        }
    }

    None
}

fn trim_bytes(data: &[u8]) -> &[u8] {
    let mut start = 0;
    let mut end = data.len() - 1;

    while start < end && data[start] == b' ' {
        start += 1;
    }
    while end > start && data[end] == b' ' {
        end -= 1;
    }

    &data[start..=end]
}

// How much linear whitespace should we skip from the beginning of the buffer (see rfc5234 for a definition)?
//
// LWSP    =  *(WSP / CRLF WSP)  ; Use of this linear-white-space rule permits
//                               ; lines containing only white space*
// WSP     =  SP / HTAB          ; white space
// CRLF    =  CR LF              ; Internet standard newline
// SP      =  %x20               ; space
// HTAB    =  %x09               ; horizontal tab
// CR      =  %x0D               ; carriage return
// LF      =  %x0A               ; linefeed
const fn skip_linear_whitespace(data: &[u8]) -> usize {
    let mut pos = 0;

    while data.len() > pos {
        match data[pos] {
            0x20 | 0x09 => pos += 1,
            0x0D => {
                if data.len() >= pos + 3 && data[pos + 1] == 0x0A && (data[pos + 2] == 0x20 || data[pos + 2] == 0x09) {
                    pos += 3;
                } else {
                    break;
                }
            },
            _ => break,
        }
    }

    pos
}

#[cfg(test)]
mod test {
    use super::*;
    use hyper::header::{CONTENT_ENCODING, CONTENT_TYPE};

    fn get_input() -> Vec<(HeaderMap, Vec<u8>)> {
        vec![
            (
                {
                    let mut m = HeaderMap::new();
                    m.insert(CONTENT_TYPE, HeaderValue::from_static("data/type"));
                    m
                },
                b"This is my first message".to_vec(),
            ),
            (
                {
                    let mut m = HeaderMap::new();
                    m.insert(CONTENT_TYPE, HeaderValue::from_static("data/another-type"));
                    m
                },
                b"\r\nAnother message\r\nWith more than one line\r\n".to_vec(),
            ),
            (
                {
                    let mut m = HeaderMap::new();
                    m.insert(CONTENT_TYPE, HeaderValue::from_static("foo/bar"));
                    m
                },
                b"Last message, don't forget it".to_vec(),
            ),
        ]
    }

    #[test]
    async fn split_by() {
        {
            let (a, b) = super::split_by(b"a:b", &b':').unwrap();
            assert_eq!(a, b"a");
            assert_eq!(b, b"b");
        }
        {
            assert_eq!(super::split_by(b"", &b':'), None);
        }
        {
            let (a, b) = super::split_by(b"asd:b", &b':').unwrap();
            assert_eq!(a, b"asd");
            assert_eq!(b, b"b");
        }
        {
            let (a, b) = super::split_by(b"asd:b:c", &b':').unwrap();
            assert_eq!(a, b"asd");
            assert_eq!(b, b"b:c");
        }
        {
            let (a, b) = super::split_by(b"a:b::c", b"::".as_ref()).unwrap();
            assert_eq!(a, b"a:b");
            assert_eq!(b, b"c");
        }
    }

    #[test]
    async fn trim_bytes() {
        assert_eq!(super::trim_bytes(b"asd"), b"asd");
        assert_eq!(super::trim_bytes(b"  asd"), b"asd");
        assert_eq!(super::trim_bytes(b"asd "), b"asd");
        assert_eq!(super::trim_bytes(b"  asd "), b"asd");
        assert_eq!(super::trim_bytes(b"  asd   asd  "), b"asd   asd");
    }

    #[test]
    async fn encode_multipart() {
        let (boundary, body) = encode(get_input().into_iter());
        assert_eq!(
            std::str::from_utf8(body.as_slice()).unwrap(),
            format!(
                "--{}\r\ncontent-type: data/type\r\n\r\nThis is my first message\r\n--{}\r\ncontent-type: data/another-type\r\n\r\n\r\nAnother message\r\nWith more than one line\r\n\r\n--{}\r\ncontent-type: foo/bar\r\n\r\nLast message, don\'t forget it\r\n--{}--",
                &boundary, &boundary, &boundary, &boundary
            )
        );
    }

    #[test]
    async fn is_multipart() {
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
    async fn parse_multipart() {
        let parsed = parse(
            b"--abc",
            b"ignore this\r\n--abc\r\nContent-Type: text/plain\r\n\r\nThis is my text\r\n--abc\r\n\r\nThis has no content type\r\n\r\n--abc--this is ignored",
        );
        assert!(parsed.is_ok());
        let parsed = parsed.unwrap();
        assert_eq!(parsed.len(), 2);
        assert_eq!(parsed[0].0, {
            let mut m = HeaderMap::new();
            m.insert(CONTENT_TYPE, HeaderValue::from_static("text/plain"));
            m
        });
        assert_eq!(parsed[0].1, b"This is my text");
        assert_eq!(parsed[1].0, HeaderMap::new());
        assert_eq!(parsed[1].1, b"This has no content type\r\n");
    }

    #[test]
    async fn parse_multipart_lwsp() {
        let parsed = parse(
            b"--abc",
            b"ignore this\r\n--abc   \r\nContent-Type: text/plain; \r\n charset=utf-8 \r\nContent-Encoding: identity\r\n\r\nThis is my text\r\n--abc\r\n \r\n\r\nThis has no content type\r\n\r\n--abc--this is ignored",
        );
        assert!(parsed.is_ok());
        let parsed = parsed.unwrap();
        assert_eq!(parsed.len(), 2);
        assert_eq!(parsed[0].0, {
            let mut m = HeaderMap::new();
            m.insert(CONTENT_TYPE, HeaderValue::from_static("text/plain; charset=utf-8"));
            m.insert(CONTENT_ENCODING, HeaderValue::from_static("identity"));
            m
        });
        assert_eq!(parsed[0].1, b"This is my text");
        assert_eq!(parsed[1].0, HeaderMap::new());
        assert_eq!(parsed[1].1, b"This has no content type\r\n");
    }

    #[test]
    async fn gen_and_parse() {
        let input = get_input();
        let (boundary, body) = encode(input.clone().into_iter());
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

    #[test]
    async fn skip_linear_whitespace() {
        assert_eq!(super::skip_linear_whitespace(b""), 0);
        assert_eq!(super::skip_linear_whitespace(b" "), 1);
        assert_eq!(super::skip_linear_whitespace(b"\t"), 1);
        assert_eq!(super::skip_linear_whitespace(b"\r\n "), 3);
        assert_eq!(super::skip_linear_whitespace(b"\r\n\t"), 3);
        assert_eq!(super::skip_linear_whitespace(b"\r\n"), 0);
        assert_eq!(super::skip_linear_whitespace(b"\r\na"), 0);
        assert_eq!(super::skip_linear_whitespace(b"   \r\n "), 6);
        assert_eq!(super::skip_linear_whitespace(b"   \t    asd"), 8);
        assert_eq!(super::skip_linear_whitespace(b"   \r\n    asd"), 9);
        assert_eq!(super::skip_linear_whitespace(b"\r\n \r\n   \r\n   asd"), 13);
        assert_eq!(super::skip_linear_whitespace(b"asd"), 0);
        assert_eq!(super::skip_linear_whitespace(b"       asd\r\n "), 7);
        assert_eq!(super::skip_linear_whitespace(b"    \r\n\r\n   asd"), 4);
        assert_eq!(super::skip_linear_whitespace(b"   \r\r\n   asd"), 3);
        assert_eq!(super::skip_linear_whitespace(b"  \n\r\n \n   asd"), 2);
    }
}
