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
        body.extend_from_slice("\r\n".as_bytes());
        for (header_name, header_value) in &headers {
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

#[derive(Debug, Clone, Copy)]
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
    for mut document in split(body, &Boundary { boundary }) {
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
                for header in split(header_text, &HeaderValueSep {}) {
                    if let Some((name, body)) = split_by(header, ":".as_bytes()) {
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

fn split<'a, 'b, M: Matcher + ?Sized>(data: &'a [u8], split_by: &'b M) -> Split<'a, 'b, M> {
    Split {
        data: Some(data),
        split_by,
    }
}

trait Matcher {
    fn min_len(&self) -> usize;

    fn does_match(&self, data: &[u8]) -> Option<usize>;
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
        if data.starts_with("\r\n".as_bytes()) && (data.len() <= 2 || (data[2] != 0x20 && data[2] != 0x09)) {
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
                buffer.push(' ' as u8);
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
        if !data.starts_with("\r\n".as_bytes()) || !(&data[2..]).starts_with(self.boundary) {
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

    while start < end && data[start] == ' ' as u8 {
        start += 1;
    }
    while end > start && data[end] == ' ' as u8 {
        end -= 1;
    }

    &data[start..end + 1]
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
fn skip_linear_whitespace(data: &[u8]) -> usize {
    let mut pos = 0;

    while data.len() > pos {
        match data[pos] {
            0x20 => pos += 1,
            0x09 => pos += 1,
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
                "This is my first message".as_bytes().to_vec(),
            ),
            (
                {
                    let mut m = HeaderMap::new();
                    m.insert(CONTENT_TYPE, HeaderValue::from_static("data/another-type"));
                    m
                },
                "\r\nAnother message\r\nWith more than one line\r\n".as_bytes().to_vec(),
            ),
            (
                {
                    let mut m = HeaderMap::new();
                    m.insert(CONTENT_TYPE, HeaderValue::from_static("foo/bar"));
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
        assert_eq!(parsed.len(), 2);
        assert_eq!(parsed[0].0, {
            let mut m = HeaderMap::new();
            m.insert(CONTENT_TYPE, HeaderValue::from_static("text/plain"));
            m
        });
        assert_eq!(parsed[0].1, "This is my text".as_bytes());
        assert_eq!(parsed[1].0, HeaderMap::new());
        assert_eq!(parsed[1].1, "This has no content type\r\n".as_bytes());
    }

    #[test]
    fn parse_multipart_lwsp() {
        let parsed = parse(
            "--abc".as_bytes(),
            "ignore this\r\n--abc   \r\nContent-Type: text/plain; \r\n charset=utf-8 \r\nContent-Encoding: identity\r\n\r\nThis is my text\r\n--abc\r\n \r\n\r\nThis has no content type\r\n\r\n--abc--this is ignored"
                .as_bytes(),
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
        assert_eq!(parsed[0].1, "This is my text".as_bytes());
        assert_eq!(parsed[1].0, HeaderMap::new());
        assert_eq!(parsed[1].1, "This has no content type\r\n".as_bytes());
    }

    #[test]
    fn gen_and_parse() {
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
    fn skip_linear_whitespace() {
        assert_eq!(super::skip_linear_whitespace("".as_bytes()), 0);
        assert_eq!(super::skip_linear_whitespace(" ".as_bytes()), 1);
        assert_eq!(super::skip_linear_whitespace("\t".as_bytes()), 1);
        assert_eq!(super::skip_linear_whitespace("\r\n ".as_bytes()), 3);
        assert_eq!(super::skip_linear_whitespace("\r\n\t".as_bytes()), 3);
        assert_eq!(super::skip_linear_whitespace("\r\n".as_bytes()), 0);
        assert_eq!(super::skip_linear_whitespace("\r\na".as_bytes()), 0);
        assert_eq!(super::skip_linear_whitespace("   \r\n ".as_bytes()), 6);
        assert_eq!(super::skip_linear_whitespace("   \t    asd".as_bytes()), 8);
        assert_eq!(super::skip_linear_whitespace("   \r\n    asd".as_bytes()), 9);
        assert_eq!(super::skip_linear_whitespace("\r\n \r\n   \r\n   asd".as_bytes()), 13);
        assert_eq!(super::skip_linear_whitespace("asd".as_bytes()), 0);
        assert_eq!(super::skip_linear_whitespace("       asd\r\n ".as_bytes()), 7);
        assert_eq!(super::skip_linear_whitespace("    \r\n\r\n   asd".as_bytes()), 4);
        assert_eq!(super::skip_linear_whitespace("   \r\r\n   asd".as_bytes()), 3);
        assert_eq!(super::skip_linear_whitespace("  \n\r\n \n   asd".as_bytes()), 2);
    }
}
