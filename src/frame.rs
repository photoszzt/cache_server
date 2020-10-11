//! Provides a type representing a Redis protocol frame as well as utilities for
//! parsing frames from a byte array.

use bytes::{Buf, Bytes, BytesMut};
use memchr::memchr;
use std::convert::TryInto;
use std::fmt;
use std::io::Cursor;
use std::num::TryFromIntError;
use std::str;
use std::string::FromUtf8Error;
use thiserror::Error;

/// A frame in the Redis protocol.
#[derive(Clone, Debug, PartialEq)]
pub enum Frame {
    Simple(String),
    Error(String),
    Integer(i64),
    Bulk(Bytes),
    Null,
    Array(Vec<Frame>),
}

#[derive(Debug, Error)]
pub enum Error {
    /// Not enough data is available to parse a message
    #[error("stream ended early")]
    Incomplete,

    /// Invalid type byte
    #[error("invalid frame type byte `{0}`")]
    InvalidTypeByte(u8),

    /// Invalid frame format
    #[error("invalid frame format")]
    InvalidFrameFormat,
    #[error("unexpected frame: {0:?}")]
    UnexpectedFrame(Frame),
    #[error("invalid Frame format")]
    FromUtf8Err(#[from] FromUtf8Error),
    #[error("invalid simple string")]
    Utf8Err(#[from] std::str::Utf8Error),
    #[error("invalid Frame format")]
    TryFromIntErr(#[from] TryFromIntError),

    /// Invalid message encoding
    #[error("other error")]
    Other(String),
}

/// The first usize is the start and the
/// second usize is the end of the bytes
#[derive(Clone, Debug)]
struct BufSplit(usize, usize);

impl BufSplit {
    /// Get a lifetime appropriate slice of the underlying buffer.
    ///
    /// Constant time.
    #[inline]
    fn as_slice<'a>(&self, buf: &'a BytesMut) -> &'a [u8] {
        &buf[self.0..self.1]
    }

    /// Get a Bytes object representing the appropriate slice
    /// of bytes.
    ///
    /// Constant time.
    #[inline]
    fn as_bytes(&self, buf: &Bytes) -> Bytes {
        buf.slice(self.0..self.1)
    }
}

#[derive(Clone)]
enum RedisBufSplit {
    Simple(BufSplit),
    Bulk(BufSplit),
    Error(BufSplit),
    Int(i64),
}

impl RedisBufSplit {
    fn frame(self, buf: &Bytes) -> Result<Frame, Error> {
        match self {
            RedisBufSplit::Simple(bfs) => {
                let simple = std::str::from_utf8(bfs.as_bytes(buf).as_ref())?.to_string();
                Ok(Frame::Simple(simple))
            }
            RedisBufSplit::Bulk(bfs) => Ok(Frame::Bulk(bfs.as_bytes(buf))),
            RedisBufSplit::Int(i) => Ok(Frame::Integer(i)),
            RedisBufSplit::Error(bfs) => {
                let err_str = std::str::from_utf8(bfs.as_bytes(buf).as_ref())?.to_string();
                Ok(Frame::Error(err_str))
            }
        }
    }
}

impl Frame {
    /// Returns an empty array
    pub(crate) fn array() -> Frame {
        Frame::Array(vec![])
    }

    /// Push a "bulk" frame into the array. `self` must be an Array frame.
    ///
    /// # Panics
    ///
    /// panics if `self` is not an array
    pub(crate) fn push_bulk(&mut self, bytes: Bytes) {
        match self {
            Frame::Array(vec) => {
                vec.push(Frame::Bulk(bytes));
            }
            _ => panic!("not an array frame"),
        }
    }

    /// Push an "integer" frame into the array. `self` must be an Array frame.
    ///
    /// # Panics
    ///
    /// panics if `self` is not an array
    pub(crate) fn push_int(&mut self, value: i64) {
        match self {
            Frame::Array(vec) => {
                vec.push(Frame::Integer(value));
            }
            _ => panic!("not an array frame"),
        }
    }

    /// Checks if an entire message can be decoded from `src`
    pub fn check(src: &mut Cursor<&[u8]>) -> Result<(), Error> {
        match get_u8(src)? {
            b'+' => {
                get_line(src)?;
                Ok(())
            }
            b'-' => {
                get_line(src)?;
                Ok(())
            }
            b':' => {
                let _ = get_decimal(src)?;
                Ok(())
            }
            b'$' => {
                if b'-' == peek_u8(src)? {
                    // Skip '-1\r\n'
                    skip(src, 4)
                } else {
                    // Read the bulk string
                    let len: usize = get_decimal(src)?.try_into()?;

                    // skip that number of bytes + 2 (\r\n).
                    skip(src, len + 2)
                }
            }
            b'*' => {
                let len = get_decimal(src)?;

                for _ in 0..len {
                    Frame::check(src)?;
                }

                Ok(())
            }
            actual => Err(Error::InvalidTypeByte(actual)),
        }
    }

    pub fn check_with_pos(src: &BytesMut, pos: usize) -> Result<usize, Error> {
        match get_u8_with_pos(src, pos)? {
            (pos, b'+') => {
                let (pos, _bfs) = get_line_with_pos(src, pos)?;
                Ok(pos)
            }
            (pos, b'-') => {
                let (pos, _bfs) = get_line_with_pos(src, pos)?;
                Ok(pos)
            }
            (pos, b':') => {
                let (pos, _bfs) = get_line_with_pos(src, pos)?;
                Ok(pos)
            }
            (pos, b'$') => {
                if b'-' == src[pos] {
                    let (pos, _bfs) = get_line_with_pos(src, pos)?;
                    Ok(pos)
                } else {
                    let (pos, len) = get_decimal_with_pos(src, pos)?;
                    skip_with_pos(src, pos, (len + 2) as usize)
                }
            }
            (pos, b'*') => {
                let (mut pos, len) = get_decimal_with_pos(src, pos)?;
                for _ in 0..len {
                    pos = Frame::check_with_pos(src, pos)?;
                }
                Ok(pos)
            }
            (_, actual) => Err(Error::InvalidTypeByte(actual)),
        }
    }

    /// The message has already been validated with `check`.
    pub fn parse(src: &mut Cursor<&[u8]>) -> Result<Frame, Error> {
        match get_u8(src)? {
            b'+' => {
                // Read the line and convert it to `Vec<u8>`
                let line = get_line(src)?.to_vec();

                // Convert the line to a String
                let string = String::from_utf8(line)?;

                Ok(Frame::Simple(string))
            }
            b'-' => {
                // Read the line and convert it to `Vec<u8>`
                let line = get_line(src)?.to_vec();

                // Convert the line to a String
                let string = String::from_utf8(line)?;

                Ok(Frame::Error(string))
            }
            b':' => {
                let len = get_decimal(src)?;
                Ok(Frame::Integer(len))
            }
            b'$' => {
                if b'-' == peek_u8(src)? {
                    let line = get_line(src)?;

                    if line != b"-1" {
                        return Err(Error::InvalidFrameFormat);
                    }

                    Ok(Frame::Null)
                } else {
                    // Read the bulk string
                    let len = get_decimal(src)?.try_into()?;
                    let n = len + 2;

                    if src.remaining() < n {
                        return Err(Error::Incomplete);
                    }

                    let data = Bytes::copy_from_slice(&src.bytes()[..len]);

                    // skip that number of bytes + 2 (\r\n).
                    skip(src, n)?;

                    Ok(Frame::Bulk(data))
                }
            }
            b'*' => {
                let len = get_decimal(src)?.try_into()?;
                let mut out = Vec::with_capacity(len);

                for _ in 0..len {
                    out.push(Frame::parse(src)?);
                }

                Ok(Frame::Array(out))
            }
            _ => unimplemented!(),
        }
    }

    pub(crate) fn parse_with_pos(src: &mut BytesMut, pos: usize) -> Result<Frame, Error> {
        match get_u8_with_pos(src, pos)? {
            (pos, b'+') => {
                let (pos, bfs) = get_line_with_pos(src, pos)?;
                let rbfs = RedisBufSplit::Simple(bfs);
                let our_data = src.split_to(pos);
                rbfs.frame(&our_data.freeze())
            }
            (pos, b'-') => {
                let (pos, bfs) = get_line_with_pos(src, pos)?;
                let rbfs = RedisBufSplit::Error(bfs);
                let our_data = src.split_to(pos);
                rbfs.frame(&our_data.freeze())
            }
            (pos, b':') => {
                let (pos, bfs) = get_decimal_with_pos(src, pos)?;
                let rbfs = RedisBufSplit::Int(bfs);
                let our_data = src.split_to(pos);
                rbfs.frame(&our_data.freeze())
            }
            (pos, b'$') => {
                if b'-' == src[pos] {
                    let (pos, bfs) = get_line_with_pos(src, pos)?;
                    let bfs_slice = bfs.as_slice(src);
                    if bfs_slice != b"-1" {
                        return Err(Error::InvalidFrameFormat);
                    }
                    src.advance(pos);
                    Ok(Frame::Null)
                } else {
                    let (pos, len) = get_decimal_with_pos(src, pos)?;
                    let n = len + 2;
                    if src.len() < pos + n as usize {
                        return Err(Error::Incomplete);
                    }
                    let rbfs = RedisBufSplit::Bulk(BufSplit(pos, pos + len as usize));
                    let our_data = src.split_to(pos + n as usize);
                    rbfs.frame(&our_data.freeze())
                }
            }
            (pos, b'*') => {
                let (pos, len) = get_decimal_with_pos(src, pos)?;
                let mut out = Vec::with_capacity(len as usize);
                for i in 0..len {
                    let value = if i == 0 {
                        Frame::parse_with_pos(src, pos)?
                    } else {
                        Frame::parse_with_pos(src, 0)?
                    };
                    out.push(value);
                }
                Ok(Frame::Array(out))
            }
            (pos, actual) => Err(Error::Other(format!(
                "unrecognized byte: pos: {}, byte: {}, src: {:?}",
                pos, actual, src
            ))),
        }
    }

    // /// Converts the frame to an "unexpected frame" error
    // pub(crate) fn to_error(&self) -> Error {
    //     Error::Other(format!("unexpected frame: {}", self))
    // }
}

impl PartialEq<&str> for Frame {
    fn eq(&self, other: &&str) -> bool {
        match self {
            Frame::Simple(s) => s.eq(other),
            Frame::Bulk(s) => s.eq(other),
            _ => false,
        }
    }
}

impl fmt::Display for Frame {
    fn fmt(&self, fmt: &mut fmt::Formatter) -> fmt::Result {
        match self {
            Frame::Simple(response) => response.fmt(fmt),
            Frame::Error(msg) => write!(fmt, "error: {}", msg),
            Frame::Integer(num) => num.fmt(fmt),
            Frame::Bulk(msg) => match str::from_utf8(msg) {
                Ok(string) => string.fmt(fmt),
                Err(_) => write!(fmt, "{:?}", msg),
            },
            Frame::Null => "(nil)".fmt(fmt),
            Frame::Array(parts) => {
                for (i, part) in parts.iter().enumerate() {
                    if i > 0 {
                        write!(fmt, " ")?;
                        part.fmt(fmt)?;
                    }
                }

                Ok(())
            }
        }
    }
}

fn peek_u8(src: &mut Cursor<&[u8]>) -> Result<u8, Error> {
    if !src.has_remaining() {
        return Err(Error::Incomplete);
    }

    Ok(src.bytes()[0])
}

fn get_u8(src: &mut Cursor<&[u8]>) -> Result<u8, Error> {
    if !src.has_remaining() {
        return Err(Error::Incomplete);
    }

    Ok(src.get_u8())
}

fn get_u8_with_pos(buf: &BytesMut, pos: usize) -> Result<(usize, u8), Error> {
    if buf.len() <= pos {
        return Err(Error::Incomplete);
    }
    Ok((pos + 1, buf[pos]))
}

fn skip(src: &mut Cursor<&[u8]>, n: usize) -> Result<(), Error> {
    if src.remaining() < n {
        return Err(Error::Incomplete);
    }

    src.advance(n);
    Ok(())
}

fn skip_with_pos(buf: &BytesMut, pos: usize, n: usize) -> Result<usize, Error> {
    if buf.len() <= pos || buf.len() < pos + n {
        return Err(Error::Incomplete);
    }
    Ok(pos + n)
}

/// Read a new-line terminated decimal
fn get_decimal(src: &mut Cursor<&[u8]>) -> Result<i64, Error> {
    let line = get_line(src)?;
    let s = str::from_utf8(line).map_err(|_| Error::InvalidFrameFormat)?;
    let i = s.parse().map_err(|_| Error::InvalidFrameFormat)?;
    Ok(i)
}

fn get_decimal_with_pos(buf: &BytesMut, pos: usize) -> Result<(usize, i64), Error> {
    let (pos, line) = get_line_with_pos(buf, pos)?;
    let s = str::from_utf8(line.as_slice(buf)).map_err(|_| Error::InvalidFrameFormat)?;
    let i = s.parse().map_err(|_| Error::InvalidFrameFormat)?;
    Ok((pos, i))
}

/// Find a line
fn get_line<'a>(src: &mut Cursor<&'a [u8]>) -> Result<&'a [u8], Error> {
    // Scan the bytes directly
    let start = src.position() as usize;
    // Scan to the second to last byte
    let end = src.get_ref().len() - 1;

    for i in start..end {
        if src.get_ref()[i] == b'\r' && src.get_ref()[i + 1] == b'\n' {
            // We found a line, update the position to be *after* the \n
            src.set_position((i + 2) as u64);

            // Return the line
            return Ok(&src.get_ref()[start..i]);
        }
    }

    Err(Error::Incomplete)
}

fn get_line_with_pos(buf: &BytesMut, pos: usize) -> Result<(usize, BufSplit), Error> {
    if buf.len() <= pos {
        return Err(Error::Incomplete);
    }
    memchr(b'\r', &buf[pos..])
        .and_then(|end| {
            if let Some(val) = buf.get(pos + end + 1) {
                if *val == b'\n' {
                    Some((pos + end + 2, BufSplit(pos, pos + end)))
                } else {
                    None
                }
            } else {
                None
            }
        })
        .ok_or(Error::Incomplete)
}

#[cfg(test)]
mod resp_parser_tests {
    use crate::frame::Error;
    use crate::frame::Frame;
    use bytes::{Bytes, BytesMut};

    fn generic_test(input: &str, output: Frame) {
        let src = &mut BytesMut::from(input);
        let pos = Frame::check_with_pos(src, 0).unwrap();
        assert_eq!(input.len(), pos);
        let frame = Frame::parse_with_pos(src, 0).unwrap();
        assert_eq!(output, frame);
    }

    fn generic_test_arr(input: &str, output: Vec<Frame>) {
        // TODO: Try to make this occur randomly
        let first: usize = input.len() / 2;
        let second = input.len() - first;
        let mut first = BytesMut::from(&input[0..=first]);
        let mut second = Some(BytesMut::from(&input[second..]));

        let mut res: Vec<Frame> = Vec::new();
        loop {
            match Frame::parse_with_pos(&mut first, 0) {
                Ok(frame) => {
                    res.push(frame);
                    break;
                }
                Err(e) => match e {
                    Error::Incomplete => {
                        if let None = second {
                            panic!("Test expected more bytes than expected!");
                        }
                        first.extend(second.unwrap());
                        second = None;
                    }
                    _ => panic!("Should not error, {:?}", e),
                },
            }
        }
        if let Some(second) = second {
            first.extend(second);
        }
        loop {
            match Frame::parse_with_pos(&mut first, 0) {
                Ok(frame) => {
                    res.push(frame);
                    break;
                }
                Err(e) => match e {
                    Error::Incomplete => break,
                    _ => panic!("Should not error, {:?}", e),
                },
            }
        }
        assert_eq!(output, res);
    }

    fn ezs() -> Bytes {
        Bytes::from_static(b"hello")
    }
    #[test]
    fn test_simple_string() {
        let t = Frame::Simple("hello".to_string());
        let s = "+hello\r\n";
        generic_test(s, t);
        let t0 = Frame::Simple("hello".to_string());
        let t1 = Frame::Simple("abcdefghijklmnopqrstuvwxyz".to_string());
        let s = "+hello\r\n+abcdefghijklmnopqrstuvwxyz\r\n";
        generic_test_arr(s, vec![t0, t1]);
    }

    #[test]
    fn test_error() {
        let t = Frame::Error("hello".to_string());
        let s = "-hello\r\n";
        generic_test(s, t);

        let t0 = Frame::Error("abcdefghijklmnopqrstuvwxyz".to_string());
        let t1 = Frame::Error(String::from_utf8(ezs().as_ref().to_vec()).unwrap());
        let s = "-abcdefghijklmnopqrstuvwxyz\r\n-hello\r\n";
        generic_test_arr(s, vec![t0, t1]);
    }

    #[test]
    fn test_bulk_string() {
        let t = Frame::Bulk(ezs());
        let s = "$5\r\nhello\r\n";
        generic_test(s, t);

        let t = Frame::Bulk(Bytes::from_static(b""));
        let s = "$0\r\n\r\n";
        generic_test(s, t);
    }

    #[test]
    fn test_int() {
        let t = Frame::Integer(0);
        let s = ":0\r\n";
        generic_test(s, t);

        let t = Frame::Integer(123);
        let s = ":123\r\n";
        generic_test(s, t);

        let t = Frame::Integer(-123);
        let s = ":-123\r\n";
        generic_test(s, t);
    }

    #[test]
    fn test_array() {
        let t = Frame::Array(vec![]);
        let s = "*0\r\n";
        generic_test(s, t);

        let inner = vec![
            Frame::Bulk(Bytes::from_static(b"foo")),
            Frame::Bulk(Bytes::from_static(b"bar")),
        ];
        let t = Frame::Array(inner);
        let s = "*2\r\n$3\r\nfoo\r\n$3\r\nbar\r\n";
        generic_test(s, t);

        let inner = vec![Frame::Integer(1), Frame::Integer(2), Frame::Integer(3)];
        let t = Frame::Array(inner);
        let s = "*3\r\n:1\r\n:2\r\n:3\r\n";
        generic_test(s, t);

        let inner = vec![
            Frame::Integer(1),
            Frame::Integer(2),
            Frame::Integer(3),
            Frame::Integer(4),
            Frame::Bulk(Bytes::from_static(b"foobar")),
        ];
        let t = Frame::Array(inner);
        let s = "*5\r\n:1\r\n:2\r\n:3\r\n:4\r\n$6\r\nfoobar\r\n";
        generic_test(s, t);

        let inner = vec![
            Frame::Array(vec![
                Frame::Integer(1),
                Frame::Integer(2),
                Frame::Integer(3),
            ]),
            Frame::Array(vec![
                Frame::Bulk(Bytes::from_static(b"Foo")),
                Frame::Error("Bar".to_string()),
            ]),
        ];
        let t = Frame::Array(inner);
        let s = "*2\r\n*3\r\n:1\r\n:2\r\n:3\r\n*2\r\n$3\r\nFoo\r\n-Bar\r\n";
        generic_test(s, t);

        let inner = vec![
            Frame::Bulk(Bytes::from_static(b"foo")),
            Frame::Null,
            Frame::Bulk(Bytes::from_static(b"bar")),
        ];
        let t = Frame::Array(inner);
        let s = "*3\r\n$3\r\nfoo\r\n$-1\r\n$3\r\nbar\r\n";
        generic_test(s, t);
    }
}
