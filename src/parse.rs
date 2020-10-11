use crate::Frame;

use bytes::Bytes;
use std::{str, vec};
use thiserror::Error;

/// Utility for parsing a command
///
/// Commands are represented as array frames. Each entry in the frame is a
/// "token". A `Parse` is initialized with the array frame and provides a
/// cursor-like API. Each command struct includes a `parse_frame` method that
/// uses a `Parse` to extract its fields.
#[derive(Debug)]
pub(crate) struct Parse {
    /// Array frame iterator.
    parts: vec::IntoIter<Frame>,
}

/// Error encountered while parsing a frame.
///
/// Only `EndOfStream` errors are handled at runtime. All other errors result in
/// the connection being terminated.
#[derive(Debug, Error)]
pub enum Error {
    /// Attempting to extract a value failed due to the frame being fully
    /// consumed.
    #[error("unexpected end of stream")]
    EndOfStream,

    #[error("expected array, got {0:?}")]
    ExpectArray(Frame),

    #[error("expected simple frame or bulk frame, got {0:?}")]
    ExpectSimpleOrBulk(Frame),

    #[error("invalid string, should be utf-8.")]
    InvalidUtf8String,

    #[error("expected int frame but got {0:?}")]
    ExpectInt(Frame),

    #[error("expected end of frame, but there was more")]
    ExpectEnd,

    #[error("invalid number")]
    InvalidNumber,
}

impl Parse {
    /// Create a new `Parse` to parse the contents of `frame`.
    ///
    /// Returns `Err` if `frame` is not an array frame.
    pub(crate) fn new(frame: Frame) -> Result<Parse, Error> {
        let array = match frame {
            Frame::Array(array) => array,
            frame => return Err(Error::ExpectArray(frame)),
        };

        Ok(Parse {
            parts: array.into_iter(),
        })
    }

    /// Return the next entry. Array frames are arrays of frames, so the next
    /// entry is a frame.
    fn next(&mut self) -> Result<Frame, Error> {
        self.parts.next().ok_or(Error::EndOfStream)
    }

    /// Return the next entry as a string.
    ///
    /// If the next entry cannot be represented as a String, then an error is returned.
    pub(crate) fn next_string(&mut self) -> Result<String, Error> {
        match self.next()? {
            // Both `Simple` and `Bulk` representation may be strings. Strings
            // are parsed to UTF-8.
            //
            // While errors are stored as strings, they are considered separate
            // types.
            Frame::Simple(s) => Ok(s),
            Frame::Bulk(data) => str::from_utf8(&data[..])
                .map(|s| s.to_string())
                .map_err(|_| Error::InvalidUtf8String),
            frame => Err(Error::ExpectSimpleOrBulk(frame)),
        }
    }

    /// Return the next entry as raw bytes.
    ///
    /// If the next entry cannot be represented as raw bytes, an error is
    /// returned.
    pub(crate) fn next_bytes(&mut self) -> Result<Bytes, Error> {
        match self.next()? {
            // Both `Simple` and `Bulk` representation may be raw bytes.
            //
            // Although errors are stored as strings and could be represented as
            // raw bytes, they are considered separate types.
            Frame::Simple(s) => Ok(Bytes::from(s.into_bytes())),
            Frame::Bulk(data) => Ok(data),
            frame => Err(Error::ExpectSimpleOrBulk(frame)),
        }
    }

    /// Return the next entry as an integer.
    ///
    /// This includes `Simple`, `Bulk`, and `Integer` frame types. `Simple` and
    /// `Bulk` frame types are parsed.
    ///
    /// If the next entry cannot be represented as an integer, then an error is
    /// returned.
    pub(crate) fn next_int(&mut self) -> Result<i64, Error> {
        match self.next()? {
            // An integer frame type is already stored as an integer.
            Frame::Integer(v) => Ok(v),
            // Simple and bulk frames must be parsed as integers. If the parsing
            // fails, an error is returned.
            Frame::Simple(data) => {
                let s = std::str::from_utf8(data.as_bytes()).map_err(|_| Error::InvalidNumber)?;
                let i = s.parse().map_err(|_| Error::InvalidNumber)?;
                Ok(i)
            }
            Frame::Bulk(data) => {
                let s = std::str::from_utf8(&data).map_err(|_| Error::InvalidNumber)?;
                let i = s.parse().map_err(|_| Error::InvalidNumber)?;
                Ok(i)
            }
            frame => Err(Error::ExpectInt(frame)),
        }
    }

    /// Ensure there are no more entries in the array
    pub(crate) fn finish(&mut self) -> Result<(), Error> {
        if self.parts.next().is_none() {
            Ok(())
        } else {
            Err(Error::ExpectEnd)
        }
    }
}
