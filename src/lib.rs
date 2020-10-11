use std::io;
use thiserror::Error;

mod cache;
use cache::Db;

mod cmd;
use cmd::Command;
pub mod lrucache;

mod connection;
use connection::Connection;

mod frame;
use frame::Frame;

pub mod server;

mod shutdown;
use shutdown::Shutdown;

mod parse;
use parse::Parse;

/// Default port that a redis server listens on.
///
/// Used if no port is specified.
pub const DEFAULT_PORT: &str = "6379";

/// Errors returned by the cache.
#[derive(Debug, Error)]
pub enum Error {
    /// A Parse Error occurred.
    #[error(transparent)]
    ParseError(#[from] parse::Error),
    #[error(transparent)]
    FrameError(#[from] frame::Error),
    /// The key was not in the cache.
    #[error("key is not in cache")]
    KeyNotFound,
    /// An IO Error occurred.
    #[error(transparent)]
    Io(#[from] io::Error),

    #[error(transparent)]
    Other(#[from] anyhow::Error),
}

/// A convenience `Result` type
pub type Result<T> = std::result::Result<T, Error>;
