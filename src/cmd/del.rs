#![allow(clippy::string_lit_as_bytes)]
use crate::Parse;
use crate::{Connection, Db, Frame};
// use bytes::Bytes;
use tracing::{debug, instrument};

/// Delete the specified keys. A key is ignored if it does not exist.
///
/// Integer reply: The number of keys that were removed.
#[derive(Debug)]
pub struct Del {
    keys: Vec<String>,
}

impl Del {
    // /// Create a new `Del` command which deletes `key`s.
    // pub fn new(keys: Vec<String>) -> Del {
    //     Del { keys }
    // }

    // /// keys to delete
    // pub fn keys(&self) -> &Vec<String> {
    //     &self.keys
    // }

    /// Parse a `Del` instance from a received frame.
    ///
    /// The `Parse` argument provides a cursor-like API to read fields from the
    /// `Frame`. At this point, the entire frame has already been received from
    /// the socket.
    ///
    /// The `DEL` string has already been consumed.
    ///
    /// # Returns
    ///
    /// Returns the number of keys that were removed on success. If the frame is malformed, `Err` is
    /// returned.
    ///
    /// # Format
    ///
    /// Expects an array frame containing a list of keys.
    ///
    /// ```text
    /// DEL key [key...]
    /// ```
    pub(crate) fn parse_frames(parse: &mut Parse) -> crate::Result<Del> {
        use crate::parse::Error::EndOfStream;

        let key = parse.next_string()?;
        let mut keys = Vec::new();
        keys.push(key);

        loop {
            match parse.next_string() {
                Ok(s) => {
                    keys.push(s);
                }
                // Finish reading all the keys
                Err(EndOfStream) => break,
                Err(err) => return Err(err.into()),
            }
        }
        Ok(Del { keys })
    }

    /// Apply the `Del` command to the specified `Db` instance.
    ///
    /// The response is written to `dst`. This is called by the server in order
    /// to execute a received command.
    #[instrument(skip(self, db, dst))]
    pub(crate) async fn apply(self, db: &Db, dst: &mut Connection) -> crate::Result<()> {
        let mut count = 0;
        for key in self.keys.iter() {
            if db.remove(key).is_ok() {
                count += 1;
            }
        }
        let response = Frame::Integer(count);
        debug!(?response);
        dst.write_frame(&response).await?;
        Ok(())
    }

    // /// Converts the command into an equivalent `Frame`.
    // ///
    // /// This is called by the client when encoding a `Del` command to send to
    // /// the server.
    // pub(crate) fn into_frame(self) -> Frame {
    //     let mut frame = Frame::array();
    //     frame.push_bulk(Bytes::from("del".as_bytes()));
    //     for key in self.keys.iter() {
    //         frame.push_bulk(Bytes::from(key.clone().into_bytes()));
    //     }
    //     frame
    // }
}
