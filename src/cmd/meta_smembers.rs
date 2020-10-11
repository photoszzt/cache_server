#![allow(clippy::string_lit_as_bytes)]
use crate::{Connection, Db, Frame, Parse};

// use bytes::Bytes;
use tracing::{debug, instrument};

/// Returns all the members of the set value stored at key.
#[derive(Debug)]
pub struct MetaSmembers {
    key: String,
}

impl MetaSmembers {
    // fn new(key: impl ToString) -> Self {
    //     MetaSmembers {
    //         key: key.to_string(),
    //     }
    // }

    /// Get the key
    pub fn key(&self) -> &str {
        &self.key
    }

    /// Parse a `Metasmembers` instance from a received frame.
    ///
    /// The `Parse` argument provides a cursor-like API to read fields from the
    /// `Frame`. At this point, the entire frame has already been received from
    /// the socket.
    ///
    /// The `MTSMEMBERS` string has already been consumed.
    ///
    /// # Returns
    ///
    /// Returns a `MetaSmembers` on success. If the frame is malformed, `Err` is
    /// returned.
    ///
    /// # Format
    ///
    /// Expects a frame containing a key.
    ///
    /// ```text
    /// MTSMEMBERS key
    /// ```
    pub(crate) fn parse_frames(parse: &mut Parse) -> crate::Result<MetaSmembers> {
        // Read the key to set. This is a required field
        let key = parse.next_string()?;
        Ok(MetaSmembers { key })
    }

    /// Apply the `MetaSmembers` command to the specified `Db` instance.
    ///
    /// The response is written to `dst`. This is called by the server in order
    /// to execute a received command.
    #[instrument(skip(self, db, dst))]
    pub(crate) async fn apply(self, db: &Db, dst: &mut Connection) -> crate::Result<()> {
        let members = db.meta_smembers(self.key());
        let mut response = Frame::array();
        for member in members.iter() {
            response.push_bulk(member.clone());
        }
        debug!(?response);
        dst.write_frame(&response).await?;
        Ok(())
    }

    // /// Converts the command into an equivalent `Frame`.
    // ///
    // /// This is called by the client when encoding a `MTSMEMBERS` command to send to
    // /// the server.
    // pub(crate) fn into_frame(self) -> Frame {
    //     let mut frame = Frame::array();
    //     frame.push_bulk(Bytes::from("mtsmembers".as_bytes()));
    //     frame.push_bulk(Bytes::from(self.key.into_bytes()));
    //     frame
    // }
}
