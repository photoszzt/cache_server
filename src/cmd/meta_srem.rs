#![allow(clippy::string_lit_as_bytes)]
use crate::{Connection, Db, Frame, Parse};

use bytes::Bytes;
use tracing::{debug, instrument};

/// Remove the specified members to the set stored at key.
///
/// Specified members that are already a member of this set are ignored.
/// If key does not exist, a new set is created before adding the specified members.
#[derive(Debug)]
pub struct MetaSrem {
    key: String,

    members: Vec<Bytes>,
}

impl MetaSrem {
    // pub fn new(key: impl ToString, members: Vec<Bytes>) -> MetaSrem {
    //     MetaSrem {
    //         key: key.to_string(),
    //         members,
    //     }
    // }

    /// Get the key
    pub fn key(&self) -> &str {
        &self.key
    }

    /// Get the members
    pub fn members(&self) -> &Vec<Bytes> {
        &self.members
    }

    /// Parse a `Metasrem` instance from a received frame.
    ///
    /// The `Parse` argument provides a cursor-like API to read fields from the
    /// `Frame`. At this point, the entire frame has already been received from
    /// the socket.
    ///
    /// The `MSREM` string has already been consumed.
    ///
    /// # Returns
    ///
    /// Returns a `MetaSrem` on success. If the frame is malformed, `Err` is
    /// returned.
    ///
    /// # Format
    ///
    /// Expects an array frame containing a key and one or a list of member(s).
    ///
    /// ```text
    /// MTSREM key member[member...]
    /// ```
    pub(crate) fn parse_frames(parse: &mut Parse) -> crate::Result<MetaSrem> {
        use crate::parse::Error::EndOfStream;

        // Read the key to set. This is a required field
        let key = parse.next_string()?;

        // Read the value to set. This is a required field.
        let member = parse.next_bytes()?;
        let mut members = Vec::new();
        members.push(member);
        loop {
            match parse.next_bytes() {
                Ok(m) => {
                    members.push(m);
                }
                // Finish reading all the keys
                Err(EndOfStream) => break,
                Err(err) => return Err(err.into()),
            }
        }
        Ok(MetaSrem { key, members })
    }

    /// Apply the `MetaSrem` command to the specified `Db` instance.
    ///
    /// The response is written to `dst`. This is called by the server in order
    /// to execute a received command.
    #[instrument(skip(self, db, dst))]
    pub(crate) async fn apply(self, db: &Db, dst: &mut Connection) -> crate::Result<()> {
        let count = db.meta_srem(self.key(), self.members());
        let response = Frame::Integer(count as i64);
        debug!(?response);
        dst.write_frame(&response).await?;
        Ok(())
    }

    // /// Converts the command into an equivalent `Frame`.
    // ///
    // /// This is called by the client when encoding a `MTSREM` command to send to
    // /// the server.
    // pub(crate) fn into_frame(self) -> Frame {
    //     let mut frame = Frame::array();
    //     frame.push_bulk(Bytes::from("mtsrem".as_bytes()));
    //     frame.push_bulk(Bytes::from(self.key.into_bytes()));
    //     for member in self.members.iter() {
    //         frame.push_bulk(member.clone());
    //     }
    //     frame
    // }
}
