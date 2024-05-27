use bytes::Bytes;

use crate::commands::executable::Executable;
use crate::commands::{CommandParser, CommandParserError};
use crate::frame::Frame;
use crate::store::Store;
use crate::Error;

/// Returns PONG if no argument is provided, otherwise return a copy of the argument as a bulk.
///
/// Ref: <https://redis.io/docs/latest/commands/ping>
#[derive(Debug, PartialEq)]
pub struct Ping {
    pub payload: Option<Bytes>,
}

impl Executable for Ping {
    fn exec(self, _store: Store) -> Result<Frame, Error> {
        let res = self
            .payload
            .map_or(Frame::Bulk(Bytes::from("PONG")), Frame::Bulk);

        Ok(res)
    }
}

impl TryFrom<&mut CommandParser> for Ping {
    type Error = Error;

    fn try_from(parser: &mut CommandParser) -> Result<Self, Self::Error> {
        let payload = match parser.next_bytes() {
            Ok(payload) => Some(payload),
            Err(CommandParserError::EndOfStream) => None,
            Err(e) => return Err(e.into()),
        };

        Ok(Self { payload })
    }
}
