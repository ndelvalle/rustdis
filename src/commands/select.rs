use bytes::Bytes;
use std::sync::{Arc, Mutex};

use crate::commands::executable::Executable;
use crate::commands::CommandParser;
use crate::frame::Frame;
use crate::store::Store;
use crate::Error;

/// Select the Redis logical database having the specified zero-based numeric index. New
/// connections always use the database 0.
///
/// Ref: <https://redis.io/docs/latest/commands/select>
#[derive(Debug, PartialEq)]
pub struct Select {
    /// The GUI clients we tested send this index value as bytes. Since we are not processing this
    /// value, there is no need to convert it to a number for now.
    pub index: Bytes,
}

impl Executable for Select {
    fn exec(self, _store: Arc<Mutex<Store>>) -> Result<Frame, Error> {
        Ok(Frame::Simple("OK".to_string()))
    }
}

impl TryFrom<&mut CommandParser> for Select {
    type Error = Error;

    fn try_from(parser: &mut CommandParser) -> Result<Self, Self::Error> {
        let index = parser.next_bytes()?;
        Ok(Self { index })
    }
}
