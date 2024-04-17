use bytes::Bytes;
use std::sync::{Arc, Mutex};

use std::{str, vec};

use crate::commands::executable::Executable;
use crate::commands::CommandParser;
use crate::frame::Frame;
use crate::store::Store;
use crate::Error;

/// The SCAN command is used in order to incrementally iterate over a collection of elements.
///
/// Ref: <https://redis.io/docs/latest/commands/scan>
#[derive(Debug, PartialEq)]
pub struct Scan {
    pub cursor: i64,
}

impl Executable for Scan {
    fn exec(self, store: Arc<Mutex<Store>>) -> Result<Frame, Error> {
        let mut store = store.lock().unwrap();

        let res = Frame::Bulk(Bytes::from("0"));
        Ok(res)
    }
}

impl TryFrom<&mut CommandParser> for Scan {
    type Error = Error;

    fn try_from(parser: &mut CommandParser) -> Result<Self, Self::Error> {
        let cursor = parser.next_bytes()?;
        let cursor = str::from_utf8(&cursor[..]).unwrap();
        let cursor = cursor.parse::<i64>().unwrap();
        Ok(Self { cursor })
    }
}
