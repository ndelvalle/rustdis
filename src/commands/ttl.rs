use std::sync::{Arc, Mutex};

use crate::commands::executable::Executable;
use crate::commands::CommandParser;
use crate::frame::Frame;
use crate::store::Store;
use crate::Error;

/// TTL returns the remaining time to live of a key that has a timeout. This introspection
/// capability allows a Redis client to check how many seconds a given key will continue to be part
/// of the dataset.
///
/// Ref: <https://redis.io/docs/latest/commands/ttl>
#[derive(Debug, PartialEq)]
pub struct Ttl {
    pub key: String,
}

impl Executable for Ttl {
    fn exec(self, store: Arc<Mutex<Store>>) -> Result<Frame, Error> {
        let store = store.lock().unwrap();
        let ttl = if store.exists(&self.key) { -1 } else { -2 };
        Ok(Frame::Integer(ttl))
    }
}

impl TryFrom<&mut CommandParser> for Ttl {
    type Error = Error;

    fn try_from(parser: &mut CommandParser) -> Result<Self, Self::Error> {
        let key = parser.next_string()?;
        Ok(Self { key })
    }
}
