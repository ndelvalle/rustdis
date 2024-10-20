use crate::commands::executable::Executable;
use crate::commands::CommandParser;
use crate::frame::Frame;
use crate::store::Store;
use crate::Error;

/// Like TTL this command returns the remaining time to live of a key that has an expire set, with
/// the sole difference that TTL returns the amount of remaining time in seconds while PTTL returns
/// it in milliseconds.
///
/// Ref: <https://redis.io/docs/latest/commands/pttl/>
#[derive(Debug, PartialEq)]
pub struct Pttl {
    pub key: String,
}

impl Executable for Pttl {
    fn exec(self, store: Store) -> Result<Frame, Error> {
        let state = store.lock();
        let ttl = if state.exists(&self.key) { -1 } else { -2 };
        let ttl = state
            .get_ttl(&self.key)
            .map(|ttl| ttl.as_millis() as i64)
            .unwrap_or(ttl);
        Ok(Frame::Integer(ttl))
    }
}

impl TryFrom<&mut CommandParser> for Pttl {
    type Error = Error;

    fn try_from(parser: &mut CommandParser) -> Result<Self, Self::Error> {
        let key = parser.next_string()?;
        Ok(Self { key })
    }
}
