use bytes::Bytes;
use std::sync::{Arc, Mutex};

use crate::commands::executable::Executable;
use crate::commands::CommandParser;
use crate::frame::Frame;
use crate::store::Store;
use crate::Error;

/// Set `key` to hold the `string` value. If `key` already holds a value, it is overwritten.
///
/// Ref: <https://redis.io/docs/latest/commands/set/>
#[derive(Debug, PartialEq)]
pub struct Set {
    pub key: String,
    pub value: Bytes,
}

impl Executable for Set {
    fn exec(self, store: Arc<Mutex<Store>>) -> Result<Frame, Error> {
        let mut store = store.lock().unwrap();

        store.set(self.key, self.value);

        let res = Frame::Simple("OK".to_string());
        Ok(res)
    }
}

impl TryFrom<&mut CommandParser> for Set {
    type Error = Error;

    fn try_from(parser: &mut CommandParser) -> Result<Self, Self::Error> {
        let key = parser.next_string()?;
        let value = parser.next_bytes()?;

        Ok(Self { key, value })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::commands::Command;
    use bytes::Bytes;

    #[test]
    fn insert_one() {
        let frame = Frame::Array(vec![
            Frame::Bulk(Bytes::from("SET")),
            Frame::Bulk(Bytes::from("key1")),
            Frame::Bulk(Bytes::from("1")),
        ]);
        let cmd = Command::try_from(frame).unwrap();

        assert_eq!(
            cmd,
            Command::Set(Set {
                key: String::from("key1"),
                value: Bytes::from("1")
            })
        );

        let store = Arc::new(Mutex::new(Store::new()));

        cmd.exec(store.clone()).unwrap();

        assert_eq!(store.lock().unwrap().get("key1"), Some(&Bytes::from("1")));
    }

    #[test]
    fn overwrite_existing() {
        let frame = Frame::Array(vec![
            Frame::Bulk(Bytes::from("SET")),
            Frame::Bulk(Bytes::from("key1")),
            Frame::Bulk(Bytes::from("2")),
        ]);
        let cmd = Command::try_from(frame).unwrap();

        assert_eq!(
            cmd,
            Command::Set(Set {
                key: String::from("key1"),
                value: Bytes::from("2")
            })
        );

        let store = Arc::new(Mutex::new(Store::new()));
        {
            let mut store = store.lock().unwrap();
            store.set(String::from("key1"), Bytes::from("1"));
        }

        assert_eq!(store.lock().unwrap().get("key1"), Some(&Bytes::from("1")));

        cmd.exec(store.clone()).unwrap();

        assert_eq!(store.lock().unwrap().get("key1"), Some(&Bytes::from("2")));
    }
}
