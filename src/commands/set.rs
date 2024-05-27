use bytes::Bytes;

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
    fn exec(self, store: Store) -> Result<Frame, Error> {
        let mut store = store.lock();

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
    use bytes::Bytes;

    use super::*;
    use crate::commands::Command;

    #[tokio::test]
    async fn insert_one() {
        let store = Store::new();

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

        let res = cmd.exec(store.clone()).unwrap();

        assert_eq!(res, Frame::Simple("OK".to_string()));
        assert_eq!(store.lock().get("key1"), Some(Bytes::from("1")));
    }

    #[tokio::test]
    async fn overwrite_existing() {
        let store = Store::new();

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

        store.lock().set(String::from("key1"), Bytes::from("1"));

        assert_eq!(store.lock().get("key1"), Some(Bytes::from("1")));

        let res = cmd.exec(store.clone()).unwrap();

        assert_eq!(res, Frame::Simple("OK".to_string()));
        assert_eq!(store.lock().get("key1"), Some(Bytes::from("2")));
    }
}
