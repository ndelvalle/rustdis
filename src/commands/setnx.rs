use bytes::Bytes;

use crate::commands::executable::Executable;
use crate::commands::CommandParser;
use crate::frame::Frame;
use crate::store::Store;
use crate::Error;

/// Set key to hold string value if key does not exist. In that case, it is equal to SET. When key
/// already holds a value, no operation is performed. SETNX is short for "SET if Not eXists".
///
/// Ref: <https://redis.io/docs/latest/commands/setnx/>
#[derive(Debug, PartialEq)]
pub struct Setnx {
    pub key: String,
    pub value: Bytes,
}

impl Executable for Setnx {
    fn exec(self, store: Store) -> Result<Frame, Error> {
        let mut store = store.lock();

        let res = match store.get(&self.key) {
            Some(_) => Frame::Integer(0),
            None => {
                store.set(self.key, self.value);
                Frame::Integer(1)
            }
        };

        Ok(res)
    }
}

impl TryFrom<&mut CommandParser> for Setnx {
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
    async fn when_key_does_not_exists() {
        let store = Store::new();

        let frame = Frame::Array(vec![
            Frame::Bulk(Bytes::from("SETNX")),
            Frame::Bulk(Bytes::from("key1")),
            Frame::Bulk(Bytes::from("1")),
        ]);
        let cmd = Command::try_from(frame).unwrap();

        assert_eq!(
            cmd,
            Command::Setnx(Setnx {
                key: String::from("key1"),
                value: Bytes::from("1")
            })
        );

        let res = cmd.exec(store.clone()).unwrap();

        assert_eq!(res, Frame::Integer(1));
        assert_eq!(store.lock().get("key1"), Some(Bytes::from("1")));
    }

    #[tokio::test]
    async fn when_key_already_exists() {
        let store = Store::new();

        let frame = Frame::Array(vec![
            Frame::Bulk(Bytes::from("SETNX")),
            Frame::Bulk(Bytes::from("key1")),
            Frame::Bulk(Bytes::from("1")),
        ]);
        let cmd = Command::try_from(frame).unwrap();

        assert_eq!(
            cmd,
            Command::Setnx(Setnx {
                key: String::from("key1"),
                value: Bytes::from("1")
            })
        );

        store.lock().set(String::from("key1"), Bytes::from("1"));

        let res = cmd.exec(store.clone()).unwrap();

        assert_eq!(res, Frame::Integer(0));
        assert_eq!(store.lock().get("key1"), Some(Bytes::from("1")));
    }
}
