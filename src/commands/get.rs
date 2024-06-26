use crate::commands::executable::Executable;
use crate::commands::CommandParser;
use crate::frame::Frame;
use crate::store::Store;
use crate::Error;

/// Get the value of `key`. If the key does not exist the special value `nil` is returned.
///
/// Ref: <https://redis.io/docs/latest/commands/get/>
#[derive(Debug, PartialEq)]
pub struct Get {
    pub key: String,
}

impl Executable for Get {
    fn exec(self, store: Store) -> Result<Frame, Error> {
        let store = store.lock();
        let value = store.get(&self.key);

        match value {
            Some(value) => Ok(Frame::Bulk(value.clone())),
            None => Ok(Frame::NullBulkString),
        }
    }
}

impl TryFrom<&mut CommandParser> for Get {
    type Error = Error;

    fn try_from(parser: &mut CommandParser) -> Result<Self, Self::Error> {
        let key = parser.next_string()?;
        Ok(Self { key })
    }
}

#[cfg(test)]
mod tests {
    use bytes::Bytes;

    use super::*;
    use crate::commands::Command;

    #[tokio::test]
    async fn existing_key() {
        let store = Store::new();

        let frame = Frame::Array(vec![
            Frame::Bulk(Bytes::from("GET")),
            Frame::Bulk(Bytes::from("key1")),
        ]);
        let cmd = Command::try_from(frame).unwrap();

        assert_eq!(
            cmd,
            Command::Get(Get {
                key: String::from("key1")
            })
        );

        store.lock().set(String::from("key1"), Bytes::from("1"));

        let result = cmd.exec(store.clone()).unwrap();

        assert_eq!(result, Frame::Bulk(Bytes::from("1")));
    }

    #[tokio::test]
    async fn non_existing_key() {
        let store = Store::new();

        let frame = Frame::Array(vec![
            Frame::Bulk(Bytes::from("GET")),
            Frame::Bulk(Bytes::from("key1")),
        ]);
        let cmd = Command::try_from(frame).unwrap();

        assert_eq!(
            cmd,
            Command::Get(Get {
                key: String::from("key1")
            })
        );

        let res = cmd.exec(store.clone()).unwrap();

        assert_eq!(res, Frame::NullBulkString);
    }
}
