use bytes::{Bytes, BytesMut};
use std::sync::{Arc, Mutex};

use crate::commands::executable::Executable;
use crate::commands::CommandParser;
use crate::frame::Frame;
use crate::store::Store;
use crate::Error;

/// If key already exists and is a string, this command appends the value at the end of the string.
/// If key does not exist it is created and set as an empty string, so APPEND will be similar to
/// SET in this special case.
///
/// Ref: <https://redis.io/docs/latest/commands/append>
#[derive(Debug, PartialEq)]
pub struct Append {
    pub key: String,
    pub value: Bytes,
}

impl Executable for Append {
    fn exec(self, store: Arc<Mutex<Store>>) -> Result<Frame, Error> {
        let mut store = store.lock().unwrap();

        let len = match store.get(&self.key) {
            Some(bytes) => {
                let new_len = bytes.len() + self.value.len();
                let mut new_value = BytesMut::with_capacity(new_len);

                new_value.extend_from_slice(bytes);
                new_value.extend_from_slice(&self.value);

                store.set(self.key, new_value.freeze());
                new_len
            }
            None => {
                let len = self.value.len();
                store.set(self.key, self.value);
                len
            }
        };

        let res = Frame::Integer(len as i64);
        Ok(res)
    }
}

impl TryFrom<&mut CommandParser> for Append {
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

    #[tokio::test]
    async fn when_key_does_not_exists() {
        let store = Arc::new(Mutex::new(Store::new()));

        let frame = Frame::Array(vec![
            Frame::Bulk(Bytes::from("APPEND")),
            Frame::Bulk(Bytes::from("foo")),
            Frame::Bulk(Bytes::from("baz")),
        ]);
        let cmd = Command::try_from(frame).unwrap();

        assert_eq!(
            cmd,
            Command::Append(Append {
                key: String::from("foo"),
                value: Bytes::from("baz")
            })
        );

        let res = cmd.exec(store.clone()).unwrap();

        assert_eq!(res, Frame::Integer(3));
        assert_eq!(store.lock().unwrap().get("foo"), Some(&Bytes::from("baz")));
    }

    #[tokio::test]
    async fn when_key_exists() {
        let store = Arc::new(Mutex::new(Store::new()));

        let frame = Frame::Array(vec![
            Frame::Bulk(Bytes::from("APPEND")),
            Frame::Bulk(Bytes::from("key1")),
            Frame::Bulk(Bytes::from("world")),
        ]);
        let cmd = Command::try_from(frame).unwrap();

        assert_eq!(
            cmd,
            Command::Append(Append {
                key: String::from("key1"),
                value: Bytes::from("world")
            })
        );

        store
            .lock()
            .unwrap()
            .set(String::from("key1"), Bytes::from("hello"));

        let res = cmd.exec(store.clone()).unwrap();

        assert_eq!(res, Frame::Integer(10));
        assert_eq!(
            store.lock().unwrap().get("key1"),
            Some(&Bytes::from("helloworld"))
        );
    }
}
