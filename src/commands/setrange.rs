use bytes::Bytes;
use std::sync::{Arc, Mutex};

use crate::commands::executable::Executable;
use crate::commands::{CommandParser, CommandParserError};
use crate::frame::Frame;
use crate::store::Store;
use crate::Error;

const MAX_OFFSET: usize = 536_870_911;

/// Setrange overwrites part of the string stored at key, starting at the specified offset, for the
/// entire length of value. If the offset is larger than the current length of the string at key,
/// the string is padded with zero-bytes to make offset fit. Non-existing keys are considered as
/// empty strings, so this command will make sure it holds a string large enough to be able to set
/// value at offset.
///
/// Note that the maximum offset that you can set is 2^29 -1 (536870911), as Redis Strings are
/// limited to 512 megabytes. If you need to grow beyond this size, you can use multiple keys.
///
/// Ref: <https://redis.io/docs/latest/commands/setrange/>
#[derive(Debug, PartialEq)]
pub struct Setrange {
    pub key: String,
    pub offset: i64,
    pub value: Bytes,
}

impl Executable for Setrange {
    fn exec(self, store: Arc<Mutex<Store>>) -> Result<Frame, Error> {
        let mut store = store.lock().unwrap();
        let current_value = store.get(&self.key).map(|b| b.as_ref()).unwrap_or_default();

        let offset = self.offset as usize;
        let new_len = offset + self.value.len();
        let mut new_value = vec![b' '; usize::max(new_len, current_value.len())];

        new_value[..current_value.len()].copy_from_slice(current_value);
        new_value[offset..new_len].copy_from_slice(&self.value);

        store.set(self.key.clone(), Bytes::from(new_value));

        Ok(Frame::Integer(new_len as i64))
    }
}

impl TryFrom<&mut CommandParser> for Setrange {
    type Error = Error;

    fn try_from(parser: &mut CommandParser) -> Result<Self, Self::Error> {
        let key = parser.next_string()?;
        let offset = parser.next_integer()?;
        let value = parser.next_bytes()?;

        if offset as usize >= MAX_OFFSET {
            return Err(CommandParserError::InvalidCommandArgument {
                command: String::from("SETRANGE"),
                argument: String::from("offset"),
            }
            .into());
        }

        Ok(Self { key, offset, value })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::commands::Command;
    use bytes::Bytes;

    #[tokio::test]
    async fn when_key_does_not_exists_with_no_offset() {
        let store = Arc::new(Mutex::new(Store::new()));

        let frame = Frame::Array(vec![
            Frame::Bulk(Bytes::from("SETRANGE")),
            Frame::Bulk(Bytes::from("key1")),
            Frame::Bulk(Bytes::from("0")),
            Frame::Bulk(Bytes::from("Hello World")),
        ]);
        let cmd = Command::try_from(frame).unwrap();

        assert_eq!(
            cmd,
            Command::Setrange(Setrange {
                key: String::from("key1"),
                offset: 0,
                value: Bytes::from("Hello World"),
            })
        );

        let res = cmd.exec(store.clone()).unwrap();

        assert_eq!(res, Frame::Integer(11));
        assert_eq!(
            store.lock().unwrap().get("key1"),
            Some(&Bytes::from("Hello World"))
        );
    }

    #[tokio::test]
    async fn when_key_does_not_exists_with_offset() {
        let store = Arc::new(Mutex::new(Store::new()));

        let frame = Frame::Array(vec![
            Frame::Bulk(Bytes::from("SETRANGE")),
            Frame::Bulk(Bytes::from("key1")),
            Frame::Bulk(Bytes::from("6")),
            Frame::Bulk(Bytes::from("Redis")),
        ]);
        let cmd = Command::try_from(frame).unwrap();

        assert_eq!(
            cmd,
            Command::Setrange(Setrange {
                key: String::from("key1"),
                offset: 6,
                value: Bytes::from("Redis"),
            })
        );

        let res = cmd.exec(store.clone()).unwrap();

        assert_eq!(res, Frame::Integer(11));
        assert_eq!(
            store.lock().unwrap().get("key1"),
            Some(&Bytes::from("      Redis"))
        );
    }

    #[tokio::test]
    async fn when_key_exists_with_offset() {
        let store = Arc::new(Mutex::new(Store::new()));

        let frame = Frame::Array(vec![
            Frame::Bulk(Bytes::from("SETRANGE")),
            Frame::Bulk(Bytes::from("key1")),
            Frame::Bulk(Bytes::from("6")),
            Frame::Bulk(Bytes::from("Redis")),
        ]);
        let cmd = Command::try_from(frame).unwrap();

        assert_eq!(
            cmd,
            Command::Setrange(Setrange {
                key: String::from("key1"),
                offset: 6,
                value: Bytes::from("Redis"),
            })
        );

        store
            .lock()
            .unwrap()
            .set(String::from("key1"), Bytes::from("Hello World!!!"));

        let res = cmd.exec(store.clone()).unwrap();

        assert_eq!(res, Frame::Integer(11));
        assert_eq!(
            store.lock().unwrap().get("key1"),
            Some(&Bytes::from("Hello Redis!!!"))
        );
    }

    #[tokio::test]
    async fn when_offset_is_to_big() {
        let frame = Frame::Array(vec![
            Frame::Bulk(Bytes::from("SETRANGE")),
            Frame::Bulk(Bytes::from("key1")),
            Frame::Bulk(Bytes::from(format!("{}", MAX_OFFSET))),
            Frame::Bulk(Bytes::from("value1")),
        ]);

        let err = Command::try_from(frame).err().unwrap();
        let err = err.downcast_ref::<CommandParserError>().unwrap();

        assert_eq!(
            *err,
            CommandParserError::InvalidCommandArgument {
                command: String::from("SETRANGE"),
                argument: "offset".to_string(),
            }
        );
    }
}
