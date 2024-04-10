use std::sync::{Arc, Mutex};

use crate::commands::executable::Executable;
use crate::commands::CommandParser;
use crate::frame::Frame;
use crate::store::Store;
use crate::Error;

/// Returns the string representation of the type of the value stored at `key`.
///
/// The different types that can be returned are: `string`, `list`, `set`, `zset`, `hash` and `stream`.
/// If the key does not exist, `none` is returned.
///
/// **NOTE**: This server implementation only supports `string` type.
///
/// Ref: <https://redis.io/docs/latest/commands/type/>
#[derive(Debug, PartialEq)]
pub struct Type {
    pub key: String,
}

impl Executable for Type {
    fn exec(self, store: Arc<Mutex<Store>>) -> Result<Frame, Error> {
        let store = store.lock().unwrap();
        let type_ = store
            .get(&self.key)
            .map(|_| "string".to_string())
            .unwrap_or_else(|| "none".to_string());

        Ok(Frame::Simple(type_))
    }
}

impl TryFrom<&mut CommandParser> for Type {
    type Error = Error;

    fn try_from(parser: &mut CommandParser) -> Result<Self, Self::Error> {
        let key = parser.next_string()?;
        Ok(Self { key })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::commands::Command;
    use bytes::Bytes;

    #[test]
    fn existing_key() {
        let frame = Frame::Array(vec![
            Frame::Bulk(Bytes::from("TYPE")),
            Frame::Bulk(Bytes::from("key1")),
        ]);
        let cmd = Command::try_from(frame).unwrap();

        assert_eq!(
            cmd,
            Command::Type(Type {
                key: String::from("key1"),
            })
        );

        let store = Arc::new(Mutex::new(Store::new()));
        {
            let mut store = store.lock().unwrap();
            store.set(String::from("key1"), Bytes::from("1"));
        }

        let result = cmd.exec(store.clone()).unwrap();

        assert_eq!(result, Frame::Simple("string".to_string()));
    }

    #[test]
    fn missing_key() {
        let frame = Frame::Array(vec![
            Frame::Bulk(Bytes::from("TYPE")),
            Frame::Bulk(Bytes::from("key1")),
        ]);
        let cmd = Command::try_from(frame).unwrap();

        assert_eq!(
            cmd,
            Command::Type(Type {
                key: String::from("key1"),
            })
        );

        let store = Arc::new(Mutex::new(Store::new()));

        let result = cmd.exec(store.clone()).unwrap();

        assert_eq!(result, Frame::Simple("none".to_string()));
    }
}
