use std::sync::{Arc, Mutex};

use crate::commands::executable::Executable;
use crate::commands::CommandParser;
use crate::frame::Frame;
use crate::store::Store;
use crate::Error;

/// Get the value of key and delete the key. This command is similar to GET, except for the fact
/// that it also deletes the key on success (if and only if the key's value type is a string).
///
/// Ref: <https://redis.io/docs/latest/commands/getdel/>
#[derive(Debug, PartialEq)]
pub struct Getdel {
    pub key: String,
}

impl Executable for Getdel {
    fn exec(self, store: Arc<Mutex<Store>>) -> Result<Frame, Error> {
        let mut store = store.lock().unwrap();
        let removed_key = store.remove(&self.key);
        let res = match removed_key {
            Some(val) => Frame::Bulk(val),
            None => Frame::Null,
        };

        Ok(res)
    }
}

impl TryFrom<&mut CommandParser> for Getdel {
    type Error = Error;

    fn try_from(parser: &mut CommandParser) -> Result<Self, Self::Error> {
        let key = parser.next_string()?;
        Ok(Self { key })
    }
}

#[cfg(test)]
mod tests {
    use bytes::Bytes;

    use crate::commands::Command;

    use super::*;

    #[tokio::test]
    async fn when_key_exists() {
        let store = Arc::new(Mutex::new(Store::default()));

        let frame = Frame::Array(vec![
            Frame::Bulk(Bytes::from("GETDEL")),
            Frame::Bulk(Bytes::from("foo")),
        ]);
        let cmd = Command::try_from(frame).unwrap();
        assert_eq!(
            cmd,
            Command::Getdel(Getdel {
                key: "foo".to_string()
            })
        );

        store
            .lock()
            .unwrap()
            .set("foo".to_string(), Bytes::from("baz"));

        let res = cmd.exec(store.clone()).unwrap();
        assert_eq!(res, Frame::Bulk(Bytes::from("baz")));
        assert_eq!(store.lock().unwrap().get("foo"), None);
    }

    #[tokio::test]
    async fn when_key_does_not_exists() {
        let store = Arc::new(Mutex::new(Store::default()));

        let frame = Frame::Array(vec![
            Frame::Bulk(Bytes::from("GETDEL")),
            Frame::Bulk(Bytes::from("foo")),
        ]);
        let cmd = Command::try_from(frame).unwrap();
        assert_eq!(
            cmd,
            Command::Getdel(Getdel {
                key: "foo".to_string()
            })
        );

        let res = cmd.exec(store.clone()).unwrap();
        assert_eq!(res, Frame::Null);
    }
}
