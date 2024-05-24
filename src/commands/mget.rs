use std::sync::{Arc, Mutex};

use crate::commands::executable::Executable;
use crate::commands::{CommandParser, CommandParserError};
use crate::frame::Frame;
use crate::store::Store;
use crate::Error;

/// Returns the values of all specified keys.
///
/// Ref: <https://redis.io/docs/latest/commands/mget/>
#[derive(Debug, PartialEq)]
pub struct Mget {
    pub keys: Vec<String>,
}

impl Executable for Mget {
    fn exec(self, store: Arc<Mutex<Store>>) -> Result<Frame, Error> {
        if self.keys.is_empty() {
            return Ok(Frame::Error(
                "ERR wrong number of arguments for command".to_string(),
            ));
        }

        let store = store.lock().unwrap();
        let values = self
            .keys
            .iter()
            .map(|key| store.get(key))
            .map(|value| {
                value
                    .map(|v| Frame::Bulk(v.clone()))
                    .unwrap_or_else(|| Frame::Null)
            })
            .collect::<Vec<_>>();

        Ok(Frame::Array(values))
    }
}

impl TryFrom<&mut CommandParser> for Mget {
    type Error = Error;

    fn try_from(parser: &mut CommandParser) -> Result<Self, Self::Error> {
        let mut keys = vec![];

        loop {
            match parser.next_string() {
                Ok(key) => keys.push(key),
                // TODO: move back the `keys.is_empty()` check here.
                // We handle the case where no keys are provided in the `exec` method,
                // because at the moment we don't have a way to return an error from here.
                Err(CommandParserError::EndOfStream) => {
                    break;
                }
                Err(err) => return Err(err.into()),
            }
        }

        Ok(Self { keys })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::commands::Command;
    use bytes::Bytes;

    #[tokio::test]
    async fn existing_key() {
        let store = Arc::new(Mutex::new(Store::new()));

        let frame = Frame::Array(vec![
            Frame::Bulk(Bytes::from("MGET")),
            Frame::Bulk(Bytes::from("key1")),
        ]);
        let cmd = Command::try_from(frame).unwrap();

        assert_eq!(
            cmd,
            Command::Mget(Mget {
                keys: vec![String::from("key1")]
            })
        );

        store
            .lock()
            .unwrap()
            .set(String::from("key1"), Bytes::from("1"));

        let res = cmd.exec(store.clone()).unwrap();

        assert_eq!(res, Frame::Array(vec![Frame::Bulk(Bytes::from("1"))]));
    }

    #[tokio::test]
    async fn existing_keys() {
        let store = Arc::new(Mutex::new(Store::new()));

        let frame = Frame::Array(vec![
            Frame::Bulk(Bytes::from("MGET")),
            Frame::Bulk(Bytes::from("key1")),
            Frame::Bulk(Bytes::from("key2")),
            Frame::Bulk(Bytes::from("key3")),
        ]);
        let cmd = Command::try_from(frame).unwrap();

        assert_eq!(
            cmd,
            Command::Mget(Mget {
                keys: vec![
                    String::from("key1"),
                    String::from("key2"),
                    String::from("key3")
                ]
            })
        );

        {
            let mut store = store.lock().unwrap();
            store.set(String::from("key1"), Bytes::from("1"));
            store.set(String::from("key2"), Bytes::from("2"));
            store.set(String::from("key3"), Bytes::from("3"));
        }

        let res = cmd.exec(store.clone()).unwrap();

        assert_eq!(
            res,
            Frame::Array(vec![
                Frame::Bulk(Bytes::from("1")),
                Frame::Bulk(Bytes::from("2")),
                Frame::Bulk(Bytes::from("3"))
            ])
        );
    }

    #[tokio::test]
    async fn non_existing_key() {
        let store = Arc::new(Mutex::new(Store::new()));

        let frame = Frame::Array(vec![
            Frame::Bulk(Bytes::from("MGET")),
            Frame::Bulk(Bytes::from("key1")),
        ]);
        let cmd = Command::try_from(frame).unwrap();

        assert_eq!(
            cmd,
            Command::Mget(Mget {
                keys: vec![String::from("key1")]
            })
        );

        let res = cmd.exec(store.clone()).unwrap();

        assert_eq!(res, Frame::Array(vec![Frame::Null]));
    }

    #[tokio::test]
    async fn mixed_keys() {
        let store = Arc::new(Mutex::new(Store::new()));

        let frame = Frame::Array(vec![
            Frame::Bulk(Bytes::from("MGET")),
            Frame::Bulk(Bytes::from("key1")),
            Frame::Bulk(Bytes::from("key2")),
            Frame::Bulk(Bytes::from("key3")),
        ]);
        let cmd = Command::try_from(frame).unwrap();

        assert_eq!(
            cmd,
            Command::Mget(Mget {
                keys: vec![
                    String::from("key1"),
                    String::from("key2"),
                    String::from("key3")
                ]
            })
        );

        {
            let mut store = store.lock().unwrap();
            store.set(String::from("key1"), Bytes::from("1"));
            store.set(String::from("key3"), Bytes::from("3"));
        }

        let res = cmd.exec(store.clone()).unwrap();

        assert_eq!(
            res,
            Frame::Array(vec![
                Frame::Bulk(Bytes::from("1")),
                Frame::Null,
                Frame::Bulk(Bytes::from("3"))
            ])
        );
    }

    #[tokio::test]
    async fn no_keys() {
        let store = Arc::new(Mutex::new(Store::new()));

        let frame = Frame::Array(vec![Frame::Bulk(Bytes::from("MGET"))]);
        let cmd = Command::try_from(frame).unwrap();

        assert_eq!(cmd, Command::Mget(Mget { keys: vec![] }));

        let res = cmd.exec(store.clone()).unwrap();

        assert_eq!(
            res,
            Frame::Error("ERR wrong number of arguments for command".to_string())
        );
    }
}
