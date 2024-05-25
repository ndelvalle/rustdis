use bytes::Bytes;

use crate::commands::executable::Executable;
use crate::commands::{CommandParser, CommandParserError};
use crate::frame::Frame;
use crate::store::Store;
use crate::Error;

/// Sets the given keys to their respective values. Replaces existing values with new values.
///
/// Ref: <https://redis.io/docs/latest/commands/mset/>
#[derive(Debug, PartialEq)]
pub struct Mset {
    pub pairs: Vec<(String, Bytes)>,
}

impl Executable for Mset {
    fn exec(self, store: Store) -> Result<Frame, Error> {
        if self.pairs.is_empty() {
            return Ok(Frame::Error(
                "ERR wrong number of arguments for command".to_string(),
            ));
        }

        let mut store = store.lock();

        for (key, value) in self.pairs.iter() {
            store.set(key.to_string(), value.clone());
        }

        Ok(Frame::Simple("OK".to_string()))
    }
}

impl TryFrom<&mut CommandParser> for Mset {
    type Error = Error;

    fn try_from(parser: &mut CommandParser) -> Result<Self, Self::Error> {
        let mut pairs = vec![];

        loop {
            match (parser.next_string(), parser.next_bytes()) {
                (Ok(key), Ok(value)) => pairs.push((key, value)),
                // TODO: move back the `keys.is_empty()` check here.
                // We handle the case where no keys are provided in the `exec` method,
                // because at the moment we don't have a way to return an error from here.
                (Err(CommandParserError::EndOfStream), _) => {
                    break;
                }
                (_, Err(CommandParserError::EndOfStream)) => {
                    break;
                }
                (Err(err), _) => return Err(err.into()),
                (_, Err(err)) => return Err(err.into()),
            }
        }

        Ok(Self { pairs })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::commands::Command;
    use bytes::Bytes;

    #[tokio::test]
    async fn insert_one() {
        let store = Store::new();

        let frame = Frame::Array(vec![
            Frame::Bulk(Bytes::from("MSET")),
            Frame::Bulk(Bytes::from("key1")),
            Frame::Bulk(Bytes::from("value1")),
        ]);
        let cmd = Command::try_from(frame).unwrap();

        assert_eq!(
            cmd,
            Command::Mset(Mset {
                pairs: vec![(String::from("key1"), Bytes::from("value1"))]
            })
        );

        let res = cmd.exec(store.clone()).unwrap();

        assert_eq!(res, Frame::Simple("OK".to_string()));

        assert_eq!(store.lock().get("key1").unwrap(), Bytes::from("value1"));
    }

    #[tokio::test]
    async fn insert_many() {
        let store = Store::new();

        let frame = Frame::Array(vec![
            Frame::Bulk(Bytes::from("MSET")),
            Frame::Bulk(Bytes::from("key1")),
            Frame::Bulk(Bytes::from("value1")),
            Frame::Bulk(Bytes::from("key2")),
            Frame::Bulk(Bytes::from("value2")),
            Frame::Bulk(Bytes::from("key3")),
            Frame::Bulk(Bytes::from("value3")),
        ]);
        let cmd = Command::try_from(frame).unwrap();

        assert_eq!(
            cmd,
            Command::Mset(Mset {
                pairs: vec![
                    (String::from("key1"), Bytes::from("value1")),
                    (String::from("key2"), Bytes::from("value2")),
                    (String::from("key3"), Bytes::from("value3"))
                ]
            })
        );

        let res = cmd.exec(store.clone()).unwrap();

        assert_eq!(res, Frame::Simple("OK".to_string()));
        assert_eq!(store.lock().get("key1"), Some(Bytes::from("value1")),);
        assert_eq!(store.lock().get("key2"), Some(Bytes::from("value2")),);
        assert_eq!(store.lock().get("key3"), Some(Bytes::from("value3")),);
    }

    #[tokio::test]
    async fn overide_existing() {
        let store = Store::new();

        let frame = Frame::Array(vec![
            Frame::Bulk(Bytes::from("MSET")),
            Frame::Bulk(Bytes::from("key1")),
            Frame::Bulk(Bytes::from("value1")),
        ]);
        let cmd = Command::try_from(frame).unwrap();

        assert_eq!(
            cmd,
            Command::Mset(Mset {
                pairs: vec![(String::from("key1"), Bytes::from("value1")),]
            })
        );

        store.lock().set(String::from("key1"), Bytes::from("1"));

        let res = cmd.exec(store.clone()).unwrap();

        assert_eq!(res, Frame::Simple("OK".to_string()));
        assert_eq!(store.lock().get("key1").unwrap(), Bytes::from("value1"));
    }

    #[tokio::test]
    async fn no_keys() {
        let store = Store::new();

        let frame = Frame::Array(vec![Frame::Bulk(Bytes::from("MSET"))]);
        let cmd = Command::try_from(frame).unwrap();

        assert_eq!(cmd, Command::Mset(Mset { pairs: vec![] }));

        let res = cmd.exec(store.clone()).unwrap();

        assert_eq!(
            res,
            Frame::Error("ERR wrong number of arguments for command".to_string())
        );
    }
}
