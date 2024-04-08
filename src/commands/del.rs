use std::sync::{Arc, Mutex};

use crate::commands::executable::Executable;
use crate::commands::{CommandParser, CommandParserError};
use crate::frame::Frame;
use crate::store::Store;
use crate::Error;

// https://redis.io/commands/del
#[derive(Debug, PartialEq)]
pub struct Del {
    pub keys: Vec<String>,
}

impl Executable for Del {
    fn exec(self, store: Arc<Mutex<Store>>) -> Result<Frame, Error> {
        let mut count = 0;
        let mut store = store.lock().unwrap();
        for key in self.keys {
            if store.remove(&key).is_some() {
                count += 1;
            }
        }
        Ok(Frame::Integer(count))
    }
}

impl TryFrom<&mut CommandParser> for Del {
    type Error = Error;

    fn try_from(parser: &mut CommandParser) -> Result<Self, Self::Error> {
        let mut keys = vec![];

        loop {
            match parser.next_string() {
                Ok(key) => keys.push(key),
                Err(CommandParserError::EndOfStream) if !keys.is_empty() => {
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
    use bytes::Bytes;

    use crate::commands::Command;

    use super::*;

    #[test]
    fn multiple_keys() {
        let frame = Frame::Array(vec![
            Frame::Bulk(Bytes::from("DEL")),
            Frame::Bulk(Bytes::from("foo")),
            Frame::Bulk(Bytes::from("bar")),
            Frame::Bulk(Bytes::from("baz")),
        ]);
        let cmd = Command::try_from(frame).unwrap();
        assert_eq!(
            cmd,
            Command::Del(Del {
                keys: vec!["foo".to_string(), "bar".to_string(), "baz".to_string()]
            })
        );
    }

    #[test]
    fn single_key() {
        let frame = Frame::Array(vec![
            Frame::Bulk(Bytes::from("DEL")),
            Frame::Bulk(Bytes::from("foo")),
        ]);
        let cmd = Command::try_from(frame).unwrap();
        assert_eq!(
            cmd,
            Command::Del(Del {
                keys: vec!["foo".to_string()]
            })
        );
    }

    #[test]
    fn zero_keys() {
        let frame = Frame::Array(vec![Frame::Bulk(Bytes::from("DEL"))]);
        let err = Command::try_from(frame).err().unwrap();
        let err = err.downcast_ref::<CommandParserError>().unwrap();

        assert_eq!(*err, CommandParserError::EndOfStream);
    }

    #[test]
    fn invalid_frame() {
        let frame = Frame::Array(vec![
            Frame::Bulk(Bytes::from("DEL")),
            Frame::Integer(42),
            Frame::Bulk(Bytes::from("foo")),
        ]);
        let err = Command::try_from(frame).err().unwrap();
        let err = err.downcast_ref::<CommandParserError>().unwrap();

        assert_eq!(
            *err,
            CommandParserError::InvalidFrame {
                expected: "simple or bulk string".to_string(),
                actual: Frame::Integer(42)
            }
        );
    }
}
