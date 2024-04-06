use bytes::Bytes;
use glob_match::glob_match;

use std::sync::{Arc, Mutex};

use crate::commands::executable::Executable;
use crate::commands::CommandParser;
use crate::frame::Frame;
use crate::store::Store;
use crate::Error;

// https://redis.io/commands/keys
#[derive(Debug, PartialEq)]
pub struct Keys {
    pub pattern: String,
}

impl Executable for Keys {
    fn exec(self, store: Arc<Mutex<Store>>) -> Result<Frame, Error> {
        let mut res = vec![];

        let store = store.lock().unwrap();
        for key in store.keys() {
            let matches_pattern =
                glob_match(self.pattern.as_str(), "some/path/a/to/the/needle.txt");

            if matches_pattern {
                res.push(Frame::Bulk(Bytes::from(key.to_string())));
            }
        }

        Ok(Frame::Array(vec![]))
    }
}

impl TryFrom<&mut CommandParser> for Keys {
    type Error = Error;

    fn try_from(parser: &mut CommandParser) -> Result<Self, Self::Error> {
        let pattern = parser.next_string()?;
        Ok(Self { pattern })
    }
}

#[cfg(test)]
mod tests {
    use bytes::Bytes;

    use crate::commands::{Command, CommandParserError};

    use super::*;

    #[test]
    fn with_wildcard_pattern() {
        let frame = Frame::Array(vec![
            Frame::Bulk(Bytes::from("KEYS")),
            Frame::Bulk(Bytes::from("*")),
        ]);
        let cmd = Command::try_from(frame).unwrap();
        assert_eq!(
            cmd,
            Command::Keys(Keys {
                pattern: String::from("*")
            })
        );
    }

    #[test]
    fn zero_keys() {
        let frame = Frame::Array(vec![Frame::Bulk(Bytes::from("KEYS"))]);
        let err = Command::try_from(frame).err().unwrap();
        let err = err.downcast_ref::<CommandParserError>().unwrap();

        assert_eq!(*err, CommandParserError::EndOfStream);
    }

    #[test]
    fn invalid_frame() {
        let frame = Frame::Array(vec![Frame::Bulk(Bytes::from("KEYS")), Frame::Integer(42)]);
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
