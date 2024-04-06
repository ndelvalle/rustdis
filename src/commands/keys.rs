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
        let store = store.lock().unwrap();
        let matching_keys: Vec<Frame> = store
            .keys()
            .filter(|key| glob_match(self.pattern.as_str(), key))
            .map(|key| Frame::Bulk(Bytes::from(key.to_string())))
            .collect();

        Ok(Frame::Array(matching_keys))
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
    use super::*;
    use crate::commands::{Command, CommandParserError};
    use bytes::Bytes;

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

        let store = Arc::new(Mutex::new(Store::new()));
        {
            let mut store = store.lock().unwrap();
            store.set(String::from("key1"), Bytes::from("1"));
            store.set(String::from("key2"), Bytes::from("2"));
            store.set(String::from("key3"), Bytes::from("3"));
        }

        // TODO: Properly test the result matches the expected keys.
        let result = cmd.exec(store.clone()).unwrap();
        let result = match result {
            Frame::Array(vec) => vec,
            _ => panic!("Expected Frame::Array but found another variant."),
        };

        assert_eq!(result.len(), 3);
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
