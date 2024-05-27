use bytes::Bytes;
use glob_match::glob_match;

use crate::commands::executable::Executable;
use crate::commands::CommandParser;
use crate::frame::Frame;
use crate::store::Store;
use crate::Error;

/// Return all keys matching `pattern`.
///
/// Uses [glob-match](https://github.com/devongovett/glob-match) to match the `pattern`.
///
/// Ref: <https://redis.io/commands/keys>
#[derive(Debug, PartialEq)]
pub struct Keys {
    pub pattern: String,
}

impl Executable for Keys {
    fn exec(self, store: Store) -> Result<Frame, Error> {
        let store = store.lock();
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
    use bytes::Bytes;

    use super::*;
    use crate::commands::{Command, CommandParserError};

    #[tokio::test]
    async fn with_wildcard_pattern() {
        let store = Store::new();

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

        {
            let mut store = store.lock();
            store.set(String::from("key1"), Bytes::from("1"));
            store.set(String::from("key2"), Bytes::from("2"));
            store.set(String::from("key3"), Bytes::from("3"));
        }

        let result = cmd.exec(store.clone()).unwrap();
        let result = match result {
            Frame::Array(mut vec) => {
                vec.sort();
                Frame::Array(vec)
            }
            f => f,
        };

        assert_eq!(
            result,
            Frame::Array(vec![
                Frame::Bulk(Bytes::from("key1")),
                Frame::Bulk(Bytes::from("key2")),
                Frame::Bulk(Bytes::from("key3")),
            ])
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
