use crate::commands::executable::Executable;
use crate::commands::CommandParser;
use crate::frame::Frame;
use crate::store::Store;
use crate::Error;

/// Returns the length of the string value stored at key. An error is returned when key holds a
/// non-string value.
///
/// Ref: <https://redis.io/docs/latest/commands/strlen/>
#[derive(Debug, PartialEq)]
pub struct Strlen {
    pub key: String,
}

impl Executable for Strlen {
    fn exec(self, store: Store) -> Result<Frame, Error> {
        let store = store.lock();
        let value = store.get(&self.key);

        match value {
            Some(value) => Ok(Frame::Integer(value.len() as i64)),
            None => Ok(Frame::Integer(0)),
        }
    }
}

impl TryFrom<&mut CommandParser> for Strlen {
    type Error = Error;

    fn try_from(parser: &mut CommandParser) -> Result<Self, Self::Error> {
        let key = parser.next_string()?;
        Ok(Self { key })
    }
}

#[cfg(test)]
mod tests {
    use bytes::Bytes;

    use super::*;
    use crate::commands::Command;

    #[tokio::test]
    async fn existing_key() {
        let store = Store::new();

        let frame = Frame::Array(vec![
            Frame::Bulk(Bytes::from("STRLEN")),
            Frame::Bulk(Bytes::from("key1")),
        ]);
        let cmd = Command::try_from(frame).unwrap();

        assert_eq!(
            cmd,
            Command::Strlen(Strlen {
                key: String::from("key1")
            })
        );

        store
            .lock()
            .set(String::from("key1"), Bytes::from("Hello world"));

        let result = cmd.exec(store.clone()).unwrap();

        assert_eq!(result, Frame::Integer(11));
    }

    #[tokio::test]
    async fn non_existing_key() {
        let store = Store::new();

        let frame = Frame::Array(vec![
            Frame::Bulk(Bytes::from("STRLEN")),
            Frame::Bulk(Bytes::from("key1")),
        ]);
        let cmd = Command::try_from(frame).unwrap();

        assert_eq!(
            cmd,
            Command::Strlen(Strlen {
                key: String::from("key1")
            })
        );

        let res = cmd.exec(store.clone()).unwrap();

        assert_eq!(res, Frame::Integer(0));
    }
}
