use crate::commands::executable::Executable;
use crate::commands::CommandParser;
use crate::frame::Frame;
use crate::store::Store;
use crate::Error;

/// Increments the number stored at key by `increment`.
///
/// Ref: <https://redis.io/docs/latest/commands/incrby/>
#[derive(Debug, PartialEq)]
pub struct IncrBy {
    pub key: String,
    pub increment: i64,
}

impl Executable for IncrBy {
    fn exec(self, store: Store) -> Result<Frame, Error> {
        let res = store.incr_by(&self.key, self.increment);
        match res {
            Ok(value) => Ok(Frame::Integer(value)),
            Err(msg) => Ok(Frame::Error(msg.to_string())),
        }
    }
}

impl TryFrom<&mut CommandParser> for IncrBy {
    type Error = Error;

    fn try_from(parser: &mut CommandParser) -> Result<Self, Self::Error> {
        let key = parser.next_string()?;
        let increment = parser.next_integer()?;

        Ok(Self { key, increment })
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
            Frame::Bulk(Bytes::from("INCRBY")),
            Frame::Bulk(Bytes::from("key1")),
            Frame::Integer(10),
        ]);
        let cmd = Command::try_from(frame).unwrap();

        assert_eq!(
            cmd,
            Command::IncrBy(IncrBy {
                key: "key1".to_string(),
                increment: 10,
            })
        );

        store.lock().set(String::from("key1"), Bytes::from("20"));

        let result = cmd.exec(store.clone()).unwrap();

        assert_eq!(result, Frame::Integer(30));
        assert_eq!(store.lock().get("key1"), Some(Bytes::from("30")));
    }

    #[tokio::test]
    async fn non_existing_key() {
        let store = Store::new();

        let frame = Frame::Array(vec![
            Frame::Bulk(Bytes::from("INCRBY")),
            Frame::Bulk(Bytes::from("key1")),
            Frame::Integer(10),
        ]);
        let cmd = Command::try_from(frame).unwrap();

        assert_eq!(
            cmd,
            Command::IncrBy(IncrBy {
                key: "key1".to_string(),
                increment: 10,
            })
        );

        let result = cmd.exec(store.clone()).unwrap();

        assert_eq!(result, Frame::Integer(10));
        assert_eq!(store.lock().get("key1"), Some(Bytes::from("10")));
    }

    #[tokio::test]
    async fn invalid_key_type() {
        let store = Store::new();

        let frame = Frame::Array(vec![
            Frame::Bulk(Bytes::from("INCRBY")),
            Frame::Bulk(Bytes::from("key1")),
            Frame::Integer(10),
        ]);
        let cmd = Command::try_from(frame).unwrap();

        assert_eq!(
            cmd,
            Command::IncrBy(IncrBy {
                key: "key1".to_string(),
                increment: 10,
            })
        );

        store.lock().set(String::from("key1"), Bytes::from("value"));

        let result = cmd.exec(store.clone()).unwrap();

        assert_eq!(
            result,
            Frame::Error("value is not an integer or out of range".to_string())
        );
        assert_eq!(store.lock().get("key1"), Some(Bytes::from("value")));
    }

    #[tokio::test]
    async fn out_of_range() {
        let store = Store::new();

        let frame = Frame::Array(vec![
            Frame::Bulk(Bytes::from("INCRBY")),
            Frame::Bulk(Bytes::from("key1")),
            Frame::Integer(10),
        ]);
        let cmd = Command::try_from(frame).unwrap();

        assert_eq!(
            cmd,
            Command::IncrBy(IncrBy {
                key: "key1".to_string(),
                increment: 10,
            })
        );

        store
            .lock()
            .set(String::from("key1"), Bytes::from("999223372036854775808"));

        let result = cmd.exec(store.clone()).unwrap();

        assert_eq!(
            result,
            Frame::Error("value is not an integer or out of range".to_string())
        );

        assert_eq!(
            store.lock().get("key1"),
            Some(Bytes::from("999223372036854775808"))
        );
    }
}
