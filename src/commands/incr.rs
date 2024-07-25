use crate::commands::executable::Executable;
use crate::commands::CommandParser;
use crate::frame::Frame;
use crate::store::Store;
use crate::Error;

/// Increments the number stored at key by one.
///
/// Ref: <https://redis.io/docs/latest/commands/incr/>
#[derive(Debug, PartialEq)]
pub struct Incr {
    pub key: String,
}

impl Executable for Incr {
    fn exec(self, store: Store) -> Result<Frame, Error> {
        let res = store.incr_by(&self.key, 1);
        match res {
            Ok(val) => Ok(Frame::Integer(val)),
            Err(msg) => Ok(Frame::Error(msg.to_string())),
        }
    }
}

impl TryFrom<&mut CommandParser> for Incr {
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
            Frame::Bulk(Bytes::from("INCR")),
            Frame::Bulk(Bytes::from("key1")),
        ]);
        let cmd = Command::try_from(frame).unwrap();

        assert_eq!(
            cmd,
            Command::Incr(Incr {
                key: "key1".to_string()
            })
        );

        store.lock().set(String::from("key1"), Bytes::from("1"));

        let result = cmd.exec(store.clone()).unwrap();

        assert_eq!(result, Frame::Integer(2));
        assert_eq!(store.lock().get("key1"), Some(Bytes::from("2")));
    }

    #[tokio::test]
    async fn non_existing_key() {
        let store = Store::new();

        let frame = Frame::Array(vec![
            Frame::Bulk(Bytes::from("INCR")),
            Frame::Bulk(Bytes::from("key1")),
        ]);
        let cmd = Command::try_from(frame).unwrap();

        assert_eq!(
            cmd,
            Command::Incr(Incr {
                key: "key1".to_string()
            })
        );

        let result = cmd.exec(store.clone()).unwrap();

        assert_eq!(result, Frame::Integer(1));
        assert_eq!(store.lock().get("key1"), Some(Bytes::from("1")));
    }

    #[tokio::test]
    async fn invalid_key_type() {
        let store = Store::new();

        let frame = Frame::Array(vec![
            Frame::Bulk(Bytes::from("INCR")),
            Frame::Bulk(Bytes::from("key1")),
        ]);
        let cmd = Command::try_from(frame).unwrap();

        assert_eq!(
            cmd,
            Command::Incr(Incr {
                key: "key1".to_string()
            })
        );

        store.lock().set(String::from("key1"), Bytes::from("value"));

        let result = cmd.exec(store.clone()).unwrap();

        assert_eq!(
            result,
            Frame::Error("value is not of the correct type or out of range".to_string())
        );
        assert_eq!(store.lock().get("key1"), Some(Bytes::from("value")));
    }

    #[tokio::test]
    async fn out_of_range() {
        let store = Store::new();

        let frame = Frame::Array(vec![
            Frame::Bulk(Bytes::from("INCR")),
            Frame::Bulk(Bytes::from("key1")),
        ]);
        let cmd = Command::try_from(frame).unwrap();

        assert_eq!(
            cmd,
            Command::Incr(Incr {
                key: "key1".to_string()
            })
        );

        store
            .lock()
            .set(String::from("key1"), Bytes::from("999223372036854775808"));

        let result = cmd.exec(store.clone()).unwrap();

        assert_eq!(
            result,
            Frame::Error("value is not of the correct type or out of range".to_string())
        );
        assert_eq!(
            store.lock().get("key1"),
            Some(Bytes::from("999223372036854775808"))
        );
    }
}
