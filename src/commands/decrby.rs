use std::sync::{Arc, Mutex};

use crate::commands::executable::Executable;
use crate::commands::CommandParser;
use crate::frame::Frame;
use crate::store::Store;
use crate::Error;

/// Decrements the number stored at key by `decrement`.
///
/// Ref: <https://redis.io/docs/latest/commands/incrby/>
#[derive(Debug, PartialEq)]
pub struct DecrBy {
    pub key: String,
    pub decrement: i64,
}

impl Executable for DecrBy {
    fn exec(self, store: Arc<Mutex<Store>>) -> Result<Frame, Error> {
        let res = store.lock().unwrap().incr_by(&self.key, -self.decrement);

        match res {
            Ok(_) => Ok(Frame::Simple("OK".to_string())),
            Err(msg) => Ok(Frame::Error(msg.to_string())),
        }
    }
}

impl TryFrom<&mut CommandParser> for DecrBy {
    type Error = Error;

    fn try_from(parser: &mut CommandParser) -> Result<Self, Self::Error> {
        let key = parser.next_string()?;
        let decrement = parser.next_integer()?;

        Ok(Self { key, decrement })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::commands::Command;
    use bytes::Bytes;

    #[tokio::test]
    async fn existing_key() {
        let frame = Frame::Array(vec![
            Frame::Bulk(Bytes::from("DECRBY")),
            Frame::Bulk(Bytes::from("key1")),
            Frame::Integer(10),
        ]);
        let cmd = Command::try_from(frame).unwrap();

        assert_eq!(
            cmd,
            Command::DecrBy(DecrBy {
                key: "key1".to_string(),
                decrement: 10,
            })
        );

        let store = Arc::new(Mutex::new(Store::new()));
        {
            let mut store = store.lock().unwrap();
            store.set(String::from("key1"), Bytes::from("20"));
        }

        let result = cmd.exec(store.clone()).unwrap();

        assert_eq!(result, Frame::Simple("OK".to_string()));

        assert_eq!(store.lock().unwrap().get("key1"), Some(&Bytes::from("10")));
    }

    #[tokio::test]
    async fn non_existing_key() {
        let frame = Frame::Array(vec![
            Frame::Bulk(Bytes::from("DECRBY")),
            Frame::Bulk(Bytes::from("key1")),
            Frame::Integer(10),
        ]);
        let cmd = Command::try_from(frame).unwrap();

        assert_eq!(
            cmd,
            Command::DecrBy(DecrBy {
                key: "key1".to_string(),
                decrement: 10,
            })
        );

        let store = Arc::new(Mutex::new(Store::new()));

        let result = cmd.exec(store.clone()).unwrap();

        assert_eq!(result, Frame::Simple("OK".to_string()));

        assert_eq!(store.lock().unwrap().get("key1"), Some(&Bytes::from("-10")));
    }

    #[tokio::test]
    async fn invalid_key_type() {
        let frame = Frame::Array(vec![
            Frame::Bulk(Bytes::from("DECRBY")),
            Frame::Bulk(Bytes::from("key1")),
            Frame::Integer(10),
        ]);
        let cmd = Command::try_from(frame).unwrap();

        assert_eq!(
            cmd,
            Command::DecrBy(DecrBy {
                key: "key1".to_string(),
                decrement: 10,
            })
        );

        let store = Arc::new(Mutex::new(Store::new()));
        {
            let mut store = store.lock().unwrap();
            store.set(String::from("key1"), Bytes::from("value"));
        }

        let result = cmd.exec(store.clone()).unwrap();

        assert_eq!(
            result,
            Frame::Error("value is not of the correct type or out of range".to_string())
        );

        assert_eq!(
            store.lock().unwrap().get("key1"),
            Some(&Bytes::from("value"))
        );
    }

    #[tokio::test]
    async fn out_of_range() {
        let frame = Frame::Array(vec![
            Frame::Bulk(Bytes::from("DECRBY")),
            Frame::Bulk(Bytes::from("key1")),
            Frame::Integer(10),
        ]);
        let cmd = Command::try_from(frame).unwrap();

        assert_eq!(
            cmd,
            Command::DecrBy(DecrBy {
                key: "key1".to_string(),
                decrement: 10,
            })
        );

        let store = Arc::new(Mutex::new(Store::new()));
        {
            let mut store = store.lock().unwrap();
            store.set(String::from("key1"), Bytes::from("999223372036854775808"));
        }

        let result = cmd.exec(store.clone()).unwrap();

        assert_eq!(
            result,
            Frame::Error("value is not of the correct type or out of range".to_string())
        );

        assert_eq!(
            store.lock().unwrap().get("key1"),
            Some(&Bytes::from("999223372036854775808"))
        );
    }
}
