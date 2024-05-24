use std::sync::{Arc, Mutex};

use crate::commands::executable::Executable;
use crate::commands::CommandParser;
use crate::frame::Frame;
use crate::store::Store;
use crate::Error;

/// Return the number of keys in the current database
///
/// Ref: <https://redis.io/docs/latest/commands/dbsize/>
#[derive(Debug, PartialEq)]
pub struct DBSize;

impl Executable for DBSize {
    fn exec(self, store: Arc<Mutex<Store>>) -> Result<Frame, Error> {
        Ok(Frame::Integer(store.lock().unwrap().size() as i64))
    }
}

impl TryFrom<&mut CommandParser> for DBSize {
    type Error = Error;

    fn try_from(_parser: &mut CommandParser) -> Result<Self, Self::Error> {
        Ok(Self)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::commands::Command;
    use bytes::Bytes;

    #[tokio::test]
    async fn zero_keys() {
        let frame = Frame::Array(vec![Frame::Bulk(Bytes::from("DBSIZE"))]);
        let cmd = Command::try_from(frame).unwrap();

        assert_eq!(cmd, Command::DBsize(DBSize));

        let store = Arc::new(Mutex::new(Store::new()));

        let result = cmd.exec(store.clone()).unwrap();

        assert_eq!(result, Frame::Integer(0));
    }

    #[tokio::test]
    async fn multiple_keys() {
        let frame = Frame::Array(vec![Frame::Bulk(Bytes::from("DBSIZE"))]);
        let cmd = Command::try_from(frame).unwrap();

        assert_eq!(cmd, Command::DBsize(DBSize));

        let store = Arc::new(Mutex::new(Store::new()));
        {
            let mut store = store.lock().unwrap();
            store.set(String::from("key1"), Bytes::from("1"));
            store.set(String::from("key2"), Bytes::from("2"));
            store.set(String::from("key3"), Bytes::from("3"));
        }

        let result = cmd.exec(store.clone()).unwrap();

        assert_eq!(result, Frame::Integer(3));
    }
}
