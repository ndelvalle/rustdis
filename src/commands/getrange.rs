use bytes::Bytes;
use std::sync::{Arc, Mutex};
use std::usize;

use crate::commands::executable::Executable;
use crate::commands::CommandParser;
use crate::frame::Frame;
use crate::store::Store;
use crate::Error;

/// Returns the substring of the string value stored at key, determined by the offsets start and
/// end (both are inclusive). Negative offsets can be used in order to provide an offset starting
/// from the end of the string. So -1 means the last character, -2 the penultimate and so forth.
/// The function handles out of range requests by limiting the resulting range to the actual length
/// of the string.
///
/// Ref: <https://redis.io/docs/latest/commands/getrange/>
#[derive(Debug, PartialEq)]
pub struct Getrange {
    pub key: String,
    pub start: i64,
    pub end: i64,
}

impl Executable for Getrange {
    fn exec(self, store: Arc<Mutex<Store>>) -> Result<Frame, Error> {
        let store = store.lock().unwrap();
        let value = store.get(&self.key);
        let bytes = match value {
            Some(val) => val,
            None => return Ok(Frame::Bulk(Bytes::new())),
        };

        let value = String::from_utf8(bytes.to_vec()).unwrap();
        // TODO: Should we worry about this conversion?
        let len = value.len() as i64;

        let start = get_positive_index(len, self.start);
        let end = get_positive_index(len, self.end);

        let subset: String = value
            .chars()
            // We don't care about out of range indexes, take and skip will handle it.
            .take((end + 1) as usize)
            .skip(start as usize)
            .collect();

        Ok(Frame::Bulk(Bytes::from(subset)))
    }
}

fn get_positive_index(str_len: i64, index: i64) -> i64 {
    let is_positive = index >= 0;
    if is_positive {
        index
    } else {
        str_len - index.abs()
    }
}

impl TryFrom<&mut CommandParser> for Getrange {
    type Error = Error;

    fn try_from(parser: &mut CommandParser) -> Result<Self, Self::Error> {
        let key = parser.next_string()?;
        let start = parser.next_integer()?;
        let end = parser.next_integer()?;

        Ok(Self { key, start, end })
    }
}

#[cfg(test)]
mod tests {
    use bytes::Bytes;

    use crate::commands::Command;

    use super::*;

    #[test]
    fn when_key_exists_using_positive_index() {
        let store = Arc::new(Mutex::new(Store::default()));

        let frame = Frame::Array(vec![
            Frame::Bulk(Bytes::from("GETRANGE")),
            Frame::Bulk(Bytes::from("mykey")),
            Frame::Bulk(Bytes::from("0")),
            Frame::Bulk(Bytes::from("3")),
        ]);
        let cmd = Command::try_from(frame).unwrap();
        assert_eq!(
            cmd,
            Command::Getrange(Getrange {
                key: "mykey".to_string(),
                start: 0,
                end: 3
            })
        );

        store
            .lock()
            .unwrap()
            .set("mykey".to_string(), Bytes::from("This is a string"));

        let res = cmd.exec(store.clone()).unwrap();
        assert_eq!(res, Frame::Bulk(Bytes::from("This")));
    }

    #[test]
    fn when_key_exists_using_negative_index() {
        let store = Arc::new(Mutex::new(Store::default()));

        let frame = Frame::Array(vec![
            Frame::Bulk(Bytes::from("GETRANGE")),
            Frame::Bulk(Bytes::from("mykey")),
            Frame::Bulk(Bytes::from("-3")),
            Frame::Bulk(Bytes::from("-1")),
        ]);
        let cmd = Command::try_from(frame).unwrap();
        assert_eq!(
            cmd,
            Command::Getrange(Getrange {
                key: "mykey".to_string(),
                start: -3,
                end: -1
            })
        );

        store
            .lock()
            .unwrap()
            .set("mykey".to_string(), Bytes::from("This is a string"));

        let res = cmd.exec(store.clone()).unwrap();
        assert_eq!(res, Frame::Bulk(Bytes::from("ing")));
    }

    #[test]
    fn when_key_exists_using_positive_and_negative_index() {
        let store = Arc::new(Mutex::new(Store::default()));

        let frame = Frame::Array(vec![
            Frame::Bulk(Bytes::from("GETRANGE")),
            Frame::Bulk(Bytes::from("mykey")),
            Frame::Bulk(Bytes::from("0")),
            Frame::Bulk(Bytes::from("-1")),
        ]);
        let cmd = Command::try_from(frame).unwrap();
        assert_eq!(
            cmd,
            Command::Getrange(Getrange {
                key: "mykey".to_string(),
                start: 0,
                end: -1
            })
        );

        store
            .lock()
            .unwrap()
            .set("mykey".to_string(), Bytes::from("This is a string"));

        let res = cmd.exec(store.clone()).unwrap();
        assert_eq!(res, Frame::Bulk(Bytes::from("This is a string")));
    }

    #[test]
    fn when_key_exists_using_out_of_bound_index() {
        let store = Arc::new(Mutex::new(Store::default()));

        let frame = Frame::Array(vec![
            Frame::Bulk(Bytes::from("GETRANGE")),
            Frame::Bulk(Bytes::from("mykey")),
            Frame::Bulk(Bytes::from("10")),
            Frame::Bulk(Bytes::from("100")),
        ]);
        let cmd = Command::try_from(frame).unwrap();
        assert_eq!(
            cmd,
            Command::Getrange(Getrange {
                key: "mykey".to_string(),
                start: 10,
                end: 100
            })
        );

        store
            .lock()
            .unwrap()
            .set("mykey".to_string(), Bytes::from("This is a string"));

        let res = cmd.exec(store.clone()).unwrap();
        assert_eq!(res, Frame::Bulk(Bytes::from("string")));
    }
}
