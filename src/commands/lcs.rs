use bytes::Bytes;
use std::str::from_utf8;
use std::sync::{Arc, Mutex};

use crate::commands::executable::Executable;
use crate::commands::CommandParser;
use crate::frame::Frame;
use crate::store::Store;
use crate::utils::lcs::lcs;
use crate::Error;

use super::CommandParserError;

/// The LCS command implements the longest common subsequence algorithm.
///
/// Note that this is different than the longest common string algorithm,
/// since matching characters in the string does not need to be contiguous.
///
/// Ref: <https://redis.io/docs/latest/commands/lcs>
#[derive(Debug, PartialEq)]
pub struct Lcs {
    pub key1: String,
    pub key2: String,
    pub len: bool,
}

impl Executable for Lcs {
    fn exec(self, store: Arc<Mutex<Store>>) -> Result<Frame, Error> {
        let store = store.lock().unwrap();

        let str1 = from_utf8(
            store
                .get(&self.key1)
                .map(|b| b.as_ref())
                .unwrap_or_default(),
        )
        .unwrap_or_default();

        let str2 = from_utf8(
            store
                .get(&self.key2)
                .map(|b| b.as_ref())
                .unwrap_or_default(),
        )
        .unwrap_or_default();

        let res = lcs(str1, str2);

        let res = if self.len {
            Frame::Integer(res.len() as i64)
        } else {
            Frame::Bulk(Bytes::from(res))
        };

        Ok(res)
    }
}

impl TryFrom<&mut CommandParser> for Lcs {
    type Error = Error;

    fn try_from(parser: &mut CommandParser) -> Result<Self, Self::Error> {
        let key1 = parser.next_string()?;
        let key2 = parser.next_string()?;
        let len = match parser.next_string() {
            Ok(s) => s == "LEN",
            Err(CommandParserError::EndOfStream) => false,
            Err(err) => return Err(err.into()),
        };

        Ok(Self { key1, key2, len })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::commands::Command;
    use bytes::Bytes;

    #[tokio::test]
    async fn no_match() {
        let store = Arc::new(Mutex::new(Store::new()));

        let frame = Frame::Array(vec![
            Frame::Bulk(Bytes::from("LCS")),
            Frame::Bulk(Bytes::from("foo")),
            Frame::Bulk(Bytes::from("bar")),
        ]);
        let cmd = Command::try_from(frame).unwrap();

        assert_eq!(
            cmd,
            Command::Lcs(Lcs {
                key1: String::from("foo"),
                key2: String::from("bar"),
                len: false
            })
        );

        {
            let mut store = store.lock().unwrap();
            store.set(String::from("foo"), Bytes::from("1"));
            store.set(String::from("bar"), Bytes::from("2"));
        }

        let res = cmd.exec(store.clone()).unwrap();

        assert_eq!(res, Frame::Bulk(Bytes::from("")));
    }

    #[tokio::test]
    async fn full_match() {
        let store = Arc::new(Mutex::new(Store::new()));

        let frame = Frame::Array(vec![
            Frame::Bulk(Bytes::from("LCS")),
            Frame::Bulk(Bytes::from("foo")),
            Frame::Bulk(Bytes::from("bar")),
        ]);
        let cmd = Command::try_from(frame).unwrap();

        assert_eq!(
            cmd,
            Command::Lcs(Lcs {
                key1: String::from("foo"),
                key2: String::from("bar"),
                len: false
            })
        );

        {
            let mut store = store.lock().unwrap();
            store.set(String::from("foo"), Bytes::from("abc"));
            store.set(String::from("bar"), Bytes::from("abc"));
        }

        let res = cmd.exec(store.clone()).unwrap();

        assert_eq!(res, Frame::Bulk(Bytes::from("abc")));
    }

    #[tokio::test]
    async fn partial_match() {
        let store = Arc::new(Mutex::new(Store::new()));

        let frame = Frame::Array(vec![
            Frame::Bulk(Bytes::from("LCS")),
            Frame::Bulk(Bytes::from("foo")),
            Frame::Bulk(Bytes::from("bar")),
        ]);
        let cmd = Command::try_from(frame).unwrap();

        assert_eq!(
            cmd,
            Command::Lcs(Lcs {
                key1: String::from("foo"),
                key2: String::from("bar"),
                len: false
            })
        );

        {
            let mut store = store.lock().unwrap();
            store.set(String::from("foo"), Bytes::from("hello world"));
            store.set(String::from("bar"), Bytes::from("world hello"));
        }

        let res = cmd.exec(store.clone()).unwrap();

        assert_eq!(res, Frame::Bulk(Bytes::from("world")));
    }

    #[tokio::test]
    async fn partial_match_inverted() {
        let store = Arc::new(Mutex::new(Store::new()));

        let frame = Frame::Array(vec![
            Frame::Bulk(Bytes::from("LCS")),
            Frame::Bulk(Bytes::from("bar")),
            Frame::Bulk(Bytes::from("foo")),
        ]);
        let cmd = Command::try_from(frame).unwrap();

        assert_eq!(
            cmd,
            Command::Lcs(Lcs {
                key1: String::from("bar"),
                key2: String::from("foo"),
                len: false
            })
        );

        {
            let mut store = store.lock().unwrap();
            store.set(String::from("foo"), Bytes::from("hello world"));
            store.set(String::from("bar"), Bytes::from("world hello"));
        }

        let res = cmd.exec(store.clone()).unwrap();

        assert_eq!(res, Frame::Bulk(Bytes::from("hello")));
    }

    #[tokio::test]
    async fn len() {
        let store = Arc::new(Mutex::new(Store::new()));

        let frame = Frame::Array(vec![
            Frame::Bulk(Bytes::from("LCS")),
            Frame::Bulk(Bytes::from("foo")),
            Frame::Bulk(Bytes::from("bar")),
            Frame::Bulk(Bytes::from("LEN")),
        ]);
        let cmd = Command::try_from(frame).unwrap();

        assert_eq!(
            cmd,
            Command::Lcs(Lcs {
                key1: String::from("foo"),
                key2: String::from("bar"),
                len: true
            })
        );

        {
            let mut store = store.lock().unwrap();
            store.set(String::from("foo"), Bytes::from("hello world"));
            store.set(String::from("bar"), Bytes::from("world hello"));
        }

        let res = cmd.exec(store.clone()).unwrap();

        assert_eq!(res, Frame::Integer(5));
    }

    #[tokio::test]
    async fn len_no_match() {
        let store = Arc::new(Mutex::new(Store::new()));

        let frame = Frame::Array(vec![
            Frame::Bulk(Bytes::from("LCS")),
            Frame::Bulk(Bytes::from("foo")),
            Frame::Bulk(Bytes::from("bar")),
            Frame::Bulk(Bytes::from("LEN")),
        ]);
        let cmd = Command::try_from(frame).unwrap();

        assert_eq!(
            cmd,
            Command::Lcs(Lcs {
                key1: String::from("foo"),
                key2: String::from("bar"),
                len: true
            })
        );

        {
            let mut store = store.lock().unwrap();
            store.set(String::from("foo"), Bytes::from("1"));
            store.set(String::from("bar"), Bytes::from("2"));
        }

        let res = cmd.exec(store.clone()).unwrap();

        assert_eq!(res, Frame::Integer(0));
    }

    #[tokio::test]
    async fn len_full_match() {
        let store = Arc::new(Mutex::new(Store::new()));

        let frame = Frame::Array(vec![
            Frame::Bulk(Bytes::from("LCS")),
            Frame::Bulk(Bytes::from("foo")),
            Frame::Bulk(Bytes::from("bar")),
            Frame::Bulk(Bytes::from("LEN")),
        ]);
        let cmd = Command::try_from(frame).unwrap();

        assert_eq!(
            cmd,
            Command::Lcs(Lcs {
                key1: String::from("foo"),
                key2: String::from("bar"),
                len: true
            })
        );

        {
            let mut store = store.lock().unwrap();
            store.set(String::from("foo"), Bytes::from("abc"));
            store.set(String::from("bar"), Bytes::from("abc"));
        }

        let res = cmd.exec(store.clone()).unwrap();

        assert_eq!(res, Frame::Integer(3));
    }

    #[tokio::test]
    async fn len_partial_match() {
        let store = Arc::new(Mutex::new(Store::new()));

        let frame = Frame::Array(vec![
            Frame::Bulk(Bytes::from("LCS")),
            Frame::Bulk(Bytes::from("foo")),
            Frame::Bulk(Bytes::from("bar")),
            Frame::Bulk(Bytes::from("LEN")),
        ]);
        let cmd = Command::try_from(frame).unwrap();

        assert_eq!(
            cmd,
            Command::Lcs(Lcs {
                key1: String::from("foo"),
                key2: String::from("bar"),
                len: true
            })
        );

        {
            let mut store = store.lock().unwrap();
            store.set(String::from("foo"), Bytes::from("hello world"));
            store.set(String::from("bar"), Bytes::from("world hello"));
        }

        let res = cmd.exec(store.clone()).unwrap();

        assert_eq!(res, Frame::Integer(5));
    }

    #[tokio::test]
    async fn missing_keys() {
        let store = Arc::new(Mutex::new(Store::new()));

        let frame = Frame::Array(vec![
            Frame::Bulk(Bytes::from("LCS")),
            Frame::Bulk(Bytes::from("foo")),
            Frame::Bulk(Bytes::from("bar")),
        ]);
        let cmd = Command::try_from(frame).unwrap();

        assert_eq!(
            cmd,
            Command::Lcs(Lcs {
                key1: String::from("foo"),
                key2: String::from("bar"),
                len: false
            })
        );

        let res = cmd.exec(store.clone()).unwrap();

        assert_eq!(res, Frame::Bulk(Bytes::from("")));
    }
}
