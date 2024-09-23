use bytes::Bytes;
use tokio::time::Duration;

use crate::commands::executable::Executable;
use crate::commands::{CommandParser, CommandParserError};
use crate::frame::Frame;
use crate::store::Store;
use crate::Error;

/// Set `key` to hold the `string` value. If `key` already holds a value, it is overwritten.
///
/// Ref: <https://redis.io/docs/latest/commands/set/>
#[derive(Debug, PartialEq)]
pub struct Set {
    pub key: String,
    pub value: Bytes,

    pub ttl: Option<Ttl>,
    pub behavior: Option<SetBehavior>,
    pub get: bool,
}

#[derive(Debug, PartialEq)]
pub enum SetBehavior {
    /// Only set the key if it does not already exist.
    Nx,
    /// Only set the key if it already exists.
    Xx,
}

#[derive(Debug, PartialEq)]
pub enum Ttl {
    Ex(u64),
    Px(u64),
    ExAt(u64),
    PxAt(u64),
    KeepTtl, // Retain the time to live associated with the key.
}

impl Ttl {
    pub fn duration(&self) -> Duration {
        match self {
            Ttl::Ex(seconds) => Duration::from_secs(*seconds),
            _ => Duration::from_secs(1),
        }
    }
}

impl Executable for Set {
    fn exec(self, store: Store) -> Result<Frame, Error> {
        let mut store = store.lock();
        let value = store.get(&self.key);

        match self.behavior {
            Some(SetBehavior::Nx) if value.is_some() => return Ok(Frame::NullBulkString),
            Some(SetBehavior::Xx) if value.is_none() => return Ok(Frame::NullBulkString),
            _ => {}
        }

        match self.ttl {
            Some(ttl) => store.set_with_ttl(self.key, self.value, ttl.duration()),
            None => store.set(self.key, self.value),
        };

        let res = if self.get {
            value.map_or(Frame::NullBulkString, Frame::Bulk)
        } else {
            Frame::Simple("OK".to_string())
        };

        Ok(res)
    }
}

impl TryFrom<&mut CommandParser> for Set {
    type Error = Error;

    fn try_from(parser: &mut CommandParser) -> Result<Self, Self::Error> {
        let key = parser.next_string()?;
        let value = parser.next_bytes()?;

        let mut ttl = None;
        let mut behavior = None;
        let mut get = false;

        loop {
            let option = match parser.next_string() {
                Ok(option) => option,
                Err(CommandParserError::EndOfStream) => {
                    break;
                }
                Err(err) => return Err(err.into()),
            };

            match option.as_str() {
                // TTL options
                "EX" if ttl.is_none() => {
                    let val = parser.next_integer()?;
                    ttl = Some(Ttl::Ex(val as u64));
                }
                "PX" if ttl.is_none() => {
                    let val = parser.next_integer()?;
                    ttl = Some(Ttl::Px(val as u64));
                }
                "EXAT" if ttl.is_none() => {
                    let val = parser.next_integer()?;
                    ttl = Some(Ttl::ExAt(val as u64));
                }
                "PXAT" if ttl.is_none() => {
                    let val = parser.next_integer()?;
                    ttl = Some(Ttl::PxAt(val as u64));
                }
                "KEEPTTL" if ttl.is_none() => {
                    ttl = Some(Ttl::KeepTtl);
                }

                // Behavior options
                "NX" if behavior.is_none() => {
                    behavior = Some(SetBehavior::Nx);
                }
                "XX" if behavior.is_none() => {
                    behavior = Some(SetBehavior::Xx);
                }

                // Get option
                "GET" => {
                    get = true;
                }

                // Unexpected option
                _ => {
                    return Err(CommandParserError::InvalidCommandArgument {
                        command: "SET".to_string(),
                        argument: option,
                    }
                    .into())
                }
            }
        }

        Ok(Self {
            key,
            value,
            ttl,
            behavior,
            get,
        })
    }
}

#[cfg(test)]
mod tests {
    use bytes::Bytes;

    use super::*;
    use crate::commands::Command;

    #[tokio::test]
    async fn insert_one() {
        let store = Store::new();

        let frame = Frame::Array(vec![
            Frame::Bulk(Bytes::from("SET")),
            Frame::Bulk(Bytes::from("key1")),
            Frame::Bulk(Bytes::from("1")),
        ]);
        let cmd = Command::try_from(frame).unwrap();

        assert_eq!(
            cmd,
            Command::Set(Set {
                key: String::from("key1"),
                value: Bytes::from("1"),
                ttl: None,
                behavior: None,
                get: false
            })
        );

        let res = cmd.exec(store.clone()).unwrap();

        assert_eq!(res, Frame::Simple("OK".to_string()));
        assert_eq!(store.lock().get("key1"), Some(Bytes::from("1")));
    }

    #[tokio::test]
    async fn overwrite_existing() {
        let store = Store::new();

        let frame = Frame::Array(vec![
            Frame::Bulk(Bytes::from("SET")),
            Frame::Bulk(Bytes::from("key1")),
            Frame::Bulk(Bytes::from("2")),
        ]);
        let cmd = Command::try_from(frame).unwrap();

        assert_eq!(
            cmd,
            Command::Set(Set {
                key: String::from("key1"),
                value: Bytes::from("2"),
                ttl: None,
                behavior: None,
                get: false
            })
        );

        store.lock().set(String::from("key1"), Bytes::from("1"));

        assert_eq!(store.lock().get("key1"), Some(Bytes::from("1")));

        let res = cmd.exec(store.clone()).unwrap();

        assert_eq!(res, Frame::Simple("OK".to_string()));
        assert_eq!(store.lock().get("key1"), Some(Bytes::from("2")));
    }

    #[tokio::test]
    async fn ttl_ex_and_xx_behavior() {
        let store = Store::new();

        let frame = Frame::Array(vec![
            Frame::Bulk(Bytes::from("SET")),
            Frame::Bulk(Bytes::from("key1")),
            Frame::Bulk(Bytes::from("3")),
            Frame::Bulk(Bytes::from("EX")),
            Frame::Bulk(Bytes::from("10")),
            Frame::Bulk(Bytes::from("XX")),
        ]);
        let cmd = Command::try_from(frame).unwrap();

        assert_eq!(
            cmd,
            Command::Set(Set {
                key: String::from("key1"),
                value: Bytes::from("3"),
                ttl: Some(Ttl::Ex(10)),
                behavior: Some(SetBehavior::Xx),
                get: false
            })
        );

        let res = cmd.exec(store.clone()).unwrap();

        assert_eq!(res, Frame::NullBulkString);
        assert_eq!(store.lock().get("key1"), None);
    }

    #[tokio::test]
    async fn xx_behavior() {
        let store = Store::new();

        store.lock().set(String::from("key1"), Bytes::from("1"));

        let frame = Frame::Array(vec![
            Frame::Bulk(Bytes::from("SET")),
            Frame::Bulk(Bytes::from("key1")),
            Frame::Bulk(Bytes::from("3")),
            Frame::Bulk(Bytes::from("XX")),
        ]);
        let cmd = Command::try_from(frame).unwrap();

        assert_eq!(
            cmd,
            Command::Set(Set {
                key: String::from("key1"),
                value: Bytes::from("3"),
                ttl: None,
                behavior: Some(SetBehavior::Xx),
                get: false
            })
        );

        let res = cmd.exec(store.clone()).unwrap();

        assert_eq!(res, Frame::Simple("OK".to_string()));
        assert_eq!(store.lock().get("key1"), Some(Bytes::from("3")));
    }

    #[tokio::test]
    async fn nx_behavior() {
        let store = Store::new();

        let frame = Frame::Array(vec![
            Frame::Bulk(Bytes::from("SET")),
            Frame::Bulk(Bytes::from("key1")),
            Frame::Bulk(Bytes::from("3")),
            Frame::Bulk(Bytes::from("NX")),
        ]);
        let cmd = Command::try_from(frame).unwrap();

        assert_eq!(
            cmd,
            Command::Set(Set {
                key: String::from("key1"),
                value: Bytes::from("3"),
                ttl: None,
                behavior: Some(SetBehavior::Nx),
                get: false
            })
        );

        let res = cmd.exec(store.clone()).unwrap();

        assert_eq!(res, Frame::Simple("OK".to_string()));
        assert_eq!(store.lock().get("key1"), Some(Bytes::from("3")));
    }

    #[tokio::test]
    async fn ttl_exat_and_nx_behavior() {
        let store = Store::new();

        let frame = Frame::Array(vec![
            Frame::Bulk(Bytes::from("SET")),
            Frame::Bulk(Bytes::from("key1")),
            Frame::Bulk(Bytes::from("3")),
            Frame::Bulk(Bytes::from("EXAT")),
            Frame::Bulk(Bytes::from("10")),
            Frame::Bulk(Bytes::from("NX")),
        ]);
        let cmd = Command::try_from(frame).unwrap();

        assert_eq!(
            cmd,
            Command::Set(Set {
                key: String::from("key1"),
                value: Bytes::from("3"),
                ttl: Some(Ttl::ExAt(10)),
                behavior: Some(SetBehavior::Nx),
                get: false
            })
        );

        let res = cmd.exec(store.clone()).unwrap();

        assert_eq!(res, Frame::Simple("OK".to_string()));
        assert_eq!(store.lock().get("key1"), Some(Bytes::from("3")));
    }

    #[tokio::test]
    async fn ttl_exat_and_nx_behavior_and_get_order_swapped() {
        let store = Store::new();

        let frame = Frame::Array(vec![
            Frame::Bulk(Bytes::from("SET")),
            Frame::Bulk(Bytes::from("key1")),
            Frame::Bulk(Bytes::from("3")),
            Frame::Bulk(Bytes::from("GET")),
            Frame::Bulk(Bytes::from("NX")),
            Frame::Bulk(Bytes::from("EXAT")),
            Frame::Bulk(Bytes::from("10")),
        ]);
        let cmd = Command::try_from(frame).unwrap();

        assert_eq!(
            cmd,
            Command::Set(Set {
                key: String::from("key1"),
                value: Bytes::from("3"),
                ttl: Some(Ttl::ExAt(10)),
                behavior: Some(SetBehavior::Nx),
                get: true
            })
        );

        let res = cmd.exec(store.clone()).unwrap();

        assert_eq!(res, Frame::NullBulkString);
        assert_eq!(store.lock().get("key1"), Some(Bytes::from("3")));
    }

    #[tokio::test]
    async fn with_get() {
        let store = Store::new();

        let frame = Frame::Array(vec![
            Frame::Bulk(Bytes::from("SET")),
            Frame::Bulk(Bytes::from("key1")),
            Frame::Bulk(Bytes::from("3")),
            Frame::Bulk(Bytes::from("GET")),
        ]);
        let cmd = Command::try_from(frame).unwrap();

        assert_eq!(
            cmd,
            Command::Set(Set {
                key: String::from("key1"),
                value: Bytes::from("3"),
                ttl: None,
                behavior: None,
                get: true
            })
        );

        let res = cmd.exec(store.clone()).unwrap();

        assert_eq!(res, Frame::NullBulkString);
        assert_eq!(store.lock().get("key1"), Some(Bytes::from("3")));
    }

    #[tokio::test]
    async fn keepttl() {
        let store = Store::new();

        let frame = Frame::Array(vec![
            Frame::Bulk(Bytes::from("SET")),
            Frame::Bulk(Bytes::from("key1")),
            Frame::Bulk(Bytes::from("3")),
            Frame::Bulk(Bytes::from("KEEPTTL")),
        ]);
        let cmd = Command::try_from(frame).unwrap();

        assert_eq!(
            cmd,
            Command::Set(Set {
                key: String::from("key1"),
                value: Bytes::from("3"),
                ttl: Some(Ttl::KeepTtl),
                behavior: None,
                get: false
            })
        );

        let res = cmd.exec(store.clone()).unwrap();

        assert_eq!(res, Frame::Simple("OK".to_string()));
        assert_eq!(store.lock().get("key1"), Some(Bytes::from("3")));
    }

    #[tokio::test]
    async fn missing_ttl_argument() {
        let frame = Frame::Array(vec![
            Frame::Bulk(Bytes::from("SET")),
            Frame::Bulk(Bytes::from("key1")),
            Frame::Bulk(Bytes::from("3")),
            Frame::Bulk(Bytes::from("EX")),
        ]);
        let res = Command::try_from(frame);

        assert!(res.is_err());
    }

    #[tokio::test]
    async fn repeated_behavior_options() {
        let frame = Frame::Array(vec![
            Frame::Bulk(Bytes::from("SET")),
            Frame::Bulk(Bytes::from("key1")),
            Frame::Bulk(Bytes::from("3")),
            Frame::Bulk(Bytes::from("NX")),
            Frame::Bulk(Bytes::from("XX")),
        ]);
        let res = Command::try_from(frame);

        assert!(res.is_err());
    }

    #[tokio::test]
    async fn repeated_ttl_options() {
        let frame = Frame::Array(vec![
            Frame::Bulk(Bytes::from("SET")),
            Frame::Bulk(Bytes::from("key1")),
            Frame::Bulk(Bytes::from("3")),
            Frame::Bulk(Bytes::from("EX")),
            Frame::Bulk(Bytes::from("10")),
            Frame::Bulk(Bytes::from("PX")),
            Frame::Bulk(Bytes::from("10")),
        ]);
        let res = Command::try_from(frame);

        assert!(res.is_err());
    }

    #[tokio::test]
    async fn invalid_command() {
        let frame = Frame::Array(vec![
            Frame::Bulk(Bytes::from("SET")),
            Frame::Bulk(Bytes::from("key1")),
            Frame::Bulk(Bytes::from("3")),
            Frame::Bulk(Bytes::from("INVALID")),
        ]);
        let res = Command::try_from(frame);

        assert!(res.is_err());
    }
}
