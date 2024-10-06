use tokio::time::Duration;

use crate::commands::executable::Executable;
use crate::commands::{CommandParser, CommandParserError};
use crate::frame::Frame;
use crate::store::Store;
use crate::Error;

/// Get the value of key and optionally set its expiration.
///
/// Ref: <https://redis.io/docs/latest/commands/getex/>
#[derive(Debug, PartialEq)]
pub struct Getex {
    pub key: String,

    pub ttl: Option<Ttl>,
}

#[derive(Debug, PartialEq)]
pub enum Ttl {
    Ex(u64),
    Px(u64),
    ExAt(u64),
    PxAt(u64),
    Persist, // Remove the expiration.
}

impl Ttl {
    pub fn duration(&self) -> Duration {
        match self {
            Ttl::Ex(seconds) => Duration::from_secs(*seconds),
            Ttl::Px(millis) => Duration::from_millis(*millis),
            // TODO: EXAT, PXAT and KeepTtl.
            _ => Duration::from_secs(1),
        }
    }
}

impl Executable for Getex {
    fn exec(self, store: Store) -> Result<Frame, Error> {
        let mut store = store.lock();
        let value = store.get(&self.key);

        match (value, self.ttl) {
            (Some(value), Some(Ttl::Persist)) => {
                store.remove_ttl(&self.key);

                Ok(Frame::Bulk(value.clone()))
            }
            (Some(value), Some(ttl)) => {
                store.set_with_ttl(self.key, value.clone(), ttl.duration());

                Ok(Frame::Bulk(value.clone()))
            }
            (Some(value), None) => Ok(Frame::Bulk(value.clone())),
            (None, _) => Ok(Frame::NullBulkString),
        }
    }
}

impl TryFrom<&mut CommandParser> for Getex {
    type Error = Error;

    fn try_from(parser: &mut CommandParser) -> Result<Self, Self::Error> {
        let key = parser.next_string()?;

        let mut ttl = None;

        loop {
            let option = match parser.next_string() {
                Ok(option) => option,
                Err(CommandParserError::EndOfStream) => {
                    break;
                }
                Err(err) => return Err(err.into()),
            };

            match option.to_uppercase().as_str() {
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
                "PERSIST" if ttl.is_none() => {
                    ttl = Some(Ttl::Persist);
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

        Ok(Self { key, ttl })
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
            Frame::Bulk(Bytes::from("GETEX")),
            Frame::Bulk(Bytes::from("key1")),
            Frame::Bulk(Bytes::from("PERSIST")),
        ]);
        let cmd = Command::try_from(frame).unwrap();

        assert_eq!(
            cmd,
            Command::Getex(Getex {
                key: String::from("key1"),
                ttl: Some(Ttl::Persist),
            })
        );

        store.lock().set(String::from("key1"), Bytes::from("1"));

        let result = cmd.exec(store.clone()).unwrap();

        assert_eq!(result, Frame::Bulk(Bytes::from("1")));
    }

    #[tokio::test]
    async fn non_existing_key() {
        let store = Store::new();

        let frame = Frame::Array(vec![
            Frame::Bulk(Bytes::from("GETEX")),
            Frame::Bulk(Bytes::from("key1")),
            Frame::Bulk(Bytes::from("EX")),
            Frame::Integer(10),
        ]);
        let cmd = Command::try_from(frame).unwrap();

        assert_eq!(
            cmd,
            Command::Getex(Getex {
                key: String::from("key1"),
                ttl: Some(Ttl::Ex(10)),
            })
        );

        store.lock().set(String::from("key1"), Bytes::from("1"));

        let res = cmd.exec(store.clone()).unwrap();

        assert_eq!(res, Frame::Bulk(Bytes::from("1")));
    }
}
