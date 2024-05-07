use bytes::Bytes;
use std::sync::{Arc, Mutex};

use crate::commands::executable::Executable;
use crate::commands::{CommandParser, CommandParserError};
use crate::frame::Frame;
use crate::store::Store;
use crate::Error;

#[derive(Debug, PartialEq)]
pub enum Object {
    Encoding(Encoding),
}

/// Encoding returns the internal encoding for the Redis object stored at `<key>`.
///
/// Ref: <https://redis.io/docs/latest/commands/object-encoding>
#[derive(Debug, PartialEq)]
pub struct Encoding {
    pub key: String,
}

impl Executable for Object {
    fn exec(self, store: Arc<Mutex<Store>>) -> Result<Frame, Error> {
        match self {
            Self::Encoding(encoding) => encoding.exec(store),
        }
    }
}

impl TryFrom<&mut CommandParser> for Object {
    type Error = Error;

    fn try_from(parser: &mut CommandParser) -> Result<Self, Self::Error> {
        let sub_command = parser.next_string()?;
        let sub_command = sub_command.to_lowercase();

        match sub_command.as_str() {
            "encoding" => {
                let key = parser.next_string()?;
                Ok(Self::Encoding(Encoding { key }))
            }
            _ => Err(CommandParserError::UnknownCommand {
                command: format!("OBJECT {}", sub_command.to_uppercase()),
            }
            .into()),
        }
    }
}

impl Executable for Encoding {
    fn exec(self, store: Arc<Mutex<Store>>) -> Result<Frame, Error> {
        let store = store.lock().unwrap();
        let res = if store.exists(&self.key) {
            Frame::Bulk(Bytes::from("raw"))
        } else {
            Frame::Null
        };

        Ok(res)
    }
}
