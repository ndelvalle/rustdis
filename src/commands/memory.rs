use std::sync::{Arc, Mutex};

use crate::commands::executable::Executable;
use crate::commands::{CommandParser, CommandParserError};
use crate::frame::Frame;
use crate::store::Store;
use crate::Error;

#[derive(Debug, PartialEq)]
pub enum Memory {
    Usage(Usage),
}

/// Ref: <https://redis.io/docs/latest/commands/memory-usage>
///
/// The MEMORY USAGE command reports the number of bytes that a key and its value require to be
/// stored in RAM.
#[derive(Debug, PartialEq)]
pub struct Usage {
    pub key: String,
}

impl Executable for Memory {
    fn exec(self, store: Arc<Mutex<Store>>) -> Result<Frame, Error> {
        match self {
            Self::Usage(encoding) => encoding.exec(store),
        }
    }
}

impl TryFrom<&mut CommandParser> for Memory {
    type Error = Error;

    fn try_from(parser: &mut CommandParser) -> Result<Self, Self::Error> {
        let sub_command = parser.next_string()?;
        let sub_command = sub_command.to_lowercase();

        match sub_command.as_str() {
            "usage" => {
                let key = parser.next_string()?;
                Ok(Self::Usage(Usage { key }))
            }
            _ => Err(CommandParserError::UnknownCommand {
                command: format!("MEMORY {}", sub_command.to_uppercase()),
            }
            .into()),
        }
    }
}

impl Executable for Usage {
    fn exec(self, store: Arc<Mutex<Store>>) -> Result<Frame, Error> {
        let store = store.lock().unwrap();
        let res = match store.get(&self.key) {
            Some(value) => Frame::Integer(value.len() as i64),
            None => Frame::Null,
        };

        Ok(res)
    }
}
