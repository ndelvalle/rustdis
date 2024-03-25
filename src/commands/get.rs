use std::sync::{Arc, Mutex};

use crate::commands::CommandParser;
use crate::frame::Frame;
use crate::store::Store;
use crate::Error;

#[derive(Debug, PartialEq)]
pub struct Get {
    pub key: String,
}

impl Get {
    pub fn exec(self, store: Arc<Mutex<Store>>) -> Result<Frame, Error> {
        let store = store.lock().unwrap();
        let value = store.get(&self.key);

        match value {
            Some(value) => Ok(Frame::Bulk(value.clone())),
            None => Ok(Frame::Null),
        }
    }
}

impl TryFrom<&mut CommandParser> for Get {
    type Error = Error;

    fn try_from(parser: &mut CommandParser) -> Result<Self, Self::Error> {
        let key = parser.next_string()?;
        Ok(Self { key })
    }
}
