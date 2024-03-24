use bytes::Bytes;
use std::sync::{Arc, Mutex};

use crate::commands::CommandParser;
use crate::frame::Frame;
use crate::store::Store;
use crate::Error;

#[derive(Debug, PartialEq)]
pub struct Set {
    pub key: String,
    pub value: Bytes,
}

impl Set {
    pub fn exec(self, store: Arc<Mutex<Store>>) -> Result<Frame, Error> {
        let mut store = store.lock().unwrap();

        store.set(self.key, self.value);

        let res = Frame::Simple("OK".to_string());
        Ok(res)
    }
}

impl TryFrom<&mut CommandParser> for Set {
    type Error = Error;

    fn try_from(parser: &mut CommandParser) -> Result<Self, Self::Error> {
        let key = parser.next_string()?;
        let value = parser.next_bytes()?;

        Ok(Self { key, value })
    }
}
