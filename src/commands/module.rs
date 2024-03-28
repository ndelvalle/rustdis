use std::sync::{Arc, Mutex};

use crate::commands::executable::Executable;
use crate::commands::CommandParser;
use crate::frame::Frame;
use crate::store::Store;
use crate::Error;

#[derive(Debug, PartialEq)]
pub struct Module;

impl Executable for Module {
    fn exec(self, _store: Arc<Mutex<Store>>) -> Result<Frame, Error> {
        Ok(Frame::Simple("OK".to_string()))
    }
}

impl TryFrom<&mut CommandParser> for Module {
    type Error = Error;

    fn try_from(_parser: &mut CommandParser) -> Result<Self, Self::Error> {
        Ok(Self)
    }
}
