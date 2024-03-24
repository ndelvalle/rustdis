use crate::commands::CommandParser;
use crate::frame::Frame;
use crate::Error;

#[derive(Debug, PartialEq)]
pub struct Command;

impl Command {
    pub fn exec(self) -> Result<Frame, Error> {
        Ok(Frame::Simple("OK".to_string()))
    }
}

impl TryFrom<&mut CommandParser> for Command {
    type Error = Error;

    fn try_from(parser: &mut CommandParser) -> Result<Self, Self::Error> {
        Ok(Self {})
    }
}
