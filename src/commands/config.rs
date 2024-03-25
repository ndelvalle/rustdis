use crate::commands::CommandParser;
use crate::frame::Frame;
use crate::Error;

#[derive(Debug, PartialEq)]
pub struct Config;

impl Config {
    pub fn exec(self) -> Result<Frame, Error> {
        Ok(Frame::Simple("OK".to_string()))
    }
}

impl TryFrom<&mut CommandParser> for Config {
    type Error = Error;

    fn try_from(parser: &mut CommandParser) -> Result<Self, Self::Error> {
        Ok(Self {})
    }
}
