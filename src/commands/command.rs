use crate::commands::CommandParser;
use crate::Error;

#[derive(Debug, PartialEq)]
pub struct Command {}

impl TryFrom<&mut CommandParser> for Command {
    type Error = Error;

    fn try_from(parser: &mut CommandParser) -> Result<Self, Self::Error> {
        Ok(Self {})
    }
}
