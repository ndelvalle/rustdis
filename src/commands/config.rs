use crate::commands::CommandParser;
use crate::Error;

#[derive(Debug, PartialEq)]
pub struct Config {}

impl TryFrom<&mut CommandParser> for Config {
    type Error = Error;

    fn try_from(parser: &mut CommandParser) -> Result<Self, Self::Error> {
        Ok(Self {})
    }
}
