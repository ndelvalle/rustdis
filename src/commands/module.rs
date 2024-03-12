use crate::commands::CommandParser;
use crate::Error;

#[derive(Debug, PartialEq)]
pub struct Module {}

impl TryFrom<&mut CommandParser> for Module {
    type Error = Error;

    fn try_from(parser: &mut CommandParser) -> Result<Self, Self::Error> {
        Ok(Self {})
    }
}
