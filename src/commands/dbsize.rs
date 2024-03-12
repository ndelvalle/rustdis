use crate::commands::CommandParser;
use crate::Error;

#[derive(Debug, PartialEq)]
pub struct DBSize;

impl TryFrom<&mut CommandParser> for DBSize {
    type Error = Error;

    fn try_from(parser: &mut CommandParser) -> Result<Self, Self::Error> {
        Ok(Self)
    }
}
