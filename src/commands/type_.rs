use crate::commands::CommandParser;
use crate::Error;

#[derive(Debug, PartialEq)]
pub struct Type {
    pub key: String,
}

impl TryFrom<&mut CommandParser> for Type {
    type Error = Error;

    fn try_from(parser: &mut CommandParser) -> Result<Self, Self::Error> {
        let key = parser.next_string()?;
        Ok(Self { key })
    }
}
