use crate::commands::CommandParser;
use crate::Error;

#[derive(Debug, PartialEq)]
pub struct Type {
    pub key: String,
}

impl Type {
    pub fn exec(self) -> Result<crate::frame::Frame, Error> {
        Ok(crate::frame::Frame::Simple("OK".to_string()))
    }
}

impl TryFrom<&mut CommandParser> for Type {
    type Error = Error;

    fn try_from(parser: &mut CommandParser) -> Result<Self, Self::Error> {
        let key = parser.next_string()?;
        Ok(Self { key })
    }
}
