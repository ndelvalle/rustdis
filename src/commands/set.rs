use bytes::Bytes;

use crate::commands::CommandParser;
use crate::Error;

#[derive(Debug, PartialEq)]
pub struct Set {
    pub key: String,
    pub value: Bytes,
}

impl TryFrom<&mut CommandParser> for Set {
    type Error = Error;

    fn try_from(parser: &mut CommandParser) -> Result<Self, Self::Error> {
        let key = parser.next_string()?;
        let value = parser.next_bytes()?;

        Ok(Self { key, value })
    }
}
