use crate::commands::CommandParser;
use crate::Error;

#[derive(Debug, PartialEq)]
pub struct Exists {
    pub keys: Vec<String>,
}

impl TryFrom<&mut CommandParser> for Exists {
    type Error = Error;

    fn try_from(parser: &mut CommandParser) -> Result<Self, Self::Error> {
        let key = parser.next_string()?;
        println!("key: {}", key);
        Ok(Self { keys: vec![key] })
    }
}
