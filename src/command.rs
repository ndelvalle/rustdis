use thiserror::Error as ThisError;

use crate::frame::Frame;
use crate::Error;

use std::{str, vec};

enum Command {
    Get(Get),
}

impl TryFrom<Frame> for Command {
    type Error = Error;

    fn try_from(frame: Frame) -> Result<Self, Self::Error> {
        let mut parser = CommandParser::try_from(frame)?;
        let command_name = parser.parse_command_name()?;

        match &command_name[..] {
            "get" => Get::try_from(&mut parser).map(Command::Get),
            name => return Err(format!("protocol error; unknown command {:?}", name).into()),
        }
    }
}

struct Get {
    key: String,
}

impl TryFrom<&mut CommandParser> for Get {
    type Error = Error;

    fn try_from(parser: &mut CommandParser) -> Result<Self, Self::Error> {
        let key = parser.next_string()?;
        Ok(Self { key })
    }
}

struct CommandParser {
    parts: vec::IntoIter<Frame>,
}

impl CommandParser {
    fn parse_command_name(&mut self) -> Result<String, CommandParserError> {
        let command_name = self
            .parts
            .next()
            .ok_or_else(|| CommandParserError::EndOfStream)?;

        match command_name {
            Frame::Simple(s) => Ok(s.to_lowercase()),
            frame => Err(CommandParserError::InvalidFrame {
                expected: "simple string".to_string(),
                actual: frame,
            }),
        }
    }

    fn next_string(&mut self) -> Result<String, CommandParserError> {
        let frame = self
            .parts
            .next()
            .ok_or_else(|| CommandParserError::EndOfStream)?;

        match frame {
            // Both `Simple` and `Bulk` representation may be strings. Strings are parsed to UTF-8.
            // While errors are stored as strings, they are considered separate types.
            Frame::Simple(s) => Ok(s),
            Frame::Bulk(bytes) => str::from_utf8(&bytes[..])
                .map(|s| s.to_string())
                .map_err(CommandParserError::InvalidUTF8String),
            frame => Err(CommandParserError::InvalidFrame {
                expected: "simple or bulk string".to_string(),
                actual: frame,
            }),
        }
    }
}

#[derive(Debug, ThisError)]
pub(crate) enum CommandParserError {
    #[error("protocol error; invalid frame, expected {expected}, got {actual}")]
    InvalidFrame { expected: String, actual: Frame },
    #[error("protocol error; invalid UTF-8 string")]
    InvalidUTF8String(#[from] str::Utf8Error),
    #[error("protocol error; attempting to extract a value failed due to the frame being fully consumed")]
    EndOfStream,
}

impl TryFrom<Frame> for CommandParser {
    type Error = CommandParserError;

    fn try_from(frame: Frame) -> Result<Self, Self::Error> {
        // Clients send commands to the Redis server as RESP arrays.
        let frames = match frame {
            Frame::Array(array) => array,
            frame => {
                return Err(CommandParserError::InvalidFrame {
                    expected: "array".to_string(),
                    actual: frame,
                })
            }
        };

        Ok(Self {
            parts: frames.into_iter(),
        })
    }
}
