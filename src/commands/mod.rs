pub mod client;
pub mod command;
pub mod config;
pub mod dbsize;
pub mod exists;
pub mod get;
pub mod info;
pub mod module;
pub mod set;
pub mod type_;

use bytes::Bytes;
use std::{str, vec};
use thiserror::Error as ThisError;

use crate::frame::Frame;
use crate::Error;

use client::Client;
use command::Command as Foo;
use config::Config;
use dbsize::DBSize;
use exists::Exists;
use get::Get;
use info::Info;
use module::Module;
use set::Set;
use type_::Type;

#[derive(Debug, PartialEq)]
pub enum Command {
    Get(Get),
    Set(Set),
    Info(Info),
    Client(Client),
    Module(Module),
    Command(Foo),
    Config(Config),
    Exists(Exists),
    DBsize(DBSize),
    Type(Type),
}

impl TryFrom<Frame> for Command {
    type Error = Error;

    fn try_from(frame: Frame) -> Result<Self, Self::Error> {
        let mut parser = CommandParser::try_from(frame)?;
        let command_name = parser.parse_command_name()?;

        match &command_name[..] {
            "get" => Get::try_from(&mut parser).map(Command::Get),
            "set" => Set::try_from(&mut parser).map(Command::Set),
            "exists" => Exists::try_from(&mut parser).map(Command::Exists),
            "dbsize" => DBSize::try_from(&mut parser).map(Command::DBsize),
            "info" => Info::try_from(&mut parser).map(Command::Info),
            "client" => Client::try_from(&mut parser).map(Command::Client),
            "module" => Module::try_from(&mut parser).map(Command::Module),
            "command" => Foo::try_from(&mut parser).map(Command::Command),
            "config" => Config::try_from(&mut parser).map(Command::Config),
            "type" => Type::try_from(&mut parser).map(Command::Type),
            name => return Err(format!("protocol error; unknown command {:?}", name).into()),
        }
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
            Frame::Bulk(bytes) => str::from_utf8(&bytes[..])
                .map(|s| s.to_lowercase())
                .map_err(CommandParserError::InvalidUTF8String),
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

    fn next_bytes(&mut self) -> Result<Bytes, CommandParserError> {
        let frame = self
            .parts
            .next()
            .ok_or_else(|| CommandParserError::EndOfStream)?;

        match frame {
            // Both `Simple` and `Bulk` representation may be strings. Strings are parsed to UTF-8.
            // While errors are stored as strings, they are considered separate types.
            Frame::Simple(s) => Ok(Bytes::from(s)),
            Frame::Bulk(bytes) => Ok(bytes),
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parse_get_command_with_simple_string() {
        let get_frame = Frame::Array(vec![
            Frame::Simple(String::from("GET")),
            Frame::Simple(String::from("foo")),
        ]);

        let get_command = Command::try_from(get_frame).unwrap();

        assert_eq!(
            get_command,
            Command::Get(Get {
                key: String::from("foo")
            })
        );
    }

    #[test]
    fn parse_get_command_with_bulk_string() {
        let get_frame = Frame::Array(vec![
            Frame::Simple(String::from("GET")),
            Frame::Bulk(Bytes::from("foo-from-bytes")),
        ]);

        let get_command = Command::try_from(get_frame).unwrap();

        assert_eq!(
            get_command,
            Command::Get(Get {
                key: String::from("foo-from-bytes")
            })
        );
    }

    #[test]
    fn parse_set_command() {
        let set_frame = Frame::Array(vec![
            Frame::Simple(String::from("SET")),
            Frame::Simple(String::from("foo")),
            Frame::Simple(String::from("baz")),
        ]);

        let set_command = Command::try_from(set_frame).unwrap();

        assert_eq!(
            set_command,
            Command::Set(Set {
                key: String::from("foo"),
                value: Bytes::from("baz")
            })
        );

        let set_frame = Frame::Array(vec![
            Frame::Simple(String::from("SET")),
            Frame::Bulk(Bytes::from("foo")),
            Frame::Bulk(Bytes::from("baz")),
        ]);

        let set_command = Command::try_from(set_frame).unwrap();

        assert_eq!(
            set_command,
            Command::Set(Set {
                key: String::from("foo"),
                value: Bytes::from("baz")
            })
        );

        let set_frame = Frame::Array(vec![
            Frame::Simple(String::from("SET")),
            Frame::Bulk(Bytes::from("foo")),
            Frame::Simple(String::from("baz")),
        ]);

        let set_command = Command::try_from(set_frame).unwrap();

        assert_eq!(
            set_command,
            Command::Set(Set {
                key: String::from("foo"),
                value: Bytes::from("baz")
            })
        );

        let set_frame = Frame::Array(vec![
            Frame::Simple(String::from("SET")),
            Frame::Simple(String::from("foo")),
            Frame::Bulk(Bytes::from("baz")),
        ]);

        let set_command = Command::try_from(set_frame).unwrap();

        assert_eq!(
            set_command,
            Command::Set(Set {
                key: String::from("foo"),
                value: Bytes::from("baz")
            })
        );
    }
}