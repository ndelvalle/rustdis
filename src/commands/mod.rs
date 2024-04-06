pub mod client;
pub mod command;
pub mod config;
pub mod dbsize;
pub mod del;
pub mod executable;
pub mod exists;
pub mod get;
pub mod info;
pub mod module;
pub mod set;
pub mod type_;

use bytes::Bytes;
use std::sync::{Arc, Mutex};
use std::{str, vec};
use thiserror::Error as ThisError;

use crate::commands::executable::Executable;
use crate::frame::Frame;
use crate::store::Store;
use crate::Error;

use client::Client;
use command::Command as Foo;
use config::Config;
use dbsize::DBSize;
use del::Del;
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
    Del(Del),
    Info(Info),
    Client(Client),
    Module(Module),
    Command(Foo),
    Config(Config),
    Exists(Exists),
    DBsize(DBSize),
    Type(Type),
}

impl Executable for Command {
    fn exec(self, store: Arc<Mutex<Store>>) -> Result<Frame, Error> {
        match self {
            Command::Get(cmd) => cmd.exec(store),
            Command::Set(cmd) => cmd.exec(store),
            Command::Del(cmd) => cmd.exec(store),
            Command::Info(cmd) => cmd.exec(store),
            Command::Client(cmd) => cmd.exec(store),
            Command::Module(cmd) => cmd.exec(store),
            Command::Command(cmd) => cmd.exec(store),
            Command::Config(cmd) => cmd.exec(store),
            Command::Exists(cmd) => cmd.exec(store),
            Command::DBsize(cmd) => cmd.exec(store),
            Command::Type(cmd) => cmd.exec(store),
        }
    }
}

impl TryFrom<Frame> for Command {
    type Error = Error;

    fn try_from(frame: Frame) -> Result<Self, Self::Error> {
        // Clients send commands to the Redis server as RESP arrays.
        let frames = match frame {
            Frame::Array(array) => array,
            frame => {
                return Err(Box::new(CommandParserError::InvalidFrame {
                    expected: "array".to_string(),
                    actual: frame,
                }))
            }
        };

        let parser = &mut CommandParser {
            parts: frames.into_iter(),
        };

        let command_name = parser.parse_command_name()?;

        match &command_name[..] {
            "get" => Get::try_from(parser).map(Command::Get),
            "set" => Set::try_from(parser).map(Command::Set),
            "del" => Del::try_from(parser).map(Command::Del),
            "exists" => Exists::try_from(parser).map(Command::Exists),
            "dbsize" => DBSize::try_from(parser).map(Command::DBsize),
            "info" => Info::try_from(parser).map(Command::Info),
            "client" => Client::try_from(parser).map(Command::Client),
            "module" => Module::try_from(parser).map(Command::Module),
            "command" => Foo::try_from(parser).map(Command::Command),
            "config" => Config::try_from(parser).map(Command::Config),
            "type" => Type::try_from(parser).map(Command::Type),
            name => Err(format!("protocol error; unknown command {:?}", name).into()),
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

#[derive(Debug, ThisError, PartialEq)]
pub(crate) enum CommandParserError {
    #[error("protocol error; invalid frame, expected {expected}, got {actual}")]
    InvalidFrame { expected: String, actual: Frame },
    #[error("protocol error; invalid UTF-8 string")]
    InvalidUTF8String(#[from] str::Utf8Error),
    #[error("protocol error; attempting to extract a value failed due to the frame being fully consumed")]
    EndOfStream,
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
