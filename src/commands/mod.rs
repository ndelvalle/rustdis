pub mod append;
pub mod client;
pub mod command;
pub mod config;
pub mod dbsize;
pub mod decr;
pub mod decrby;
pub mod del;
pub mod executable;
pub mod exists;
pub mod get;
pub mod getdel;
pub mod getrange;
pub mod incr;
pub mod incrby;
pub mod info;
pub mod keys;
pub mod lcs;
pub mod memory;
pub mod mget;
pub mod module;
pub mod object;
pub mod ping;
pub mod scan;
pub mod select;
pub mod set;
pub mod setnx;
pub mod setrange;
pub mod strlen;
pub mod ttl;
pub mod type_;

use bytes::Bytes;
use std::sync::{Arc, Mutex};
use std::{str, vec};
use thiserror::Error as ThisError;

use crate::commands::executable::Executable;
use crate::frame::Frame;
use crate::store::Store;
use crate::Error;

use append::Append;
use client::Client;
use command::Command as Command_;
use config::Config;
use dbsize::DBSize;
use decr::Decr;
use decrby::DecrBy;
use del::Del;
use exists::Exists;
use get::Get;
use getdel::Getdel;
use getrange::Getrange;
use incr::Incr;
use incrby::IncrBy;
use info::Info;
use keys::Keys;
use lcs::Lcs;
use memory::Memory;
use mget::Mget;
use module::Module;
use object::Object;
use ping::Ping;
use scan::Scan;
use select::Select;
use set::Set;
use setnx::Setnx;
use setrange::Setrange;
use strlen::Strlen;
use ttl::Ttl;
use type_::Type;

#[derive(Debug, PartialEq)]
pub enum Command {
    Append(Append),
    DBsize(DBSize),
    Decr(Decr),
    DecrBy(DecrBy),
    Del(Del),
    Exists(Exists),
    Get(Get),
    Getdel(Getdel),
    Getrange(Getrange),
    Incr(Incr),
    IncrBy(IncrBy),
    Keys(Keys),
    Lcs(Lcs),
    Memory(Memory),
    Mget(Mget),
    Object(Object),
    Scan(Scan),
    Set(Set),
    Setnx(Setnx),
    Setrange(Setrange),
    Strlen(Strlen),
    Ttl(Ttl),
    Type(Type),

    Client(Client),
    Command(Command_),
    Config(Config),
    Info(Info),
    Module(Module),
    Ping(Ping),
    Select(Select),
}

impl Executable for Command {
    fn exec(self, store: Arc<Mutex<Store>>) -> Result<Frame, Error> {
        match self {
            Command::Append(cmd) => cmd.exec(store),
            Command::Client(cmd) => cmd.exec(store),
            Command::Command(cmd) => cmd.exec(store),
            Command::Config(cmd) => cmd.exec(store),
            Command::DBsize(cmd) => cmd.exec(store),
            Command::Decr(cmd) => cmd.exec(store),
            Command::DecrBy(cmd) => cmd.exec(store),
            Command::Del(cmd) => cmd.exec(store),
            Command::Exists(cmd) => cmd.exec(store),
            Command::Get(cmd) => cmd.exec(store),
            Command::Getdel(cmd) => cmd.exec(store),
            Command::Getrange(cmd) => cmd.exec(store),
            Command::Incr(cmd) => cmd.exec(store),
            Command::IncrBy(cmd) => cmd.exec(store),
            Command::Info(cmd) => cmd.exec(store),
            Command::Keys(cmd) => cmd.exec(store),
            Command::Lcs(cmd) => cmd.exec(store),
            Command::Memory(cmd) => cmd.exec(store),
            Command::Mget(cmd) => cmd.exec(store),
            Command::Module(cmd) => cmd.exec(store),
            Command::Object(cmd) => cmd.exec(store),
            Command::Ping(cmd) => cmd.exec(store),
            Command::Scan(cmd) => cmd.exec(store),
            Command::Select(cmd) => cmd.exec(store),
            Command::Set(cmd) => cmd.exec(store),
            Command::Setnx(cmd) => cmd.exec(store),
            Command::Setrange(cmd) => cmd.exec(store),
            Command::Strlen(cmd) => cmd.exec(store),
            Command::Ttl(cmd) => cmd.exec(store),
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
                return Err(CommandParserError::InvalidFrame {
                    expected: "array".to_string(),
                    actual: frame,
                }
                .into())
            }
        };

        let parser = &mut CommandParser {
            parts: frames.into_iter(),
        };

        let command_name = parser.parse_command_name()?;

        match &command_name[..] {
            "append" => Append::try_from(parser).map(Command::Append),
            "client" => Client::try_from(parser).map(Command::Client),
            "command" => Command_::try_from(parser).map(Command::Command),
            "config" => Config::try_from(parser).map(Command::Config),
            "dbsize" => DBSize::try_from(parser).map(Command::DBsize),
            "decr" => Decr::try_from(parser).map(Command::Decr),
            "decrby" => DecrBy::try_from(parser).map(Command::DecrBy),
            "del" => Del::try_from(parser).map(Command::Del),
            "exists" => Exists::try_from(parser).map(Command::Exists),
            "get" => Get::try_from(parser).map(Command::Get),
            "getdel" => Getdel::try_from(parser).map(Command::Getdel),
            "getrange" => Getrange::try_from(parser).map(Command::Getrange),
            "incr" => Incr::try_from(parser).map(Command::Incr),
            "incrby" => IncrBy::try_from(parser).map(Command::IncrBy),
            "info" => Info::try_from(parser).map(Command::Info),
            "keys" => Keys::try_from(parser).map(Command::Keys),
            "lcs" => Lcs::try_from(parser).map(Command::Lcs),
            "memory" => Memory::try_from(parser).map(Command::Memory),
            "mget" => Mget::try_from(parser).map(Command::Mget),
            "module" => Module::try_from(parser).map(Command::Module),
            "object" => Object::try_from(parser).map(Command::Object),
            "ping" => Ping::try_from(parser).map(Command::Ping),
            "scan" => Scan::try_from(parser).map(Command::Scan),
            "select" => Select::try_from(parser).map(Command::Select),
            "set" => Set::try_from(parser).map(Command::Set),
            "setnx" => Setnx::try_from(parser).map(Command::Setnx),
            "setrange" => Setrange::try_from(parser).map(Command::Setrange),
            "strlen" => Strlen::try_from(parser).map(Command::Strlen),
            "ttl" => Ttl::try_from(parser).map(Command::Ttl),
            "type" => Type::try_from(parser).map(Command::Type),
            _ => Err(CommandParserError::UnknownCommand {
                command: command_name,
            }
            .into()),
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

    fn next_integer(&mut self) -> Result<i64, CommandParserError> {
        let frame = self
            .parts
            .next()
            .ok_or_else(|| CommandParserError::EndOfStream)?;

        match frame {
            Frame::Integer(i) => Ok(i),
            Frame::Simple(string) => {
                string
                    .parse::<i64>()
                    .map_err(|_| CommandParserError::InvalidFrame {
                        expected: "parseable i64 frame".to_string(),
                        actual: Frame::Simple(string),
                    })
            }
            Frame::Bulk(bytes) => str::from_utf8(&bytes[..])
                .map_err(CommandParserError::InvalidUTF8String)?
                .parse::<i64>()
                .map_err(|_| CommandParserError::InvalidFrame {
                    expected: "parseable i64 frame".to_string(),
                    actual: Frame::Bulk(bytes),
                }),
            frame => Err(CommandParserError::InvalidFrame {
                expected: "integer".to_string(),
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
    #[error("protocol error; unknown command {command}")]
    UnknownCommand { command: String },
    #[error("protocol error; invalid command argument {command} {argument}")]
    InvalidCommandArgument { command: String, argument: String },
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
