use std::sync::{Arc, Mutex};

use crate::commands::executable::Executable;
use crate::commands::{Command as RootCommand, CommandParser, CommandParserError};
use crate::frame::Frame;
use crate::store::Store;
use crate::Error;

#[derive(Debug, PartialEq)]
pub enum Command {
    /// Return an array with details about every Redis command.
    ///
    /// **NOTE**: only lists names of the implemented commands.
    ///
    /// Ref: <https://redis.io/docs/latest/commands/command/>
    Root(Root),
    /// Return documentary information about commands.
    ///
    /// **NOTE**: it is out of scope for this project to implement this command.
    ///
    /// Ref: <https://redis.io/docs/latest/commands/command-docs/>
    Docs(Docs),
}

impl Executable for Command {
    fn exec(self, store: Arc<Mutex<Store>>) -> Result<Frame, Error> {
        match self {
            Self::Root(root) => root.exec(store),
            Self::Docs(docs) => docs.exec(store),
        }
    }
}

impl TryFrom<&mut CommandParser> for Command {
    type Error = Error;

    fn try_from(parser: &mut CommandParser) -> Result<Self, Self::Error> {
        let sub = parser.next_string().map(|sub| sub.to_lowercase());

        match sub {
            Ok(sub) if sub == "docs" => Ok(Self::Docs(Docs)),
            Ok(sub) => Err(CommandParserError::UnknownCommand {
                command: format!("COMMAND {}", sub.to_uppercase()),
            }
            .into()),
            Err(CommandParserError::EndOfStream) => Ok(Self::Root(Root)),
            Err(err) => Err(err.into()),
        }
    }
}

#[derive(Debug, PartialEq)]
pub struct Root;

impl Executable for Root {
    fn exec(self, _store: Arc<Mutex<Store>>) -> Result<Frame, Error> {
        // TODO: list subcommands
        let cmds = RootCommand::all_variants()
            .iter()
            .map(|s| Frame::Simple(s.to_uppercase().to_string()))
            .collect();

        Ok(Frame::Array(cmds))
    }
}

#[derive(Debug, PartialEq)]
pub struct Docs;

impl Executable for Docs {
    fn exec(self, _store: Arc<Mutex<Store>>) -> Result<Frame, Error> {
        Ok(Frame::Simple("OK".to_string()))
    }
}
