// https://redis.io/docs/reference/protocol-spec

use bytes::Buf;
use bytes::Bytes;
use std::fmt;
use std::io::Cursor;
use std::string::FromUtf8Error;

static CRLF: &[u8; 2] = b"\r\n";

#[derive(Debug)]
pub enum Error {
    /// Not enough data is available to parse an entire frame.
    Incomplete,
    InvalidDataType(u8),
    /// Invalid message encoding.
    Other(crate::Error),
}

#[derive(Clone, Debug, PartialEq)]
pub enum Frame {
    Simple(String),
    Error(String),
    Integer(u64),
    Bulk(Bytes),
    Null,
    Array(Vec<Frame>),
}

impl Frame {
    // The \r\n (CRLF) is the protocol's terminator, which always separates its parts.
    pub fn can_parse(buffer: &[u8]) -> bool {
        buffer.windows(2).any(|window| window == CRLF)
    }

    pub fn parse(src: &mut Cursor<&[u8]>) -> Result<Self, Error> {
        // The first byte in an RESP-serialized payload always identifies its type. Subsequent
        // bytes constitute the type's contents.
        let first_byte = get_byte(src)?;
        let data_type = DataType::try_from(first_byte)?;

        match data_type {
            DataType::SimpleString => {
                let bytes = get_frame_bytes(src)?.to_vec();
                let string = String::from_utf8(bytes)?;
                Ok(Frame::Simple(string))
            }
            _ => todo!(),
        }
    }
}

fn get_frame_bytes<'a>(src: &mut Cursor<&'a [u8]>) -> Result<&'a [u8], Error> {
    let start = src.position() as usize;
    let end = src.get_ref().len();

    let frame_end_position = src.get_ref()[start..end]
        .windows(2)
        .enumerate()
        .position(|(_, window)| window == CRLF)
        .ok_or(Error::Incomplete)
        .map(|index| start + index)?;

    src.set_position((frame_end_position + CRLF.len()) as u64);

    return Ok(&src.get_ref()[start..frame_end_position]);
}

fn get_byte(src: &mut Cursor<&[u8]>) -> Result<u8, Error> {
    if !src.has_remaining() {
        return Err(Error::Incomplete);
    }
    Ok(src.get_u8())
}

#[derive(Debug)]
enum DataType {
    // Simple strings are encoded as a plus (+) character, followed by a string. The string mustn't
    // contain a CR (\r) or LF (\n) character and is terminated by CRLF (i.e., \r\n). Simple
    // strings transmit short, non-binary strings with minimal overhead.
    SimpleString,   // '+'
    SimpleError,    // '-'
    Integer,        // ':'
    BulkString,     // '$'
    Array,          // '*'
    Null,           // '_'
    Boolean,        // '#'
    Double,         // ','
    BigNumber,      // '('
    BulkError,      // '!'
    VerbatimString, // '='
    Map,            // '%'
    Set,            // '~'
    Push,           // '>'
}

impl TryFrom<u8> for DataType {
    type Error = Error;

    fn try_from(byte: u8) -> Result<Self, Self::Error> {
        match byte {
            b'+' => Ok(Self::SimpleString),
            b'-' => Ok(Self::SimpleError),
            b':' => Ok(Self::Integer),
            b'$' => Ok(Self::BulkString),
            b'*' => Ok(Self::Array),
            b'_' => Ok(Self::Null),
            b'#' => Ok(Self::Boolean),
            b',' => Ok(Self::Double),
            b'(' => Ok(Self::BigNumber),
            b'!' => Ok(Self::BulkError),
            b'=' => Ok(Self::VerbatimString),
            b'%' => Ok(Self::Map),
            b'~' => Ok(Self::Set),
            b'>' => Ok(Self::Push),
            _ => Err(Error::InvalidDataType(byte)),
        }
    }
}

impl From<FromUtf8Error> for Error {
    fn from(_src: FromUtf8Error) -> Error {
        "protocol error; invalid frame format".into()
    }
}

impl From<&str> for Error {
    fn from(src: &str) -> Error {
        src.to_string().into()
    }
}

impl From<String> for Error {
    fn from(src: String) -> Error {
        Error::Other(src.into())
    }
}

impl std::error::Error for Error {}

impl fmt::Display for Error {
    fn fmt(&self, fmt: &mut fmt::Formatter) -> fmt::Result {
        match self {
            Error::Incomplete => "stream ended early".fmt(fmt),
            Error::InvalidDataType(data_type) => {
                write!(fmt, "invalid data type: {}", data_type)
            }
            Error::Other(err) => err.fmt(fmt),
        }
    }
}
