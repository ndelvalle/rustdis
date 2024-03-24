// https://redis.io/docs/reference/protocol-spec

use std::fmt;

use bytes::Buf;
use bytes::Bytes;
use std::io::Cursor;
use std::string::FromUtf8Error;
use thiserror::Error as ThisError;

static CRLF: &[u8; 2] = b"\r\n";

#[derive(Debug, ThisError)]
pub enum Error {
    #[error("not enough data is available to parse an entire frame")]
    Incomplete,
    #[error("invalid frame data type: {0}")]
    InvalidDataType(u8),
    /// Invalid message encoding.
    #[error("{0}")]
    Other(crate::Error),
}

#[derive(Clone, Debug, PartialEq)]
pub enum Frame {
    Simple(String),
    Error(String),
    Integer(i64),
    Bulk(Bytes),
    Null,
    Array(Vec<Frame>),
}

// Protocol specification: https://redis.io/docs/reference/protocol-spec/
impl Frame {
    pub fn parse(src: &mut Cursor<&[u8]>) -> Result<Self, Error> {
        // The first byte in an RESP-serialized payload always identifies its type.
        // Subsequent bytes constitute the type's contents.
        let first_byte = get_byte(src)?;
        let data_type = DataType::try_from(first_byte)?;

        match data_type {
            DataType::SimpleString => {
                let bytes = get_frame_bytes(src)?.to_vec();
                let string = String::from_utf8(bytes)?;
                Ok(Frame::Simple(string))
            }
            DataType::SimpleError => {
                let bytes = get_frame_bytes(src)?.to_vec();
                let string = String::from_utf8(bytes)?;
                Ok(Frame::Error(string))
            }
            DataType::Integer => {
                let bytes = get_frame_bytes(src)?.to_vec();
                let string = String::from_utf8(bytes)?;
                let integer = string
                    .parse::<i64>()
                    .map_err(|e| -> Box<dyn std::error::Error + Send + Sync> { Box::new(e) })
                    .map_err(Error::Other)?;

                Ok(Frame::Integer(integer))
            }
            // $<length>\r\n<data>\r\n
            DataType::BulkString => {
                let length = get_frame_bytes(src)?;
                let length = String::from_utf8(length.to_vec())?;
                let length = length
                    .parse::<isize>()
                    .map_err(|e| -> Box<dyn std::error::Error + Send + Sync> { Box::new(e) })
                    .map_err(Error::Other)?;

                if length == -1 {
                    return Ok(Frame::Null);
                }

                let data = get_frame_bytes(src)?;
                let data = Bytes::from(data.to_vec());

                Ok(Frame::Bulk(data))
            }
            // !<length>\r\n<error>\r\n
            DataType::BulkError => {
                let length = get_frame_bytes(src)?;
                let length = String::from_utf8(length.to_vec())?;
                let length = length
                    .parse::<isize>()
                    .map_err(|e| -> Box<dyn std::error::Error + Send + Sync> { Box::new(e) })
                    .map_err(Error::Other)?;

                // NOTE: the protocol does not specify a way to represent a null bulk error
                if length == -1 {
                    return Ok(Frame::Null);
                }

                let msg = get_frame_bytes(src)?;
                let msg = String::from_utf8(msg.to_vec())?;

                Ok(Frame::Error(msg))
            }
            // *<number-of-elements>\r\n<element-1>...<element-n>
            DataType::Array => {
                let length = get_frame_bytes(src)?;
                let length = String::from_utf8(length.to_vec())?;
                let length = length
                    .parse::<isize>()
                    .map_err(|e| -> Box<dyn std::error::Error + Send + Sync> { Box::new(e) })
                    .map_err(Error::Other)?;

                if length == -1 {
                    return Ok(Frame::Null);
                }

                let mut frames = Vec::with_capacity(length as usize);
                for _ in 0..length {
                    let frame = Self::parse(src)?;
                    frames.push(frame);
                }

                Ok(Frame::Array(frames))
            }
            DataType::Null => {
                // Advance the cursor to the end of the frame.
                let _ = get_frame_bytes(src)?.to_vec();

                Ok(Frame::Null)
            }
            data_type => {
                println!("Unsupported data type: {:?}", data_type);
                todo!()
            }
        }
    }

    pub fn serialize(&self) -> Vec<u8> {
        match self {
            Frame::Simple(s) => {
                let mut bytes = Vec::with_capacity(1 + s.len() + CRLF.len());
                bytes.push(u8::from(DataType::SimpleString));
                bytes.extend_from_slice(s.as_bytes());
                bytes.extend_from_slice(CRLF);
                bytes
            }
            Frame::Error(s) => {
                let mut bytes = Vec::with_capacity(1 + s.len() + CRLF.len());
                bytes.push(u8::from(DataType::SimpleError));
                bytes.extend_from_slice(s.as_bytes());
                bytes.extend_from_slice(CRLF);
                bytes
            }
            Frame::Integer(i) => {
                let mut bytes = Vec::with_capacity(1 + i.to_string().len() + CRLF.len());
                bytes.push(u8::from(DataType::Integer));
                bytes.extend_from_slice(i.to_string().as_bytes());
                bytes.extend_from_slice(CRLF);
                bytes
            }
            Frame::Bulk(bytes) => {
                let length_str = bytes.len().to_string();
                let mut result = Vec::with_capacity(
                    1 + length_str.len() + CRLF.len() + bytes.len() + CRLF.len(),
                );
                result.push(u8::from(DataType::BulkString));
                result.extend_from_slice(length_str.as_bytes());
                result.extend_from_slice(CRLF);
                result.extend_from_slice(bytes);
                result.extend_from_slice(CRLF);
                result
            }
            Frame::Null => {
                let mut bytes = Vec::with_capacity(3);
                bytes.push(u8::from(DataType::Null));
                bytes.extend_from_slice(CRLF);
                bytes
            }
            Frame::Array(arr) => {
                let length_str = arr.len().to_string();
                let mut bytes = Vec::with_capacity(1 + length_str.len() + CRLF.len());
                bytes.push(u8::from(DataType::Array));
                bytes.extend_from_slice(length_str.as_bytes());
                bytes.extend_from_slice(CRLF);
                for frame in arr {
                    bytes.extend(frame.serialize());
                }
                bytes
            }
        }
    }
}

impl From<Frame> for Vec<u8> {
    fn from(frame: Frame) -> Self {
        frame.serialize()
    }
}

// TODO: Not sure about this display implementation, should we log the actual bytes? I think not,
// but maybe it will be useful for debugging.
impl fmt::Display for Frame {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Frame::Simple(s) => write!(f, "+{}", s),
            Frame::Error(s) => write!(f, "-{}", s),
            Frame::Integer(i) => write!(f, ":{}", i),
            Frame::Bulk(bytes) => write!(f, "${}", String::from_utf8_lossy(bytes)),
            Frame::Null => write!(f, "$-1"),
            Frame::Array(arr) => {
                write!(f, "*{}\r\n", arr.len())?;
                for frame in arr {
                    write!(f, "{}\r\n", frame)?;
                }
                Ok(())
            }
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
    SimpleString,   // '+'
    BulkString,     // '$'
    VerbatimString, // '='
    SimpleError,    // '-'
    BulkError,      // '!'
    Boolean,        // '#'
    Integer,        // ':'
    Double,         // ','
    BigNumber,      // '('
    Array,          // '*'
    Map,            // '%'
    Set,            // '~'
    Push,           // '>'
    // Due to historical reasons, RESP2 features two specially crafted values for representing null
    // values of bulk strings and arrays. This duality has always been a redundancy that added zero
    // semantical value to the protocol itself. The null type, introduced in RESP3, aims to fix
    // this wrong.
    Null, // '_'
}

impl TryFrom<u8> for DataType {
    type Error = Error;

    fn try_from(byte: u8) -> Result<Self, Self::Error> {
        match byte {
            b'+' => Ok(Self::SimpleString),
            b'-' => Ok(Self::SimpleError),
            b':' => Ok(Self::Integer),
            b'$' => Ok(Self::BulkString),
            b'!' => Ok(Self::BulkError),
            b'*' => Ok(Self::Array),
            b'_' => Ok(Self::Null),
            b'#' => Ok(Self::Boolean),
            b',' => Ok(Self::Double),
            b'(' => Ok(Self::BigNumber),
            b'=' => Ok(Self::VerbatimString),
            b'%' => Ok(Self::Map),
            b'~' => Ok(Self::Set),
            b'>' => Ok(Self::Push),
            _ => Err(Error::InvalidDataType(byte)),
        }
    }
}

impl From<DataType> for u8 {
    fn from(value: DataType) -> Self {
        match value {
            DataType::SimpleString => b'+',
            DataType::SimpleError => b'-',
            DataType::Integer => b':',
            DataType::BulkString => b'$',
            DataType::BulkError => b'!',
            DataType::Array => b'*',
            DataType::Null => b'_',
            DataType::Boolean => b'#',
            DataType::Double => b',',
            DataType::BigNumber => b'(',
            DataType::VerbatimString => b'=',
            DataType::Map => b'%',
            DataType::Set => b'~',
            DataType::Push => b'>',
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parse_simple_string_frame() {
        let data = b"+OK\r\n";
        let mut cursor = Cursor::new(&data[..]);

        let frame = Frame::parse(&mut cursor);

        assert!(matches!(frame, Ok(Frame::Simple(ref s)) if s == "OK"));
    }

    #[test]
    fn parse_simple_error_frame() {
        let data = b"-Error message\r\n";
        let mut cursor = Cursor::new(&data[..]);

        let frame = Frame::parse(&mut cursor);

        assert!(matches!(
            frame,
            Ok(Frame::Error(ref s)) if s == "Error message"
        ));
    }

    fn parse_integer_frame(data: &[u8], expected: i64) {
        let mut cursor = Cursor::new(&data[..]);

        let frame = Frame::parse(&mut cursor);

        assert!(matches!(frame, Ok(Frame::Integer(i)) if i == expected));
    }

    #[test]
    fn parse_integer_frame_positive() {
        parse_integer_frame(b":1000\r\n", 1000);
    }

    #[test]
    fn parse_integer_frame_negative() {
        parse_integer_frame(b":-1000\r\n", -1000);
    }

    #[test]
    fn parse_integer_frame_zero() {
        parse_integer_frame(b":0\r\n", 0);
    }

    #[test]
    fn parse_integer_frame_positive_singned() {
        parse_integer_frame(b":+1000\r\n", 1000);
    }

    #[test]
    fn parse_bulk_string_frame() {
        let data = b"$6\r\nfoobar\r\n";
        let mut cursor = Cursor::new(&data[..]);

        let frame = Frame::parse(&mut cursor);

        assert!(matches!(
            frame,
            Ok(Frame::Bulk(ref b)) if b == &Bytes::from("foobar")
        ));
    }

    #[test]
    fn parse_bulk_string_frame_empty() {
        let data = b"$0\r\n\r\n";
        let mut cursor = Cursor::new(&data[..]);

        let frame = Frame::parse(&mut cursor);

        assert!(matches!(
            frame,
            Ok(Frame::Bulk(ref b)) if b == &Bytes::from("")
        ));
    }

    #[test]
    fn parse_bulk_string_frame_null() {
        let data = b"$-1\r\n";
        let mut cursor = Cursor::new(&data[..]);

        let frame = Frame::parse(&mut cursor);

        assert!(matches!(frame, Ok(Frame::Null)));
    }

    #[test]
    fn parse_bulk_error_frame() {
        let data = b"!6\r\nfoobar\r\n";
        let mut cursor = Cursor::new(&data[..]);

        let frame = Frame::parse(&mut cursor);

        assert!(matches!(
            frame,
            Ok(Frame::Error(ref s)) if s == "foobar"
        ));
    }

    #[test]
    fn parse_bulk_error_frame_empty() {
        let data = b"!0\r\n\r\n";
        let mut cursor = Cursor::new(&data[..]);

        let frame = Frame::parse(&mut cursor);

        assert!(matches!(
            frame,
            Ok(Frame::Error(ref s)) if s == ""
        ));
    }

    #[test]
    fn parse_bulk_error_frame_null() {
        let data = b"!-1\r\n";
        let mut cursor = Cursor::new(&data[..]);

        let frame = Frame::parse(&mut cursor);

        assert!(matches!(frame, Ok(Frame::Null)));
    }

    #[test]
    fn parse_array_frame_empty() {
        let data = b"*0\r\n";
        let mut cursor = Cursor::new(&data[..]);

        let frame = Frame::parse(&mut cursor);

        assert!(matches!(frame, Ok(Frame::Array(ref a)) if a.is_empty()));
    }

    #[test]
    fn parse_array_frame() {
        let data = b"*2\r\n$5\r\nhello\r\n$5\r\nworld\r\n";
        let mut cursor = Cursor::new(&data[..]);

        let frame = Frame::parse(&mut cursor);

        assert!(matches!(
            frame,
            Ok(Frame::Array(ref a)) if a.len() == 2
        ));

        assert!(matches!(
            frame,
            Ok(Frame::Array(ref a)) if a[0] == Frame::Bulk(Bytes::from("hello"))
        ));

        assert!(matches!(
            frame,
            Ok(Frame::Array(ref a)) if a[1] == Frame::Bulk(Bytes::from("world"))
        ));
    }

    #[test]
    fn parse_array_frame_nested() {
        let data = b"*2\r\n*3\r\n:1\r\n:2\r\n:3\r\n*2\r\n+Hello\r\n-World\r\n";
        let mut cursor = Cursor::new(&data[..]);

        let frame = Frame::parse(&mut cursor);

        assert!(matches!(
            frame,
            Ok(Frame::Array(ref a)) if a.len() == 2
        ));

        assert!(matches!(
            frame,
            Ok(Frame::Array(ref a)) if a[0] == Frame::Array(vec![
                Frame::Integer(1),
                Frame::Integer(2),
                Frame::Integer(3)
            ])
        ));

        assert!(matches!(
            frame,
            Ok(Frame::Array(ref a)) if a[1] == Frame::Array(vec![
                Frame::Simple("Hello".to_string()),
                Frame::Error("World".to_string())
            ])
        ));
    }

    #[test]
    fn parse_array_frame_null() {
        let data = b"*-1\r\n";
        let mut cursor = Cursor::new(&data[..]);

        let frame = Frame::parse(&mut cursor);

        assert!(matches!(frame, Ok(Frame::Null)));
    }

    #[test]
    fn parse_array_frame_null_in_the_middle() {
        let data = b"*3\r\n$5\r\nhello\r\n$-1\r\n$5\r\nworld\r\n";
        let mut cursor = Cursor::new(&data[..]);

        let frame = Frame::parse(&mut cursor);

        assert!(matches!(
            frame,
            Ok(Frame::Array(ref a)) if a.len() == 3
        ));

        assert!(matches!(
            frame,
            Ok(Frame::Array(ref a)) if a[0] == Frame::Bulk(Bytes::from("hello"))
        ));

        assert!(matches!(frame, Ok(Frame::Array(ref a)) if a[1] == Frame::Null));

        assert!(matches!(
            frame,
            Ok(Frame::Array(ref a)) if a[2] == Frame::Bulk(Bytes::from("world"))
        ));
    }
}
