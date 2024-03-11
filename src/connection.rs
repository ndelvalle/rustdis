use bytes::Buf;
use bytes::BytesMut;
use std::io::Cursor;
use tokio::io::AsyncReadExt;
use tokio::net::TcpStream;

use crate::frame::Frame;
use crate::Result;

pub struct Connection {
    pub stream: TcpStream,
    // Data is read from the socket into the read buffer. When a frame is parsed, the corresponding
    // data is removed from the buffer.
    pub buffer: BytesMut,
}

impl Connection {
    pub fn new(stream: TcpStream) -> Connection {
        Connection {
            stream,
            // Allocate the buffer with 4kb of capacity.
            buffer: BytesMut::with_capacity(4096),
        }
    }

    pub async fn read_frame(&mut self) -> Result<Option<Frame>> {
        loop {
            // Attempt to parse a frame from the buffered data. If enough data has been buffered,
            // the frame is returned.
            if Frame::can_parse(&self.buffer) {
                return self.parse_frame().map(Some);
            }

            // There is not enough buffered data to read a frame. Attempt to
            // read more data from the socket.
            //
            // On success, the number of bytes is returned. `0` indicates "end
            // of stream".
            if 0 == self.stream.read_buf(&mut self.buffer).await? {
                // The remote closed the connection. For this to be a clean
                // shutdown, there should be no data in the read buffer. If
                // there is, this means that the peer closed the socket while
                // sending a frame.
                if self.buffer.is_empty() {
                    return Ok(None);
                } else {
                    return Err("connection reset by peer".into());
                }
            }
        }
    }

    /// Parse a frame from the buffer and removes the frame's corresponding bytes from it.
    fn parse_frame(&mut self) -> crate::Result<Frame> {
        // Cursor is used to track the "current" location in the buffer. Cursor also implements
        // `Buf` from the `bytes` crate which provides a number of helpful utilities for working
        // with bytes.
        let mut buf = Cursor::new(&self.buffer[..]);
        let frame = Frame::parse(&mut buf)?;

        // Discard the parsed data from the read buffer.
        //
        // When `advance` is called on the read buffer, all of the data up to `len` is discarded.
        // The details of how this works is left to `BytesMut`. This is often done by moving an
        // internal cursor, but it may be done by reallocating and copying data.
        self.buffer.advance(buf.position() as usize);

        Ok(frame)
    }
}
