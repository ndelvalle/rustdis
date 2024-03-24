use bytes::{Buf, BytesMut};
use std::convert::TryInto;
use std::io::Cursor;
use tokio_util::codec::Decoder;

use crate::frame::{self, Frame};
use crate::Error;

pub struct FrameCodec;

impl Decoder for FrameCodec {
    type Item = Frame;
    type Error = Error;

    // TODO:
    // * Use src.reserve. This is a more efficient way to allocate space in the buffer.
    // * Return an error if the frame is too large. This is a simple way to prevent a malicious
    // client from sending a large frame and causing the server to run out of memory.
    // * Read more here: https://docs.rs/tokio-util/latest/tokio_util/codec/index.html
    fn decode(&mut self, src: &mut BytesMut) -> Result<Option<Self::Item>, Self::Error> {
        let mut cursor = Cursor::new(&src[..]);
        let frame = match Frame::parse(&mut cursor) {
            Ok(frame) => frame,
            Err(frame::Error::Incomplete) => return Ok(None), // Not enough data to parse a frame.
            Err(err) => return Err(err.into()),
        };

        let position: usize = cursor
            .position()
            .try_into()
            .expect("Cursor position is too large");

        // Remove the parsed frame from the buffer.
        src.advance(position);

        Ok(Some(frame))
    }
}
