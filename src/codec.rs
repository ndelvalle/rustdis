use bytes::{Buf, BytesMut};
use std::convert::TryInto;
use std::env;
use std::io::Cursor;
use tokio_util::codec::Decoder;

use crate::frame::{self, Frame};
use crate::Error;

pub struct FrameCodec;

impl FrameCodec {
    fn max_frame_size() -> usize {
        env::var("MAX_FRAME_SIZE")
            .map(|s| s.parse().expect("MAX_FRAME_SIZE must be a number"))
            .unwrap_or(512 * 1024 * 1024)
    }
}

impl Decoder for FrameCodec {
    type Item = Frame;
    type Error = Error;

    // TODO:
    // * Use src.reserve. This is a more efficient way to allocate space in the buffer.
    // * Read more here: https://docs.rs/tokio-util/latest/tokio_util/codec/index.html
    fn decode(&mut self, src: &mut BytesMut) -> Result<Option<Self::Item>, Self::Error> {
        // Check if the frame size exceeds a certain limit to prevent DoS attacks

        println!("src.len(): {}", src.len());

        if src.len() > FrameCodec::max_frame_size() {
            return Err("frame size exceeds limit".into());
        }

        print!("processing frame: ");

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
