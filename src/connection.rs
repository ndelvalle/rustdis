use futures::stream::StreamExt; // Use the correct StreamExt trait
use tokio::net::tcp::{OwnedReadHalf, OwnedWriteHalf};
use tokio::net::TcpStream;
use tokio_util::codec::FramedRead;

use crate::codec::FrameCodec;
use crate::frame::Frame;
use crate::Result;

pub struct Connection {
    pub writer: OwnedWriteHalf,
    reader: FramedRead<OwnedReadHalf, FrameCodec>,
}

impl Connection {
    pub fn new(stream: TcpStream) -> Connection {
        let (reader, writer) = stream.into_split();
        let reader = FramedRead::new(reader, FrameCodec);

        Connection { writer, reader }
    }

    pub async fn read_frame(&mut self) -> Result<Option<Frame>> {
        match self.reader.next().await {
            Some(Ok(frame)) => Ok(Some(frame)),
            Some(Err(e)) => Err(e),
            None => Ok(None),
        }
    }
}
