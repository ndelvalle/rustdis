use futures::stream::StreamExt; // Use the correct StreamExt trait
use tokio::net::tcp::{OwnedReadHalf, OwnedWriteHalf};
use tokio::net::TcpStream;
use tokio_util::codec::FramedRead;
use uuid::Uuid;

use std::net::SocketAddr;

use crate::codec::FrameCodec;
use crate::frame::Frame;
use crate::Result;

pub struct Connection {
    pub id: Uuid,
    pub client_address: SocketAddr,
    pub writer: OwnedWriteHalf,
    reader: FramedRead<OwnedReadHalf, FrameCodec>,
}

impl Connection {
    pub fn new(stream: TcpStream, client_address: SocketAddr) -> Connection {
        let (reader, writer) = stream.into_split();
        let reader = FramedRead::new(reader, FrameCodec);
        let id = Uuid::new_v4();

        Connection {
            id,
            writer,
            reader,
            client_address,
        }
    }

    pub async fn read_frame(&mut self) -> Result<Option<Frame>> {
        match self.reader.next().await {
            Some(Ok(frame)) => Ok(Some(frame)),
            Some(Err(e)) => Err(e),
            None => Ok(None),
        }
    }
}
