use bytes::Bytes;

use crate::frame::Frame;
use crate::Error;

pub enum Command {
    Get { key: String },
    Set { key: String, value: Bytes },
}

impl TryFrom<Frame> for Command {
    type Error = Error;

    fn try_from(frame: Frame) -> Result<Self, Self::Error> {
        // Clients send commands to the Redis server as RESP arrays.
        let array = match frame {
            Frame::Array(array) => array,
            frame => return Err(format!("protocol error; expected array, got {:?}", frame).into()),
        };

        todo!()
    }
}
