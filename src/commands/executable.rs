use crate::frame::Frame;
use crate::store::Store;
use crate::Error;

pub trait Executable {
    fn exec(self, store: Store) -> Result<Frame, Error>;
}
