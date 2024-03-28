use std::sync::{Arc, Mutex};

use crate::frame::Frame;
use crate::store::Store;
use crate::Error;

pub trait Executable {
    fn exec(self, store: Arc<Mutex<Store>>) -> Result<Frame, Error>;
}
