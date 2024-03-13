pub mod codec;
pub mod commands;
pub mod connection;
pub mod frame;
pub mod server;
pub mod store;

pub type Error = Box<dyn std::error::Error + Send + Sync>;
pub type Result<T> = std::result::Result<T, Error>;
