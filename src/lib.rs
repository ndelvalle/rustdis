pub type Error = Box<dyn std::error::Error + Send + Sync>;

pub mod connection;
pub mod frame;
pub mod server;
