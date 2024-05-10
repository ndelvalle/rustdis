//! Rustdis is a partial Redis server implementation intended purely for educational purposes.
//!
//! The primary goal of rustdis is to offer a straightforward and comprehensible implementation,
//! with no optimization techniques to ensure the code remains accessible and easy to understand.
//! As of now, rustdis focuses exclusively on implementing Redis' String data type and its
//! associated methods. You can find more about Redis strings here: [Redis
//! Strings](https://redis.io/docs/data-types/strings/).
//!
//! # Architecture
//!
//! * `server`: Redis server module. Provides a run function that initiates the server, enabling it
//! to begin handling incoming connections from Redis clients. It manages client requests, executes
//! Redis commands, and handles connection lifecycles.
//!
//! * `connection`: The Connection module manages a TCP connection for a Redis client. It separates
//! the TCP stream into readable and writable components to facilitate data consumption and
//! transmission. The server uses this connection module to read data from the TCP connection.
//! Additionally, the connection module uses the codec module to convert raw TCP bytes into
//! comprehensible data structures (Frames).

pub mod codec;
pub mod commands;
pub mod connection;
pub mod frame;
pub mod server;
pub mod store;
pub mod utils;

pub type Error = Box<dyn std::error::Error + Send + Sync>;
pub type Result<T> = std::result::Result<T, Error>;
