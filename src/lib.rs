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
//!
//! * `codec`: This module is responsible for decoding raw TCP byte streams into `Frame` data
//! structures. This is an essential component for translating incoming client requests into
//! meaningful Redis commands.
//!
//! * `frame`: This module defines the `Frame` enum, representing different types of Redis protocol
//! messages, and provides parsing and serialization functionalities. It adheres to the RESP (Redis
//! Serialization Protocol) specifications.
//!
//! * `store`: This module provides a simple key-value store for managing Redis string data types.
//! It supports basic operations such as setting, getting, removing, and incrementing values
//! associated with keys.
//!
//!                         +--------------------------------------+
//!                         |             Redis Client             |
//!                         +-------------------+------------------+
//!                                             |
//!                                             | Request (e.g., SET key value)
//!                                             v
//!                         +-------------------+------------------+
//!                         |                  Server              |
//!                         |    (module: server, function: run)   |
//!                         +-------------------+------------------+
//!                                             |
//!                                             | Accept Connection
//!                                             v
//!                         +-------------------+------------------+
//!                         |                Connection            |
//!                         |   (module: connection, manages TCP   |
//!                         |        connections and streams)      |
//!                         +-------------------+------------------+
//!                                             |
//!                                             | Read Data from TCP Stream
//!                                             v
//!                         +-------------------+------------------+
//!                         |                   Codec              |
//!                         |  (module: codec, function: decode)   |
//!                         +-------------------+------------------+
//!                                             |
//!                                             | Decode Request
//!                                             v
//!                         +-------------------+------------------+
//!                         |                   Frame              |
//!                         |  (module: frame, function: parse)    |
//!                         +-------------------+------------------+
//!                                             |
//!                                             | Parse Command and Data
//!                                             v
//!                         +-------------------+------------------+
//!                         |                   Store              |
//!                         |  (module: store, manages key-value   |
//!                         |          data storage)               |
//!                         +-------------------+------------------+
//!                                             |
//!                                             | Execute Command (e.g., set, get, incr_by)
//!                                             v
//!                         +-------------------+------------------+
//!                         |                   Frame              |
//!                         |  (module: frame, function: serialize)|
//!                         +-------------------+------------------+
//!                                             |
//!                                             | Encode Response
//!                                             v
//!                         +-------------------+------------------+
//!                         |                   Codec              |
//!                         |  (module: codec, function: encode)   |
//!                         +-------------------+------------------+
//!                                             |
//!                                             | Write Data to TCP Stream
//!                                             v
//!                         +-------------------+------------------+
//!                         |                Connection            |
//!                         |   (module: connection, manages TCP   |
//!                         |        connections and streams)      |
//!                         +-------------------+------------------+
//!                                             |
//!                                             | Send Response
//!                                             v
//!                         +-------------------+------------------+
//!                         |             Redis Client             |
//!                         +--------------------------------------+
//!

pub mod codec;
pub mod commands;
pub mod connection;
pub mod frame;
pub mod server;
pub mod store;
pub mod utils;

pub type Error = Box<dyn std::error::Error + Send + Sync>;
pub type Result<T> = std::result::Result<T, Error>;
