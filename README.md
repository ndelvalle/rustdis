# rustdis

Welcome to rustdis, a partial Redis server implementation written in Rust.

This project came to life out of pure curiosity, and because we wanted to learn
more about Rust and Redis. So doing this project seemed like a good idea. The
primary goal of rustdis is to offer a straightforward and comprehensible
implementation, with no optimization techniques to ensure the code remains
accessible and easy to understand. As of now, rustdis focuses exclusively on
implementing Redis' String data type and its associated methods. You can find
more about Redis strings here: [Redis
Strings](https://redis.io/docs/data-types/strings/).

This server is not production-ready; it is intended purely for educational
purposes.

### Run
```shell
make run
```
### Test
```shell
make test
```
### Architecture

                         +--------------------------------------+
                         |             Redis Client             |
                         +-------------------+------------------+
                                             |
                                             | Request (e.g., SET key value)
                                             v
                         +-------------------+------------------+
                         |                  Server              |
                         |    (module: server, function: run)   |
                         +-------------------+------------------+
                                             |
                                             | Accept Connection
                                             v
                         +-------------------+------------------+
                         |                Connection            |
                         |   (module: connection, manages TCP   |
                         |        connections and streams)      |
                         +-------------------+------------------+
                                             |
                                             | Read Data from TCP Stream
                                             v
                         +-------------------+------------------+
                         |                   Codec              |
                         |  (module: codec, function: decode)   |
                         +-------------------+------------------+
                                             |
                                             | Decode Request
                                             v
                         +-------------------+------------------+
                         |                   Frame              |
                         |  (module: frame, function: parse)    |
                         +-------------------+------------------+
                                             |
                                             | Parse Command and Data
                                             v
                         +-------------------+------------------+
                         |                   Store              |
                         |  (module: store, manages key-value   |
                         |          data storage)               |
                         +-------------------+------------------+
                                             |
                                             | Execute Command (e.g., set, get, incr_by)
                                             v
                         +-------------------+------------------+
                         |                   Frame              |
                         |  (module: frame, function: serialize)|
                         +-------------------+------------------+
                                             |
                                             | Encode Response
                                             v
                         +-------------------+------------------+
                         |                   Codec              |
                         |  (module: codec, function: encode)   |
                         +-------------------+------------------+
                                             |
                                             | Write Data to TCP Stream
                                             v
                         +-------------------+------------------+
                         |                Connection            |
                         |   (module: connection, manages TCP   |
                         |        connections and streams)      |
                         +-------------------+------------------+
                                             |
                                             | Send Response
                                             v
                         +-------------------+------------------+
                         |             Redis Client             |
                         +--------------------------------------+
