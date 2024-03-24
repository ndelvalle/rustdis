use bytes::Bytes;
use tokio::io::AsyncWriteExt;
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::mpsc::{self, UnboundedSender};

use rustdis::connection::Connection;
use rustdis::frame::Frame;

async fn create_tcp_connection() -> Result<(UnboundedSender<Vec<u8>>, TcpStream), std::io::Error> {
    let listener = TcpListener::bind("127.0.0.1:0").await?;
    let local_addr = listener.local_addr()?;

    let (tx, mut rx) = mpsc::unbounded_channel::<Vec<u8>>();

    tokio::spawn(async move {
        if let Ok((mut socket, _)) = listener.accept().await {
            while let Some(data) = rx.recv().await {
                // Write the received channel data to the socket.
                if socket.write_all(&data).await.is_err() {
                    // TODO: Handle error (e.g., connection closed) and possibly exit the loop.
                    break;
                }
            }
        }
    });

    // Connect to the server as a client to complete the setup.
    let stream = TcpStream::connect(local_addr).await?;

    Ok((tx, stream))
}

#[tokio::test]
async fn test_parse_single_string() {
    let (tcp_stream_tx, tcp_stream) = create_tcp_connection().await.unwrap();
    let mut connection = Connection::new(tcp_stream);

    let bytes = b"+OK\r\n";

    tcp_stream_tx.send(bytes.to_vec()).unwrap();

    let actual = connection.read_frame().await.unwrap();
    let expected = Some(Frame::Simple("OK".to_string()));

    assert_eq!(actual, expected);
}

#[tokio::test]
async fn test_parse_bulk_string() {
    let (tcp_stream_tx, tcp_stream) = create_tcp_connection().await.unwrap();
    let mut connection = Connection::new(tcp_stream);

    let bytes = b"$5\r\nhello\r\n";

    tcp_stream_tx.send(bytes.to_vec()).unwrap();

    let actual = connection.read_frame().await.unwrap();
    let expected = Some(Frame::Bulk(Bytes::from("hello")));

    assert_eq!(actual, expected);
}

#[tokio::test]
async fn test_parse_array() {
    let (tcp_stream_tx, tcp_stream) = create_tcp_connection().await.unwrap();
    let mut connection = Connection::new(tcp_stream);

    let bytes = b"*3\r\n$3\r\nSET\r\n$5\r\nmykey\r\n$7\r\nmyvalue\r\n";

    tcp_stream_tx.send(bytes.to_vec()).unwrap();

    let actual = connection.read_frame().await.unwrap();
    let expected = Some(Frame::Array(vec![
        Frame::Bulk(Bytes::from("SET")),
        Frame::Bulk(Bytes::from("mykey")),
        Frame::Bulk(Bytes::from("myvalue")),
    ]));

    assert_eq!(actual, expected);
}

#[tokio::test]
async fn test_parse_simple_error() {
    let (tcp_stream_tx, tcp_stream) = create_tcp_connection().await.unwrap();
    let mut connection = Connection::new(tcp_stream);

    let bytes = b"-Error message\r\n";

    tcp_stream_tx.send(bytes.to_vec()).unwrap();

    let actual = connection.read_frame().await.unwrap();
    let expected = Some(Frame::Error(String::from("Error message")));

    assert_eq!(actual, expected);
}

#[tokio::test]
async fn test_parse_integer() {
    let (tcp_stream_tx, tcp_stream) = create_tcp_connection().await.unwrap();
    let mut connection = Connection::new(tcp_stream);

    let bytes = b":1000\r\n";

    tcp_stream_tx.send(bytes.to_vec()).unwrap();

    let actual = connection.read_frame().await.unwrap();
    let expected = Some(Frame::Integer(1000));

    assert_eq!(actual, expected);
}

#[tokio::test]
async fn test_parse_null_bulk_string() {
    let (tcp_stream_tx, tcp_stream) = create_tcp_connection().await.unwrap();
    let mut connection = Connection::new(tcp_stream);

    let bytes = b"$-1\r\n";

    tcp_stream_tx.send(bytes.to_vec()).unwrap();

    let actual = connection.read_frame().await.unwrap();
    let expected = Some(Frame::Null);

    assert_eq!(actual, expected);
}

#[tokio::test]
async fn test_parse_multiple_commands_sequentially() {
    let (tcp_stream_tx, tcp_stream) = create_tcp_connection().await.unwrap();
    let mut connection = Connection::new(tcp_stream);

    let simple_string = b"+OK\r\n";
    let bulk_string = b"$5\r\nhello\r\n";
    let array_1 = b"*3\r\n$3\r\nSET\r\n$5\r\nmykey_1\r\n$7\r\nmyvalue_1\r\n";
    let array_2 = b"*3\r\n$3\r\nSET\r\n$5\r\nmykey_2\r\n$7\r\nmyvalue_2\r\n";
    let simple_error = b"-Error message\r\n";
    let integer = b":1000\r\n";

    tcp_stream_tx.send(simple_string.to_vec()).unwrap();
    tcp_stream_tx.send(bulk_string.to_vec()).unwrap();
    tcp_stream_tx.send(array_1.to_vec()).unwrap();
    tcp_stream_tx.send(array_2.to_vec()).unwrap();
    tcp_stream_tx.send(simple_error.to_vec()).unwrap();
    tcp_stream_tx.send(integer.to_vec()).unwrap();

    // Simple string.
    let actual = connection.read_frame().await.unwrap();
    let expected = Some(Frame::Simple("OK".to_string()));
    assert_eq!(actual, expected);

    // Bulk string.
    let actual = connection.read_frame().await.unwrap();
    let expected = Some(Frame::Bulk(Bytes::from("hello")));
    assert_eq!(actual, expected);

    // Array 1.
    let actual = connection.read_frame().await.unwrap();
    let expected = Some(Frame::Array(vec![
        Frame::Bulk(Bytes::from("SET")),
        Frame::Bulk(Bytes::from("mykey_1")),
        Frame::Bulk(Bytes::from("myvalue_1")),
    ]));
    assert_eq!(actual, expected);

    // Array 2.
    let actual = connection.read_frame().await.unwrap();
    let expected = Some(Frame::Array(vec![
        Frame::Bulk(Bytes::from("SET")),
        Frame::Bulk(Bytes::from("mykey_2")),
        Frame::Bulk(Bytes::from("myvalue_2")),
    ]));
    assert_eq!(actual, expected);

    // Simple error.
    let actual = connection.read_frame().await.unwrap();
    let expected = Some(Frame::Error(String::from("Error message")));
    assert_eq!(actual, expected);

    // Integer.
    let actual = connection.read_frame().await.unwrap();
    let expected = Some(Frame::Integer(1000));
    assert_eq!(actual, expected);
}

#[tokio::test]
async fn test_parse_incomplete_frame() {
    let (tcp_stream_tx, tcp_stream) = create_tcp_connection().await.unwrap();
    let mut connection = Connection::new(tcp_stream);

    // Command split into three parts to simulate partial/incomplete data sending.
    // "*3\r\n$3\r\nSET\r\n$5\r\nmykey\r\n$7\r\nmyvalue\r\n";
    let part1 = b"*3\r\n$3\r\nSE";
    let part2 = b"T\r\n$5\r\nmyke";
    let part3 = b"y\r\n$7\r\nmyvalue\r\n";

    tokio::spawn(async move {
        let parts = vec![part1.to_vec(), part2.to_vec(), part3.to_vec()];
        for part in parts {
            tcp_stream_tx.send(part.to_vec()).unwrap();
            // Simulate a delay in sending/receiving the data.
            tokio::time::sleep(tokio::time::Duration::from_millis(100)).await;
        }
    });

    let actual = connection.read_frame().await.unwrap();
    let expected = Some(Frame::Array(vec![
        Frame::Bulk(Bytes::from("SET")),
        Frame::Bulk(Bytes::from("mykey")),
        Frame::Bulk(Bytes::from("myvalue")),
    ]));
    assert_eq!(actual, expected);
}
