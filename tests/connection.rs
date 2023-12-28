use tokio::io::AsyncWriteExt;
use tokio::net::{TcpListener, TcpStream};

use rustdis::connection::Connection;
use rustdis::frame::Frame;
use rustdis::server;

#[tokio::test]
async fn connect_to_server() {
    start_server().await;

    // Establish a connection to the server.
    let mut stream = TcpStream::connect("127.0.0.1:6379").await.unwrap();

    // Assert that the stream is writable.
    stream
        .write_all(b"*2\r\n$3\r\nGET\r\n$5\r\nhello\r\n")
        .await
        .unwrap();
}

async fn start_server() {
    tokio::spawn(async move { server::run().await });
    tokio::time::sleep(tokio::time::Duration::from_millis(10)).await;
}

#[tokio::test]
async fn read_frame_with_simple_string() {
    async fn setup_test_server(data: &[u8]) -> Result<TcpStream, std::io::Error> {
        // Start a TCP listener on a random port.
        let listener = TcpListener::bind("127.0.0.1:0").await?;
        let local_addr = listener.local_addr().unwrap();
        let data = data.to_vec();

        // Spawn a task to accept the connection and send data.
        tokio::spawn(async move {
            if let Ok((mut socket, _)) = listener.accept().await {
                // Write test data and close the connection.
                let _ = socket.write_all(&data).await;
            }
        });

        // Connect to the listener
        TcpStream::connect(local_addr).await
    }

    let bytes = b"+OK\r\n";
    let stream = setup_test_server(bytes).await.unwrap();

    let mut connection = Connection::new(stream);
    let frame = connection.read_frame().await.unwrap();

    let actual = frame;
    let expected = Some(Frame::Simple("OK".to_string()));

    assert_eq!(actual, expected);
}
