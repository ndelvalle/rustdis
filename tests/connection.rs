use rustdis::server;

use tokio::io::AsyncWriteExt;
use tokio::net::TcpStream;

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
