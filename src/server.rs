use std::net::SocketAddr;
use tokio::io::AsyncWriteExt;
use tokio::net::{TcpListener, TcpStream};
use tracing::{error, info, instrument};

use crate::commands::executable::Executable;
use crate::commands::Command;
use crate::connection::Connection;
use crate::store::Store;
use crate::Error;

const PORT: u16 = 6379;

pub async fn run() -> Result<(), Error> {
    let subscriber = tracing_subscriber::FmtSubscriber::new();
    tracing::subscriber::set_global_default(subscriber)?;

    let listener = TcpListener::bind(("127.0.0.1", PORT)).await?;
    let store = Store::new();

    info!("Redis server listening on {}", listener.local_addr()?);

    loop {
        let (socket, client_address) = listener.accept().await?;
        let store = store.clone();
        info!("Accepted connection from {:?}", client_address);

        tokio::spawn(async move {
            if let Err(e) = handle_connection(socket, client_address, store).await {
                error!(e);
            }
        });
    }
}

#[instrument(
    name = "connection",
    skip(stream, store),
    fields(connection_id, client_address)
)]
async fn handle_connection(
    stream: TcpStream,
    client_address: SocketAddr,
    store: Store,
) -> Result<(), Error> {
    let mut conn = Connection::new(stream, client_address);

    tracing::Span::current()
        .record("connection_id", conn.id.to_string())
        .record("client_address", client_address.to_string());

    while let Some(frame) = conn.read_frame().await? {
        info!("Received frame from client: {:?}", frame);
        let cmd = Command::try_from(frame)?;
        let res = cmd.exec(store.clone())?;
        info!("Sending response to client: {:?}", res);
        let res: Vec<u8> = res.into();

        conn.writer.write_all(&res).await?;
    }

    info!("Connection closed");
    Ok(())
}
