use std::sync::{Arc, Mutex};
use tokio::io::AsyncWriteExt;
use tokio::net::{TcpListener, TcpStream};
use tracing::info;

use crate::commands::executable::Executable;
use crate::commands::Command;
use crate::connection::Connection;
use crate::store::Store;
use crate::Error;

pub async fn run() -> Result<(), Error> {
    let subscriber = tracing_subscriber::FmtSubscriber::new();
    tracing::subscriber::set_global_default(subscriber)?;

    let listener = TcpListener::bind("127.0.0.1:6379").await?;
    let store = Arc::new(Mutex::new(Store::new()));

    info!("Redis server listening on {}", listener.local_addr()?);

    loop {
        let (socket, _) = listener.accept().await?;
        let store = store.clone();
        info!("Accepted connection from {:?}", socket.peer_addr()?);

        tokio::spawn(async move {
            if let Err(e) = handle_connection(socket, store).await {
                println!("Error: {}", e);
            }
        });
    }
}

async fn handle_connection(stream: TcpStream, store: Arc<Mutex<Store>>) -> Result<(), Error> {
    let mut con = Connection::new(stream);

    while let Some(frame) = con.read_frame().await? {
        info!("Received frame: {:?}", frame);
        let cmd = Command::try_from(frame)?;
        let res = cmd.exec(store.clone())?;
        let res: Vec<u8> = res.into();

        con.writer.write_all(&res).await?;
    }

    Ok(())
}
