use std::sync::{Arc, Mutex};
use tokio::io::AsyncWriteExt;
use tokio::net::{TcpListener, TcpStream};

use crate::commands::Command;
use crate::connection::Connection;
use crate::store::Store;
use crate::Error;
use crate::commands::executable::Executable;

pub async fn run() -> Result<(), Error> {
    let listener = TcpListener::bind("127.0.0.1:6379").await?;
    let store = Arc::new(Mutex::new(Store::new()));

    loop {
        let (socket, _) = listener.accept().await?;
        let store = store.clone();

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
        let cmd = Command::try_from(frame)?;
        let res = cmd.exec(store.clone())?;
        let res: Vec<u8> = res.into();

        con.writer.write_all(&res).await?;
    }

    Ok(())
}
