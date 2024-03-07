use std::sync::{Arc, Mutex};
use tokio::net::{TcpListener, TcpStream};

use crate::command::Command;
use crate::connection::Connection;
use crate::store::Store;
use crate::Error;

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

    let frame = con.read_frame().await?.unwrap();

    let cmd = Command::try_from(frame)?;

    match cmd {
        Command::Get(cmd) => {
            let store = store.lock().unwrap();
            if let Some(value) = store.get(&cmd.key) {
                println!("Found value: {:?}", value);
            } else {
                println!("Value not found");
            }
        }
        Command::Set(cmd) => {
            let mut store = store.lock().unwrap();
            store.set(cmd.key, cmd.value);
            println!("Set value");
        }
    }

    Ok(())
}
