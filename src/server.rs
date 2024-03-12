use bytes::Bytes;
use std::sync::{Arc, Mutex};
use tokio::io::AsyncWriteExt;
use tokio::net::{TcpListener, TcpStream};

use crate::commands::Command;
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

    while let Some(frame) = con.read_frame().await? {
        let cmd = Command::try_from(frame)?;

        match cmd {
            Command::Get(cmd) => {
                get(store.clone(), cmd.key)?;
                println!("GET");
                con.stream.write_all(b"+OK\r\n").await?;
            }
            Command::Set(cmd) => {
                println!("SET");
                set(store.clone(), cmd.key, cmd.value)?;
                con.stream.write_all(b"+OK\r\n").await?;
            }
            Command::Exists(_cmd) => {
                println!("Exists");
                con.stream.write_all(b":0\r\n").await?;
            }
            Command::DBsize(_cmd) => {
                println!("DBsize");
                con.stream.write_all(b":1\r\n").await?;
            }
            Command::Type(_cmd) => {
                println!("Type");
                con.stream.write_all(b"+string\r\n").await?;
            }
            Command::Info(_cmd) => {
                println!("Info command");
                con.stream.write_all(b"+OK\r\n").await?;
            }
            Command::Client(_cmd) => {
                println!("Client command");
                con.stream.write_all(b"+OK\r\n").await?;
            }
            Command::Module(_cmd) => {
                println!("Module command");
                con.stream.write_all(b"+OK\r\n").await?;
            }
            Command::Command(_cmd) => {
                println!("Command command");
                con.stream.write_all(b"+OK\r\n").await?;
            }
            Command::Config(_cmd) => {
                println!("Config command");
                con.stream.write_all(b"+OK\r\n").await?;
            }
        }
    }

    Ok(())
}

fn get(store: Arc<Mutex<Store>>, key: String) -> Result<(), Error> {
    let store = store.lock().unwrap();

    if let Some(value) = store.get(&key) {
        println!("Found value: {:?}", value);
    } else {
        println!("Value not found");
    }
    Ok(())
}

fn set(store: Arc<Mutex<Store>>, key: String, value: Bytes) -> Result<(), Error> {
    let mut store = store.lock().unwrap();
    store.set(key, value);
    Ok(())
}
