use std::collections::HashMap;
use std::sync::{Arc, Mutex};
use tokio::net::{TcpListener, TcpStream};

use crate::Error;

type Store = Arc<Mutex<HashMap<String, String>>>;

pub async fn run() -> Result<(), Error> {
    let listener = TcpListener::bind("127.0.0.1:6379").await?;
    let store = Arc::new(Mutex::new(HashMap::new()));

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

async fn handle_connection(
    mut stream: TcpStream,
    store: Store,
) -> Result<(), Box<dyn std::error::Error>> {
    todo!()
}
