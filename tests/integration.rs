use redis::Connection;
use redis::RedisError;
use redis::Value;
use rustdis::server::run;

use tokio::time::{sleep, Duration};

async fn connect() -> Result<(Connection, Connection), RedisError> {
    tokio::spawn(async { run(6378).await });
    sleep(Duration::from_millis(100)).await;

    let our_client = redis::Client::open("redis://127.0.0.1:6378/")?;
    let our_connection = our_client.get_connection()?;

    let thir_client = redis::Client::open("redis://127.0.0.1:6379/")?;
    let their_connection = thir_client.get_connection()?;

    Ok((our_connection, their_connection))
}

#[tokio::test(flavor = "multi_thread")]
async fn test_set_and_get() {
    let (mut our_connection, mut their_connection) = connect().await.unwrap();

    let mut pipeline = redis::pipe();

    pipeline.cmd("SET").arg("key_1").arg(1);
    pipeline.cmd("SET").arg("key_2").arg("Argentina");
    pipeline.cmd("GET").arg("key_1").cmd("GET");
    pipeline.arg("key_2").cmd("GET").arg("nonexistentkey");

    let our_response: (Value, Value, Value, Value, Value) =
        pipeline.clone().query(&mut our_connection).unwrap();

    let their_response: (Value, Value, Value, Value, Value) =
        pipeline.clone().query(&mut their_connection).unwrap();

    assert_eq!(our_response, their_response);
}
