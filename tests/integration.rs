use bytes::Bytes;
use redis::aio::MultiplexedConnection;
use redis::AsyncCommands;
use redis::RedisError;
use redis::Value;
use rustdis::server::run;

use tokio::time::{sleep, Duration};

async fn connect() -> Result<(MultiplexedConnection, MultiplexedConnection), RedisError> {
    tokio::spawn(async { run(6378).await });
    sleep(Duration::from_millis(100)).await;

    let our_client = redis::Client::open("redis://127.0.0.1:6378/")?;
    let our_connection = our_client.get_multiplexed_async_connection().await?;

    let thir_client = redis::Client::open("redis://127.0.0.1:6379/")?;
    let their_connection = thir_client.get_multiplexed_async_connection().await?;

    Ok((our_connection, their_connection))
}

#[tokio::test]
async fn test_set_and_get() {
    let (mut our_connection, mut their_connection) = connect().await.unwrap();

    let mut pipeline = redis::pipe();

    pipeline.cmd("SET").arg("key_1").arg(1);
    pipeline.cmd("SET").arg("key_2").arg("Argentina");
    pipeline
        .cmd("SET")
        .arg("key_3")
        .arg(Bytes::from("Hello, World!").as_ref());

    pipeline.cmd("GET").arg("key_1");
    pipeline.cmd("GET").arg("key_2");
    pipeline.cmd("GET").arg("key_3");
    pipeline.cmd("GET").arg("nonexistentkey");

    type Response = (Value, Value, Value, Value, Value, Value, Value);

    let our_response: Response = pipeline
        .clone()
        .query_async(&mut our_connection)
        .await
        .unwrap();

    let their_response: Response = pipeline
        .clone()
        .query_async(&mut their_connection)
        .await
        .unwrap();

    assert_eq!(our_response, their_response);
}
