use bytes::Bytes;
use redis::aio::MultiplexedConnection;
use redis::FromRedisValue;
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

async fn test_compare<Res>(f: impl FnOnce(&mut redis::Pipeline))
where
    Res: std::fmt::Debug + PartialEq + Send + FromRedisValue,
{
    let (mut our_connection, mut their_connection) = connect().await.unwrap();

    let mut pipeline = redis::pipe();

    f(&mut pipeline);

    let our_response: Res = pipeline
        .clone()
        .query_async(&mut our_connection)
        .await
        .unwrap();

    let their_response: Res = pipeline
        .clone()
        .query_async(&mut their_connection)
        .await
        .unwrap();

    assert_eq!(our_response, their_response);
}

#[tokio::test]
async fn test_set_and_get() {
    type Response = (Value, Value, Value, Value, Value, Value, Value);

    test_compare::<Response>(|p| {
        p.cmd("SET").arg("set_get_key_1").arg(1);
        p.cmd("SET").arg("set_get_key_2").arg("Argentina");
        p.cmd("SET")
            .arg("set_get_key_3")
            .arg(Bytes::from("Hello, World!").as_ref());

        p.cmd("GET").arg("set_get_key_1");
        p.cmd("GET").arg("set_get_key_2");
        p.cmd("GET").arg("set_get_key_3");
        p.cmd("GET").arg("set_get_nonexistentkey");
    })
    .await;
}

#[tokio::test]
async fn test_remove() {
    type Response = (
        Value,
        Value,
        Value,
        Value,
        Value,
        Value,
        Value,
        Value,
        Value,
        Value,
        Value,
        Value,
    );

    test_compare::<Response>(|p| {
        p.cmd("SET").arg("del_key_1").arg(1);
        p.cmd("SET").arg("del_key_2").arg("Argentina");
        p.cmd("SET").arg("del_key_3").arg("Thailand");
        p.cmd("SET").arg("del_key_4").arg("Netherlands");

        p.cmd("DEL").arg("del_key_1");
        p.cmd("DEL").arg("del_key_2");
        p.cmd("DEL").arg("del_key_3").arg("key_4");
        p.cmd("DEL").arg("del_nonexistentkey");

        p.cmd("GET").arg("del_key_1");
        p.cmd("GET").arg("del_key_2");
        p.cmd("GET").arg("del_key_3");
        p.cmd("GET").arg("del_key_4");
    })
    .await;
}

#[tokio::test]
async fn test_exists() {
    type Response = (Value, Value, Value, Value, Value);

    test_compare::<Response>(|p| {
        p.cmd("SET").arg("exists_key_1").arg(1);
        p.cmd("SET").arg("exists_key_2").arg("Argentina");

        p.cmd("EXISTS").arg("exists_key_1");
        p.cmd("EXISTS").arg("exists_key_2");
        p.cmd("EXISTS").arg("exists_key_3");
    })
    .await;
}
