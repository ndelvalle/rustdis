use bytes::Bytes;
use redis::aio::MultiplexedConnection;
use redis::FromRedisValue;
use redis::RedisError;
use redis::Value;
use rustdis::server::run;
use serial_test::serial;
use tokio::time::{sleep, Duration};

async fn connect() -> Result<(MultiplexedConnection, MultiplexedConnection), RedisError> {
    tokio::spawn(run(6378));
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

    // Since we use the same Redis instance for all tests, we flush it to start fresh.
    // NOTE: our implementation doesn't yet persist data between runs.
    let _: Value = redis::pipe()
        .cmd("FLUSHDB")
        .query_async(&mut their_connection)
        .await
        .unwrap();

    let our_response: Result<Res, _> = pipeline.clone().query_async(&mut our_connection).await;
    let their_response: Result<Res, _> = pipeline.clone().query_async(&mut their_connection).await;

    assert!(
        our_response.is_ok(),
        "Not Ok, use `test_compare_err` instead if expecting an error"
    );
    assert!(
        their_response.is_ok(),
        "Not Ok, use `test_compare_err` instead if expecting an error"
    );
    assert_eq!(our_response, their_response);
}

/// When the server responds with an error, the client parses it into `Err(RedisError)`,
/// ignoring all the other values from previous commands in the pipeline.
///
/// Thus, when testing errors, we want to run the least number of commands in the pipeline,
/// because their outputs will be ignored.
async fn test_compare_err(f: impl FnOnce(&mut redis::Pipeline)) {
    let (mut our_connection, mut their_connection) = connect().await.unwrap();

    let mut pipeline = redis::pipe();
    f(&mut pipeline);

    type Res = Result<(), RedisError>;

    let our_response: Res = pipeline.clone().query_async(&mut our_connection).await;

    // Since we use the same Redis instance for all tests, we flush it to start fresh.
    // NOTE: our implementation doesn't yet persist data between runs.
    let _: Value = redis::pipe()
        .cmd("FLUSHDB")
        .query_async(&mut their_connection)
        .await
        .unwrap();

    let their_response: Res = pipeline.clone().query_async(&mut their_connection).await;

    assert!(
        our_response.is_err(),
        "Not Err, use `test_compare` instead if expecting a value"
    );
    assert!(
        their_response.is_err(),
        "Not Err, use `test_compare` instead if expecting a value"
    );

    // The `redis` crate does some parsing and wrapping of the error message. See:
    // https://github.com/redis-rs/redis-rs/blob/e6a59325ca963c09675fafac15fbf10ddf5f4cd4/redis/src/types.rs#L743-L766
    //
    // We only care about the error message sent by the Redis server, which is the `detail`.
    match (our_response, their_response) {
        (Err(ref our_err), Err(ref their_err)) => {
            let our_msg = our_err.detail();
            let their_msg = their_err.detail();

            assert_eq!(our_msg, their_msg);
        }
        _ => {}
    }
}

#[tokio::test]
#[serial]
async fn test_set_and_get() {
    test_compare::<Vec<Value>>(|p| {
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
#[serial]
async fn test_getex() {
    test_compare::<Vec<Value>>(|p| {
        p.cmd("SET").arg("getex_key_1").arg(1).arg("EX").arg(1);
        p.cmd("GETEX").arg("getex_key_1").arg("PERSIST");
        p.cmd("TTL").arg("getex_key_1");

        p.cmd("SET").arg("getex_key_2").arg(1).arg("EX").arg(1);
        p.cmd("TTL").arg("getex_key_2");
        p.cmd("GETEX").arg("getex_key_2").arg("EX").arg(10);
        p.cmd("TTL").arg("getex_key_2");
    })
    .await;
}

#[tokio::test]
#[serial]
async fn test_pttl() {
    test_compare::<Vec<Value>>(|p| {
        p.cmd("SET").arg("pttl_key_1").arg(1).arg("EX").arg(1);
        p.cmd("PTTL").arg("pttl_key_1");

        p.cmd("SET").arg("pttl_key_2").arg(1);
        p.cmd("PTTL").arg("pttl_key_2");

        p.cmd("PTTL").arg("pttl_key_3");
    })
    .await;
}

#[tokio::test]
#[serial]
async fn test_set_args() {
    test_compare::<Vec<Value>>(|p| {
        p.cmd("SET").arg("set_args_key_1").arg(1).arg("XX");
        p.cmd("SET").arg("set_args_key_1").arg(2).arg("NX");
        p.cmd("SET").arg("set_args_key_1").arg(3).arg("XX");
        p.cmd("GET").arg("set_args_key_1");

        p.cmd("SET").arg("set_args_key_2").arg(1).arg("GET");
        p.cmd("SET").arg("set_args_key_2").arg(2).arg("GET");
    })
    .await;
}

#[tokio::test]
#[serial]
async fn test_del() {
    test_compare::<Vec<Value>>(|p| {
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
#[serial]
async fn test_exists() {
    test_compare::<Vec<Value>>(|p| {
        p.cmd("SET").arg("exists_key_1").arg(1);
        p.cmd("SET").arg("exists_key_2").arg("Argentina");

        p.cmd("EXISTS").arg("exists_key_1");
        p.cmd("EXISTS").arg("exists_key_2");
        p.cmd("EXISTS").arg("exists_key_3");
    })
    .await;
}

#[tokio::test]
#[serial]
async fn test_incr() {
    test_compare::<Vec<Value>>(|p| {
        p.cmd("SET").arg("incr_key_1").arg(1);
        p.cmd("SET").arg("incr_key_2").arg(1);
        p.cmd("SET").arg("incr_key_3").arg("1");

        p.cmd("INCR").arg("incr_key_1");
        p.cmd("INCR").arg("incr_key_2");
        p.cmd("INCR").arg("incr_key_3");

        p.cmd("INCR").arg("incr_key_4");
    })
    .await;
}

#[tokio::test]
#[serial]
async fn test_incr_by() {
    test_compare::<Vec<Value>>(|p| {
        p.cmd("SET").arg("incr_by_key_1").arg(2);
        p.cmd("SET").arg("incr_by_key_2").arg(10);
        p.cmd("SET").arg("incr_by_key_3").arg("2");

        p.cmd("INCRBY").arg("incr_by_key_1").arg(10);
        p.cmd("INCRBY").arg("incr_by_key_2").arg("7");
        p.cmd("INCRBY").arg("incr_by_key_3").arg(-2);
    })
    .await;

    test_compare_err(|p| {
        // Value is not an integer or out of range error.
        p.cmd("SET")
            .arg("incr_by_key_4")
            .arg("234293482390480948029348230948");
        p.cmd("INCRBY").arg("incr_by_key_4").arg(1);
    })
    .await;
}

#[tokio::test]
#[serial]
async fn test_incr_by_float() {
    test_compare::<Vec<Value>>(|p| {
        p.cmd("SET").arg("incr_by_float_key_1").arg("10.50");
        p.cmd("SET").arg("incr_by_float_key_2").arg(4);
        p.cmd("SET").arg("incr_by_float_key_3").arg("2.2");

        p.cmd("INCRBYFLOAT").arg("incr_by_float_key_1").arg("0.1");
        p.cmd("INCRBYFLOAT").arg("incr_by_float_key_2").arg("-5");
        p.cmd("INCRBYFLOAT").arg("incr_by_float_key_3").arg("-1.2");
    })
    .await;
}

#[tokio::test]
#[serial]
async fn test_decr() {
    test_compare::<Vec<Value>>(|p| {
        p.cmd("SET").arg("decr_key_1").arg(2);
        p.cmd("SET").arg("decr_key_2").arg(2);
        p.cmd("SET").arg("decr_key_3").arg("2");

        p.cmd("DECR").arg("decr_key_1");
        p.cmd("DECR").arg("decr_key_2");
        p.cmd("DECR").arg("decr_key_3");

        p.cmd("DECR").arg("decr_key_4");
    })
    .await;

    test_compare_err(|p| {
        // Value is not an integer or out of range error.
        p.cmd("SET")
            .arg("decr_key_5")
            .arg("234293482390480948029348230948");
        p.cmd("DECR").arg("decr_key_5");
    })
    .await;
}

#[tokio::test]
#[serial]
async fn test_decr_by() {
    test_compare::<Vec<Value>>(|p| {
        p.cmd("SET").arg("decr_by_key_1").arg(2);
        p.cmd("SET").arg("decr_by_key_2").arg(10);
        p.cmd("SET").arg("decr_by_key_3").arg("2");

        p.cmd("DECRBY").arg("decr_by_key_1").arg(10);
        p.cmd("DECRBY").arg("decr_by_key_2").arg("7");
        p.cmd("DECRBY").arg("decr_by_key_3").arg(2);
    })
    .await;

    test_compare_err(|p| {
        // Value is not an integer or out of range error.
        p.cmd("SET")
            .arg("decr_by_key_4")
            .arg("234293482390480948029348230948");
        p.cmd("DECRBY").arg("decr_by_key_4").arg(1);
    })
    .await;
}

#[tokio::test]
#[serial]
async fn test_append() {
    test_compare::<Vec<Value>>(|p| {
        p.cmd("APPEND").arg("append_key_1").arg("hello");
        p.cmd("APPEND").arg("append_key_1").arg(" World");
        p.cmd("GET").arg("append_key_1");

        p.cmd("SET").arg("append_key_2").arg(1);
        p.cmd("APPEND").arg("append_key_2").arg(" hello");
        p.cmd("GET").arg("append_key_2");

        p.cmd("APPEND").arg("append_key_3").arg(1);
        p.cmd("APPEND").arg("append_key_3").arg(2);
        p.cmd("GET").arg("append_key_3");
    })
    .await;
}

#[tokio::test]
#[serial]
async fn test_getdel() {
    test_compare::<Vec<Value>>(|p| {
        p.cmd("SET").arg("getdel_key_1").arg(2);
        p.cmd("SET").arg("getdel_key_2").arg("2");

        p.cmd("GETDEL").arg("getdel_key_1");
        p.cmd("GETDEL").arg("getdel_key_2");

        p.cmd("GET").arg("getdel_key_1");
        p.cmd("GET").arg("getdel_key_2");
    })
    .await;
}

#[tokio::test]
#[serial]
async fn test_getrange() {
    test_compare::<Vec<Value>>(|p| {
        p.cmd("SET").arg("getrange_key_1").arg("This is a string");
        p.cmd("GETRANGE").arg("getrange_key_1").arg(0).arg(0);
        p.cmd("GETRANGE").arg("getrange_key_1").arg(0).arg(3);
        p.cmd("GETRANGE").arg("getrange_key_1").arg(-3).arg(-1);
        p.cmd("GETRANGE").arg("getrange_key_1").arg("0").arg(-1);
        p.cmd("GETRANGE").arg("getrange_key_1").arg(10).arg("100");

        p.cmd("SET").arg("getrange_key_2").arg("");
        p.cmd("GETRANGE").arg("getrange_key_2").arg(0).arg(0);
        p.cmd("GETRANGE").arg("getrange_key_2").arg(0).arg(3);
        p.cmd("GETRANGE").arg("getrange_key_2").arg(-3).arg(-1);
    })
    .await;
}

#[tokio::test]
#[serial]
async fn test_keys() {
    // TODO: The response order from the server is not guaranteed, to ensure accurate comparison
    // with the expected result, we need to sort the response before performing the comparison.
    test_compare::<Vec<Value>>(|p| {
        p.cmd("SET").arg("keys_key_1").arg("Argentina");
        p.cmd("SET").arg("keys_key_2").arg("Spain");
        p.cmd("SET").arg("keys_key_3").arg("Netherlands");

        p.cmd("KEYS").arg("*");
        p.cmd("KEYS").arg("*key*");
        p.cmd("KEYS").arg("*3");
    })
    .await;
}

#[tokio::test]
#[serial]
async fn test_mget() {
    test_compare::<Vec<Value>>(|p| {
        p.cmd("SET").arg("mget_key_1").arg("Argentina");
        p.cmd("SET").arg("mget_key_2").arg("Spain");
        p.cmd("SET").arg("mget_key_3").arg("Netherlands");

        p.cmd("MGET")
            .arg("mget_key_1")
            .arg("mget_key_2")
            .arg("mget_key_3")
            .arg("nonexisting");
    })
    .await;
}

#[tokio::test]
#[serial]
async fn test_mset() {
    test_compare::<Vec<Value>>(|p| {
        p.cmd("MSET")
            .arg("mset_key_1")
            .arg("Argentina")
            .arg("mset_key_2")
            .arg("Spain")
            .arg("mset_key_3")
            .arg("Netherlands");

        p.cmd("MGET")
            .arg("mset_key_1")
            .arg("mset_key_2")
            .arg("mset_key_3");
    })
    .await;
}

#[tokio::test]
#[serial]
async fn test_msetnx() {
    test_compare::<Vec<Value>>(|p| {
        // When a key already exists, MSETNX does not perform any operation.
        p.cmd("SET").arg("msetnx_key_1").arg("Argentina");

        p.cmd("MSETNX")
            .arg("msetnx_key_1")
            .arg("Argentina")
            .arg("msetnx_key_2")
            .arg("Spain");

        p.cmd("MSETNX")
            .arg("msetnx_key_3")
            .arg("Thailand")
            .arg("msetnx_key_4")
            .arg("Brazil")
            .arg("msetnx_key_5")
            .arg("Peru");

        p.cmd("MGET")
            .arg("msetnx_key_1")
            .arg("msetnx_key_2")
            .arg("msetnx_key_3")
            .arg("msetnx_key_4")
            .arg("msetnx_key_5");
    })
    .await;
}
