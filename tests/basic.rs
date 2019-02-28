extern crate redis_cluster_rs;
extern crate tokio;

use tokio::{prelude::*, runtime::current_thread::Runtime};

use redis_cluster_rs::{redis::cmd, Client};

#[test]
fn basic() {
    let _ = env_logger::try_init();
    let nodes = vec![
        "redis://127.0.0.1:7000/",
        "redis://127.0.0.1:7001/",
        "redis://127.0.0.1:7002/",
    ];

    let mut runtime = Runtime::new().unwrap();
    runtime
        .block_on(future::lazy(|| {
            let client = Client::open(nodes).unwrap();
            client.get_connection().and_then(|connection| {
                cmd("SET")
                    .arg("test")
                    .arg("test_data")
                    .clone()
                    .query_async(connection)
                    .and_then(|(connection, ())| {
                        cmd("GET").arg("test").clone().query_async(connection)
                    })
                    .map(|(_, res): (_, String)| {
                        assert_eq!(res, "test_data");
                    })
            })
        }))
        .unwrap()
}
