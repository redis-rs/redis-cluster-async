extern crate lazy_static;
extern crate proptest;
extern crate redis_cluster_rs;
extern crate tokio;

use std::{
    cell::Cell,
    error::Error,
    sync::{Mutex, MutexGuard},
};

use {
    proptest::proptest,
    tokio::{prelude::*, runtime::current_thread::Runtime},
};

use redis_cluster_rs::{redis::cmd, Client};

const REDIS_URL: &str = "redis://127.0.0.1:7000/";

pub struct RedisProcess;
pub struct RedisLock(MutexGuard<'static, RedisProcess>);

impl RedisProcess {
    // Blocks until we have sole access.
    pub fn lock() -> RedisLock {
        lazy_static::lazy_static! {
            static ref REDIS: Mutex<RedisProcess> = Mutex::new(RedisProcess {});
        }

        // If we panic in a test we don't want subsequent to fail because of a poisoned error
        let redis_lock = REDIS
            .lock()
            .unwrap_or_else(|poison_error| poison_error.into_inner());
        RedisLock(redis_lock)
    }
}

// ----------------------------------------------------------------------------

pub struct RedisEnv {
    _redis_lock: RedisLock,
    pub runtime: Runtime,
    pub client: Client,
    pub nodes: Vec<String>,
    pub replicas: Vec<String>,
}

impl RedisEnv {
    pub fn new() -> Self {
        let mut runtime = Runtime::new().unwrap();
        let redis_lock = RedisProcess::lock();

        let redis_client = redis::Client::open(REDIS_URL)
            .unwrap_or_else(|_| panic!("Failed to connect to '{}'", REDIS_URL));

        let node_infos = redis::cmd("CLUSTER")
            .arg("NODES")
            .query(&redis_client)
            .map(|s: String| {
                s.lines()
                    .map(|line| {
                        let mut iter = line.split(' ');
                        (
                            format!(
                                "redis://{}",
                                iter.by_ref()
                                    .nth(1)
                                    .expect("Node ip")
                                    .splitn(2, '@')
                                    .next()
                                    .unwrap()
                            ),
                            iter.next().expect("master").contains("master"),
                        )
                    })
                    .collect::<Vec<_>>()
            })
            .expect("Unable to query nodes for information");

        let mut nodes = Vec::new();
        let mut replicas = Vec::new();
        // Clear databases:
        for (url, master) in node_infos {
            if master {
                nodes.push(url.to_string());
                let redis_client = redis::Client::open(&url[..])
                    .unwrap_or_else(|_| panic!("Failed to connect to '{}'", url));
                let () = redis::Cmd::new()
                    .arg("FLUSHALL")
                    .query(&redis_client)
                    .unwrap();
            } else {
                replicas.push(url.to_string());
            }
        }

        let client = runtime
            .block_on(future::lazy(|| {
                Client::open(nodes.iter().map(|s| &s[..]).collect())
            }))
            .unwrap();

        RedisEnv {
            runtime,
            client,
            nodes,
            replicas,
            _redis_lock: redis_lock,
        }
    }
}

#[test]
fn basic() {
    let _ = env_logger::try_init();

    let mut env = RedisEnv::new();
    let client = env.client;
    env.runtime
        .block_on(future::lazy(|| {
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

#[test]
fn proptests() {
    let _ = env_logger::try_init();

    let env = std::cell::RefCell::new(FailoverEnv::new());

    proptest!(|(requests in 0..15, value in 0..i32::max_value())| {
        test_failover(&mut env.borrow_mut(), requests, value)
    });
}

#[test]
fn failover() {
    test_failover(&mut FailoverEnv::new(), 10, 123);
}

struct FailoverEnv {
    env: RedisEnv,
    connection: redis_cluster_rs::Connection,
    failover_node_redis: redis::r#async::SharedConnection,
}

impl FailoverEnv {
    fn new() -> Self {
        let mut env = RedisEnv::new();
        let connection = env.runtime.block_on(env.client.get_connection()).unwrap();

        let failover_node_redis = {
            let redis_client = redis::Client::open(&env.replicas[1][..])
                .unwrap_or_else(|_| panic!("Failed to connect to '{}'", env.replicas[1]));
            env.runtime
                .block_on(redis_client.get_shared_async_connection())
                .unwrap()
        };
        FailoverEnv {
            env,
            connection,
            failover_node_redis,
        }
    }
}

fn do_failover(
    redis: redis::r#async::SharedConnection,
) -> impl Future<Item = (), Error = Box<Error + Send + Sync + 'static>> {
    cmd("CLUSTER")
        .arg("FAILOVER")
        .query_async(redis)
        .map(|(_, ())| ())
        .from_err()
}

fn test_failover(env: &mut FailoverEnv, requests: i32, value: i32) {
    let completed = Cell::new(0);
    let completed = &completed;

    let FailoverEnv {
        env: RedisEnv { runtime, .. },
        connection,
        failover_node_redis,
    } = env;

    let test_future = future::lazy(|| {
        stream::futures_unordered((0..requests).map(|i| {
            let key = format!("test-{}-{}", value, i);
            cmd("SET")
                .arg(&key)
                .arg(i)
                .clone()
                .query_async(connection.clone())
                .and_then(move |(connection, ())| {
                    cmd("GET").arg(key).clone().query_async(connection)
                })
                .map(move |(_, res): (_, i32)| {
                    assert_eq!(res, i);
                    completed.set(completed.get() + 1);
                })
        }))
        .collect()
        .from_err()
    });
    runtime
        .block_on(
            test_future
                .select(
                    future::lazy(|| do_failover(failover_node_redis.clone()))
                        .and_then(|_| future::empty()),
                )
                .map_err(|(err, _)| err),
        )
        .unwrap();
    assert_eq!(completed.get(), requests);
}
