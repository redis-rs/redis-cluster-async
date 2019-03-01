extern crate lazy_static;
extern crate proptest;
extern crate redis_cluster_rs;
extern crate tokio;

use std::{
    cell::Cell,
    sync::{Mutex, MutexGuard},
};

use {
    proptest::{prelude::*, proptest},
    tokio::{prelude::*, runtime::current_thread::Runtime},
};

use redis_cluster_rs::{redis::cmd, Client};

const NODES: &[&str] = &[
    "redis://127.0.0.1:7000/",
    "redis://127.0.0.1:7001/",
    "redis://127.0.0.1:7002/",
];

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

        // Clear databases:
        for url in NODES {
            let redis_client = redis::Client::open(*url)
                .unwrap_or_else(|_| panic!("Failed to connect to '{}'", url));
            let () = redis::Cmd::new()
                .arg("FLUSHALL")
                .query(&redis_client)
                .unwrap();
        }
        RedisLock(redis_lock)
    }
}

// ----------------------------------------------------------------------------

pub struct RedisEnv {
    _redis_lock: RedisLock,
    pub runtime: Runtime,
    pub client: Client,
}

impl RedisEnv {
    pub fn new() -> Self {
        let mut runtime = Runtime::new().unwrap();
        let redis_lock = RedisProcess::lock();

        let client = runtime
            .block_on(future::lazy(|| {
                Client::open(NODES.iter().cloned().collect())
            }))
            .unwrap();

        RedisEnv {
            runtime,
            client,
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

    let env = std::cell::RefCell::new(RedisEnv::new());

    proptest!(ProptestConfig { cases: 50, .. Default::default() }, |(requests in 0..15, value in 0..i32::max_value())| {
        let mut env = env.borrow_mut();
        let env = &mut *env;

        let client = &env.client;

        let completed = Cell::new(0);
        let completed = &completed;
        env.runtime
            .block_on(future::lazy(|| {
                client.get_connection().and_then(|connection| {
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
                    })).collect()
                })
            }))
            .unwrap();
        assert_eq!(completed.get(), requests);
    });
}
