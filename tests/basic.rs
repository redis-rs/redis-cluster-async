use std::{
    cell::Cell,
    error::Error,
    sync::{Mutex, MutexGuard},
    time::Duration,
};

use {
    proptest::proptest,
    tokio::{prelude::*, runtime::current_thread::Runtime},
};

use redis_cluster_rs::{
    redis::{cmd, RedisError},
    Client,
};

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
}

impl RedisEnv {
    pub fn new() -> Self {
        let _ = env_logger::try_init();

        let mut runtime = Runtime::new().unwrap();
        let redis_lock = RedisProcess::lock();

        let redis_client = redis::Client::open(REDIS_URL)
            .unwrap_or_else(|_| panic!("Failed to connect to '{}'", REDIS_URL));

        let node_infos = loop {
            let node_infos = runtime
                .block_on(future::lazy(|| {
                    redis_client
                        .get_shared_async_connection()
                        .and_then(|conn| Self::cluster_info(&conn))
                }))
                .expect("Unable to query nodes for information");
            // Wait for the cluster to stabilize
            if node_infos.iter().filter(|(_, master)| *master).count() == 3 {
                break node_infos;
            }
            std::thread::sleep(std::time::Duration::from_millis(100));
        };

        let mut nodes = Vec::new();
        // Clear databases:
        for (url, master) in node_infos {
            if master {
                nodes.push(url.to_string());
                let mut redis_client = redis::Client::open(&url[..])
                    .unwrap_or_else(|_| panic!("Failed to connect to '{}'", url));
                let () = redis::Cmd::new()
                    .arg("FLUSHALL")
                    .query(&mut redis_client)
                    .unwrap_or_else(|err| panic!("Unable to flush {}: {}", url, err));
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
            _redis_lock: redis_lock,
        }
    }

    fn cluster_info<T>(
        redis_client: &T,
    ) -> impl Future<Item = Vec<(String, bool)>, Error = RedisError>
    where
        T: Clone + redis::aio::ConnectionLike + Send + 'static,
    {
        redis::cmd("CLUSTER")
            .arg("NODES")
            .query_async(redis_client.clone())
            .map(|(_, s): (_, String)| {
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
    }

    fn replicas(&self) -> impl Future<Item = Vec<String>, Error = RedisError> {
        fn replicas_(
            conn: redis_cluster_rs::Connection,
            attempt: i32,
        ) -> impl Future<Item = Vec<String>, Error = RedisError> {
            if attempt > 10 {
                panic!("To many replica queries. The nodes does not seem to re-synchronize.",);
            }
            RedisEnv::cluster_info(&conn)
                .map(|cluster_info| {
                    cluster_info
                        .into_iter()
                        .filter_map(|(url, master)| if master { None } else { Some(url) })
                        .collect()
                })
                .and_then(move |replicas: Vec<_>| {
                    if !replicas.is_empty() {
                        Box::new(future::ok(replicas)) as Box<dyn Future<Item = _, Error = _>>
                    } else {
                        Box::new(
                            tokio_timer::sleep(Duration::from_millis(500))
                                .map_err(|err| panic!("{}", err))
                                .and_then(move |_| replicas_(conn, attempt + 1)),
                        ) as Box<dyn Future<Item = _, Error = _>>
                    }
                })
        }

        self.client
            .get_connection()
            .and_then(|conn| replicas_(conn, 1))
    }
}

#[test]
fn basic() {
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
    let env = std::cell::RefCell::new(FailoverEnv::new());

    proptest!(
        proptest::prelude::ProptestConfig { cases: 50, .. Default::default() },
        |(requests in 0..15, value in 0..i32::max_value())| {
            test_failover(&mut env.borrow_mut(), requests, value)
        }
    );
}

#[test]
fn basic_failover() {
    test_failover(&mut FailoverEnv::new(), 10, 123);
}

struct FailoverEnv {
    env: RedisEnv,
    connection: redis_cluster_rs::Connection,
}

impl FailoverEnv {
    fn new() -> Self {
        let mut env = RedisEnv::new();
        let connection = env.runtime.block_on(env.client.get_connection()).unwrap();

        FailoverEnv { env, connection }
    }
}

fn do_failover(
    redis: redis::aio::SharedConnection,
) -> impl Future<Item = (), Error = Box<dyn Error + Send + Sync + 'static>> {
    cmd("CLUSTER")
        .arg("FAILOVER")
        .query_async(redis)
        .map(|(_, ())| ())
        .from_err()
}

fn test_failover(env: &mut FailoverEnv, requests: i32, value: i32) {
    let completed = Cell::new(0);
    let completed = &completed;

    let FailoverEnv { env, connection } = env;

    let replicas = env.runtime.block_on(env.replicas()).unwrap();
    let failover_node_redis = {
        let redis_client = redis::Client::open(&replicas[0][..])
            .unwrap_or_else(|_| panic!("Failed to connect to '{}'", replicas[0]));
        env.runtime
            .block_on(redis_client.get_shared_async_connection())
            .unwrap()
    };

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
    env.runtime
        .block_on(test_future.join(future::lazy(|| do_failover(failover_node_redis.clone()))))
        .unwrap_or_else(|err| panic!("{}", err));
    assert_eq!(completed.get(), requests, "Some requests never completed!");
}
