use std::{
    collections::HashMap,
    sync::{Arc, RwLock},
};

use {
    futures::future,
    redis_cluster_rs::{
        redis::{
            aio::ConnectionLike, cmd, parse_redis_value, IntoConnectionInfo, RedisFuture,
            RedisResult, Value,
        },
        Client, Connect,
    },
    tokio::runtime::current_thread::Runtime,
};

type Handler = Arc<dyn Fn(&[u8]) -> Result<(), RedisResult<Value>> + Send + Sync>;

lazy_static::lazy_static! {
    static ref HANDLERS: RwLock<HashMap<String, Handler>>
        = Default::default();
}

#[derive(Clone)]
pub struct MockConnection(Handler);

impl Connect for MockConnection {
    fn connect<T>(info: T) -> RedisFuture<Self>
    where
        T: IntoConnectionInfo,
    {
        let info = info.into_connection_info().unwrap();

        let name = match &*info.addr {
            redis::ConnectionAddr::Tcp(addr, ..) => addr,
            _ => unreachable!(),
        };
        Box::new(future::ok(MockConnection(
            HANDLERS
                .read()
                .unwrap()
                .get(name)
                .unwrap_or_else(|| panic!("Handler `{}` were not installed", name))
                .clone(),
        )))
    }
}

fn contains_slice(xs: &[u8], ys: &[u8]) -> bool {
    for i in 0..xs.len() {
        if xs[i..].starts_with(ys) {
            return true;
        }
    }
    false
}

fn respond_startup(name: &str, cmd: &[u8]) -> Result<(), RedisResult<Value>> {
    if contains_slice(&cmd, b"PING") {
        Err(Ok(Value::Status("OK".into())))
    } else if contains_slice(&cmd, b"CLUSTER") && contains_slice(&cmd, b"SLOTS") {
        Err(Ok(Value::Bulk(vec![Value::Bulk(vec![
            Value::Int(0),
            Value::Int(16383),
            Value::Bulk(vec![
                Value::Data(name.as_bytes().to_vec()),
                Value::Int(6379),
            ]),
        ])])))
    } else {
        Ok(())
    }
}

impl ConnectionLike for MockConnection {
    fn req_packed_command(self, cmd: Vec<u8>) -> RedisFuture<(Self, Value)> {
        Box::new(future::result(
            (self.0)(&cmd)
                .expect_err("Handler did not specify a response")
                .map(|value| (self, value)),
        ))
    }

    fn req_packed_commands(
        self,
        _cmd: Vec<u8>,
        _offset: usize,
        _count: usize,
    ) -> RedisFuture<(Self, Vec<Value>)> {
        Box::new(future::ok((self, vec![])))
    }

    fn get_db(&self) -> i64 {
        0
    }
}

pub struct MockEnv {
    pub runtime: Runtime,
    pub connection: redis_cluster_rs::Connection<MockConnection>,
    #[allow(unused)]
    handler: RemoveHandler,
}

struct RemoveHandler(String);

impl Drop for RemoveHandler {
    fn drop(&mut self) {
        HANDLERS.write().unwrap().remove(&self.0);
    }
}

impl MockEnv {
    fn new(
        id: &str,
        handler: impl Fn(&[u8]) -> Result<(), RedisResult<Value>> + Send + Sync + 'static,
    ) -> Self {
        let mut runtime = Runtime::new().unwrap();

        let id = id.to_string();
        HANDLERS
            .write()
            .unwrap()
            .insert(id.clone(), Arc::new(handler));

        let connection = runtime
            .block_on(
                Client::open(vec![&*format!("redis://{}", id)])
                    .unwrap()
                    .get_generic_connection(),
            )
            .unwrap();
        MockEnv {
            runtime,
            connection,
            handler: RemoveHandler(id),
        }
    }
}

#[test]
fn tryagain() {
    let _ = env_logger::try_init();
    let name = "tryagain";
    let MockEnv {
        mut runtime,
        connection,
        handler: _,
    } = MockEnv::new(name, move |cmd: &[u8]| {
        respond_startup(name, cmd)?;

        Err(parse_redis_value(b"-MOVED 1\r\n"))
    });

    runtime
        .block_on(
            cmd("GET")
                .arg("test")
                .query_async::<_, Option<i32>>(connection),
        )
        .map(|(_, x)| x)
        .unwrap_err();
}
