use {
    futures::future,
    redis_cluster_rs::{
        redis::{
            aio::ConnectionLike, cmd, parse_redis_value, IntoConnectionInfo, RedisFuture, Value,
        },
        Client, Connect,
    },
    tokio::runtime::current_thread::Runtime,
};

#[derive(Clone)]
pub struct MockConnection;

impl Connect for MockConnection {
    fn connect<T>(_: T) -> RedisFuture<Self>
    where
        T: IntoConnectionInfo,
    {
        Box::new(future::ok(MockConnection))
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

impl ConnectionLike for MockConnection {
    fn req_packed_command(self, cmd: Vec<u8>) -> RedisFuture<(Self, Value)> {
        if contains_slice(&cmd, b"PING") {
            Box::new(future::ok((self, Value::Status("OK".into()))))
        } else if contains_slice(&cmd, b"CLUSTER") && contains_slice(&cmd, b"SLOTS") {
            Box::new(future::ok((
                self,
                Value::Bulk(vec![Value::Bulk(vec![
                    Value::Int(0),
                    Value::Int(16383),
                    Value::Bulk(vec![Value::Data(b"127.0.0.1".to_vec()), Value::Int(6379)]),
                ])]),
            )))
        } else {
            Box::new(future::err(parse_redis_value(b"-MOVED 1\r\n").unwrap_err()))
        }
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
}

impl MockEnv {
    fn new() -> Self {
        let mut runtime = Runtime::new().unwrap();

        let connection = runtime
            .block_on(
                Client::open(vec!["redis://test"])
                    .unwrap()
                    .get_generic_connection(),
            )
            .unwrap();
        MockEnv {
            runtime,
            connection,
        }
    }
}

#[test]
fn recover() {
    let _ = env_logger::try_init();
    let MockEnv {
        mut runtime,
        connection,
    } = MockEnv::new();

    runtime
        .block_on(
            cmd("GET")
                .arg("test")
                .query_async::<_, Option<i32>>(connection),
        )
        .map(|(_, x)| x)
        .unwrap_err();
}
