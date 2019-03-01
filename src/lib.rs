//! This is a rust implementation for Redis cluster library.
//!
//! This library extends redis-rs library to be able to use cluster.
//! Client impletemts traits of ConnectionLike and Commands.
//! So you can use redis-rs's access methods.
//! If you want more information, read document of redis-rs.
//!
//! Note that this library is currently not have features of Pubsub.
//!
//! # Example
//! ```rust
//! extern crate redis_cluster_rs;
//! extern crate tokio;
//!
//!
//! use tokio::{prelude::*, runtime::current_thread::Runtime};
//!
//! use redis_cluster_rs::{Client, redis::{Commands, cmd}};
//!
//! fn main() {
//! #   let _ = env_logger::try_init();
//!     let nodes = vec!["redis://127.0.0.1:7000/", "redis://127.0.0.1:7001/", "redis://127.0.0.1:7002/"];
//!
//!     let mut runtime = Runtime::new().unwrap();
//!     runtime.block_on(future::lazy(|| {
//!         let client = Client::open(nodes).unwrap();
//!         client.get_connection().and_then(|connection| {
//!             cmd("SET").arg("test").arg("test_data").clone().query_async(connection)
//!                 .and_then(|(connection, ())| {
//!                     cmd("GET").arg("test").clone().query_async(connection)
//!                 })
//!                 .map(|(_, res): (_, String)| {
//!                     assert_eq!(res, "test_data");
//!                 })
//!         })
//!     })).unwrap()
//! }
//! ```
//!
//! # Pipelining
//! ```rust
//! extern crate redis_cluster_rs;
//! extern crate tokio;
//!
//! use tokio::{prelude::*, runtime::current_thread::Runtime};
//!
//! use redis_cluster_rs::{Client, redis::{PipelineCommands, pipe}};
//!
//! fn main() {
//! #   let _ = env_logger::try_init();
//!     let nodes = vec!["redis://127.0.0.1:7000/", "redis://127.0.0.1:7001/", "redis://127.0.0.1:7002/"];
//!
//!     let mut runtime = Runtime::new().unwrap();
//!     runtime.block_on(future::lazy(|| {
//!         let client = Client::open(nodes).unwrap();
//!         client.get_connection().and_then(|connection| {
//!             let key = "test2";
//!
//!             let mut pipe = pipe();
//!             pipe.rpush(key, "123").ignore()
//!                 .ltrim(key, -10, -1).ignore()
//!                 .expire(key, 60).ignore();
//!             pipe.query_async(connection)
//!                 .map(|(_, ())| ())
//!         })
//!     })).unwrap();
//! }
//! ```
extern crate crc16;
#[macro_use]
extern crate futures;
extern crate log;
extern crate rand;

pub extern crate redis;

use std::{
    collections::{HashMap, HashSet},
    fmt,
    io::{self, BufRead, Cursor},
    iter::Iterator,
    mem,
};

use crc16::*;
use futures::{
    future,
    prelude::*,
    stream,
    sync::{mpsc, oneshot},
    StartSend,
};
use log::trace;
use rand::seq::sample_iter;
use rand::thread_rng;
use redis::{
    r#async::ConnectionLike, Cmd, ConnectionAddr, ConnectionInfo, ErrorKind, IntoConnectionInfo,
    RedisError, RedisFuture, RedisResult, Value,
};

const SLOT_SIZE: usize = 16384;

/// This is a Redis cluster client.
pub struct Client {
    initial_nodes: Vec<ConnectionInfo>,
}

impl Client {
    /// Connect to a redis cluster server and return a cluster client.
    /// This does not actually open a connection yet but it performs some basic checks on the URL.
    ///
    /// # Errors
    ///
    /// If it is failed to parse initial_nodes, an error is returned.
    pub fn open<T: IntoConnectionInfo>(initial_nodes: Vec<T>) -> RedisResult<Client> {
        let mut nodes = Vec::with_capacity(initial_nodes.len());

        for info in initial_nodes {
            let info = info.into_connection_info()?;
            if let ConnectionAddr::Unix(_) = *info.addr {
                return Err(RedisError::from((ErrorKind::InvalidClientConfig,
                                             "This library cannot use unix socket because Redis's cluster command returns only cluster's IP and port.")));
            }
            nodes.push(info);
        }

        Ok(Client {
            initial_nodes: nodes,
        })
    }

    /// Open and get a Redis cluster connection.
    ///
    /// # Errors
    ///
    /// If it is failed to open connections and to create slots, an error is returned.
    pub fn get_connection(&self) -> impl ImplRedisFuture<Connection> {
        Connection::new(self.initial_nodes.clone())
    }
}

/// This is a connection of Redis cluster.
#[derive(Clone)]
pub struct Connection(mpsc::Sender<Message>);

impl Connection {
    fn new(initial_nodes: Vec<ConnectionInfo>) -> impl ImplRedisFuture<Connection> {
        Pipeline::new(initial_nodes).map(|pipeline| {
            let (tx, rx) = mpsc::channel(100);
            tokio_executor::spawn(rx.forward(pipeline).map(|_| ()));
            Connection(tx)
        })
    }
}

struct Pipeline {
    connections: HashMap<String, redis::r#async::SharedConnection>,
    slots: HashMap<u16, String>,
    state: ConnectionState,
    futures: Vec<Request<RedisFuture<(String, RedisResult<Vec<Value>>)>, Vec<Value>>>,
}

#[derive(Clone)]
struct CmdArg {
    cmd: Vec<u8>,
    offset: usize,
    count: usize,
}

struct Message {
    cmd: CmdArg,
    sender: oneshot::Sender<RedisResult<Vec<Value>>>,
    func: fn(
        redis::r#async::SharedConnection,
        CmdArg,
    ) -> RedisFuture<(redis::r#async::SharedConnection, Vec<Value>)>,
}

enum ConnectionState {
    PollComplete,
    Recover(
        RedisFuture<(
            HashMap<u16, String>,
            HashMap<String, redis::r#async::SharedConnection>,
        )>,
    ),
}

impl fmt::Debug for ConnectionState {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(
            f,
            "{}",
            match self {
                ConnectionState::PollComplete => "PollComplete",
                ConnectionState::Recover(_) => "Recover",
            }
        )
    }
}

struct RequestInfo {
    cmd: CmdArg,
    slot: Option<u16>,
    func: fn(
        redis::r#async::SharedConnection,
        CmdArg,
    ) -> RedisFuture<(redis::r#async::SharedConnection, Vec<Value>)>,
    excludes: HashSet<String>,
}

struct Request<F, I> {
    retries: u32,
    sender: Option<oneshot::Sender<RedisResult<I>>>,
    info: RequestInfo,
    future: F,
}

#[must_use]
enum Next {
    TryNewConnection,
    Delay,
    Done,
}

impl<F, I> Request<F, I>
where
    F: Future<Item = (String, RedisResult<I>)>,
{
    fn poll_request(&mut self, connections_len: usize) -> Poll<Next, RedisError> {
        match self.future.poll() {
            Ok(Async::Ready((_, Ok(item)))) => {
                self.send(Ok(item));
                Ok(Async::Ready(Next::Done))
            }
            Ok(Async::NotReady) => Ok(Async::NotReady),
            Ok(Async::Ready((addr, Err(err)))) => {
                if err.kind() == ErrorKind::ExtensionError {
                    let error_code = err.extension_error_code().unwrap();

                    if error_code == "MOVED" || error_code == "ASK" {
                        // Refresh slots and request again.
                        self.info.excludes.clear();
                        return Err(err);
                    } else if error_code == "TRYAGAIN" || error_code == "CLUSTERDOWN" {
                        // Sleep and retry.
                        let _sleep_time_millis = 2u64.pow(16 - self.retries.max(9)) * 10;
                        self.info.excludes.clear();
                        return Ok(Async::Ready(Next::Delay)); // FIXME Register delay
                    }
                }

                self.info.excludes.insert(addr);

                if self.info.excludes.len() >= connections_len {
                    self.send(Err(err));
                    return Ok(Async::Ready(Next::Done));
                }

                Ok(Async::Ready(Next::TryNewConnection))
            }
            Err(_) => unreachable!(), // TODO USe Void
        }
    }

    fn send(&mut self, msg: RedisResult<I>) {
        // If `send` errors the receiver has dropped and thus does not care about the message
        let _ = self
            .sender
            .take()
            .expect("Result should only be sent once")
            .send(msg);
    }
}

impl Pipeline {
    fn new(initial_nodes: Vec<ConnectionInfo>) -> impl ImplRedisFuture<Pipeline> {
        Self::create_initial_connections(&initial_nodes).and_then(|connections| {
            let mut connection = Some(Pipeline {
                connections,
                slots: HashMap::with_capacity(SLOT_SIZE),
                futures: Vec::new(),
                state: ConnectionState::PollComplete,
            });
            let mut refresh_slots_future = connection.as_mut().unwrap().refresh_slots();
            future::poll_fn(move || {
                let (slots, connections) = try_ready!(refresh_slots_future.poll());
                let mut connection = connection.take().unwrap();
                connection.slots = slots;
                connection.connections = connections;
                Ok(connection.into())
            })
        })
    }

    fn create_initial_connections(
        initial_nodes: &Vec<ConnectionInfo>,
    ) -> impl ImplRedisFuture<HashMap<String, redis::r#async::SharedConnection>> {
        let connections = HashMap::with_capacity(initial_nodes.len());

        stream::iter_ok(initial_nodes.clone())
            .and_then(|info| {
                let addr = match *info.addr {
                    ConnectionAddr::Tcp(ref host, port) => format!("redis://{}:{}", host, port),
                    _ => panic!("No reach."),
                };

                connect_and_check(info.clone()).then(|result| {
                    Ok(match result {
                        Ok(conn) => Some((addr, conn)),
                        Err(_) => None,
                    })
                })
            })
            .fold(connections, |mut connections, conn| -> RedisResult<_> {
                connections.extend(conn);
                Ok(connections)
            })
            .and_then(|connections| {
                if connections.len() == 0 {
                    return Err(RedisError::from((
                        ErrorKind::IoError,
                        "It is failed to check startup nodes.",
                    )));
                }
                Ok(connections)
            })
    }

    // Query a node to discover slot-> master mappings.
    fn refresh_slots(
        &mut self,
    ) -> impl ImplRedisFuture<(
        HashMap<u16, String>,
        HashMap<String, redis::r#async::SharedConnection>,
    )> {
        let slots_len = self.slots.len();

        let slots_future = {
            let mut rng = thread_rng();
            let samples = sample_iter(
                &mut rng,
                self.connections.values().cloned().collect::<Vec<_>>(),
                self.connections.len(),
            )
            .ok()
            .unwrap();

            stream::iter_ok(samples)
                .and_then(|conn| get_slots(conn).then(|result| Ok(result.ok())))
                .filter_map(|opt| opt)
                .into_future()
                .map_err(|(err, _)| err)
                .and_then(move |(opt, _)| {
                    let mut new_slots = HashMap::with_capacity(slots_len);
                    if let Some(slots_data) = opt {
                        for slot_data in slots_data {
                            for slot in slot_data.start()..slot_data.end() + 1 {
                                new_slots.insert(slot, slot_data.master().to_string());
                            }
                        }
                    }
                    if new_slots.len() != SLOT_SIZE {
                        return Err(RedisError::from((
                            ErrorKind::ResponseError,
                            "Slot refresh error.",
                        )));
                    }
                    Ok(new_slots)
                })
        };

        let mut connections = mem::replace(&mut self.connections, Default::default());

        // Remove dead connections and connect to new nodes if necessary
        let new_connections = HashMap::with_capacity(connections.len());

        slots_future.and_then(|slots| {
            stream::iter_ok(slots.values().cloned().collect::<Vec<_>>())
                .fold(new_connections, move |mut new_connections, addr| {
                    if !new_connections.contains_key(&addr) {
                        let mut new_connection_future =
                            if let Some(conn) = connections.remove(&addr) {
                                future::Either::A(
                                    check_connection(conn)
                                        .map({
                                            let addr = addr.clone();
                                            move |conn| Some((addr.to_string(), conn))
                                        })
                                        .or_else(move |_| {
                                            connect_and_check(addr.as_ref())
                                                .then(move |result| {
                                                    Ok(match result {
                                                        Ok(conn) => Some((addr.to_string(), conn)),
                                                        Err(_) => None,
                                                    })
                                                })
                                                .map_err(|err: RedisError| err)
                                        }),
                                )
                            } else {
                                future::Either::B(connect_and_check(addr.as_ref()).then(
                                    move |result| {
                                        Ok(match result {
                                            Ok(conn) => Some((addr.to_string(), conn)),
                                            Err(_) => None,
                                        })
                                    },
                                ))
                            };
                        future::Either::A(future::poll_fn(move || {
                            let new_connection = try_ready!(new_connection_future.poll());
                            new_connections.extend(new_connection);
                            Ok(mem::replace(&mut new_connections, Default::default()).into())
                        }))
                    } else {
                        future::Either::B(
                            future::ok(new_connections).map_err(|err: RedisError| err),
                        )
                    }
                })
                .map(|connections| (slots, connections))
        })
    }

    fn get_connection(
        &mut self,
        slot: u16,
    ) -> impl ImplRedisFuture<(String, redis::r#async::SharedConnection)> + 'static {
        if let Some(addr) = self.slots.get(&slot) {
            if self.connections.contains_key(addr) {
                return future::Either::A(future::ok((
                    addr.clone(),
                    self.connections.get(addr).unwrap().clone(),
                )));
            }

            // connections.entry(addr.to_string()).or_insert(conn),
            // Create new connection.
            //
            let random_conn = get_random_connection(&self.connections, None); // TODO Only do this lookup if the first check fails
            let addr = addr.clone();
            future::Either::B(
                connect_and_check(addr.as_ref())
                    .map(|conn| (addr, conn))
                    .or_else(|_| future::ok(random_conn)),
            )
        } else {
            // Return a random connection
            future::Either::A(future::ok(get_random_connection(&self.connections, None)))
        }
    }

    fn try_request(
        &mut self,
        info: &RequestInfo,
    ) -> impl ImplRedisFuture<(String, RedisResult<Vec<Value>>)> {
        let cmd = info.cmd.clone();
        let func = info.func;
        (if info.excludes.len() > 0 || info.slot.is_none() {
            future::Either::A(future::ok(get_random_connection(
                &self.connections,
                Some(&info.excludes),
            )))
        } else {
            future::Either::B(self.get_connection(info.slot.unwrap()))
        })
        .and_then(move |(addr, conn)| {
            func(conn, cmd)
                .map(|(_, value)| value)
                .then(|result| Ok((addr, result)))
        })
    }
}

impl Sink for Pipeline {
    type SinkItem = Message;
    type SinkError = ();

    fn start_send(&mut self, msg: Message) -> StartSend<Message, Self::SinkError> {
        trace!("start_send");
        let cmd = msg.cmd;

        let retries = 16;
        let excludes = HashSet::new();
        let slot = slot_for_packed_command(&cmd.cmd);

        let info = RequestInfo {
            cmd: cmd.clone(), // TODO remove clone
            func: msg.func,
            slot,
            excludes,
        };
        let request = Request {
            retries,
            sender: Some(msg.sender),
            future: Box::new(self.try_request(&info))
                as RedisFuture<(String, RedisResult<Vec<Value>>)>,
            info,
        };
        self.futures.push(request);
        Ok(AsyncSink::Ready)
    }

    fn poll_complete(&mut self) -> Poll<(), Self::SinkError> {
        trace!("poll_complete: {:?}", self.state);
        loop {
            self.state = match mem::replace(&mut self.state, ConnectionState::PollComplete) {
                ConnectionState::Recover(mut future) => match future.poll() {
                    Ok(Async::Ready((slots, connections))) => {
                        self.slots = slots;
                        self.connections = connections;
                        ConnectionState::PollComplete
                    }
                    Ok(Async::NotReady) => {
                        self.state = ConnectionState::Recover(future);
                        return Ok(Async::NotReady);
                    }
                    Err(_err) => ConnectionState::Recover(Box::new(self.refresh_slots())),
                },
                ConnectionState::PollComplete => {
                    let mut error = None;
                    let mut i = 0;

                    while i < self.futures.len() {
                        match self.futures[i].poll_request(self.connections.len()) {
                            Ok(Async::NotReady) => {
                                i += 1;
                            }
                            Ok(Async::Ready(next)) => match next {
                                Next::Delay => unimplemented!(),
                                Next::Done => {
                                    self.futures.swap_remove(i);
                                }
                                Next::TryNewConnection => {
                                    let mut request = self.futures.swap_remove(i);
                                    request.retries -= 1;
                                    request.future = Box::new(self.try_request(&request.info));
                                    self.futures.push(request);
                                }
                            },
                            Err(err) => error = Some(err),
                        }
                    }

                    if let Some(_err) = error {
                        ConnectionState::Recover(Box::new(self.refresh_slots()))
                    } else if self.futures.is_empty() {
                        return Ok(Async::Ready(()));
                    } else {
                        return Ok(Async::NotReady);
                    }
                }
            }
        }
    }
}

impl ConnectionLike for Connection {
    fn req_packed_command(self, cmd: Vec<u8>) -> RedisFuture<(Self, Value)> {
        trace!("req_packed_command");
        let (sender, receiver) = oneshot::channel();
        Box::new(
            self.0
                .clone()
                .send(Message {
                    cmd: CmdArg {
                        cmd,
                        offset: 0,
                        count: 0,
                    },
                    sender,
                    func: |conn, cmd| {
                        Box::new(
                            conn.req_packed_command(cmd.cmd)
                                .map(|(conn, value)| (conn, vec![value])),
                        )
                    }, // FIXME Slow
                })
                .map_err(|_| RedisError::from(io::Error::from(io::ErrorKind::BrokenPipe)))
                .and_then(move |_| {
                    receiver.then(|result| {
                        result
                            .unwrap_or_else(|_| {
                                Err(RedisError::from(io::Error::from(io::ErrorKind::BrokenPipe)))
                            })
                            .map(|mut vec| (self, vec.pop().unwrap()))
                    })
                }),
        )
    }

    fn req_packed_commands(
        self,
        cmd: Vec<u8>,
        offset: usize,
        count: usize,
    ) -> RedisFuture<(Self, Vec<Value>)> {
        let (sender, receiver) = oneshot::channel();
        Box::new(
            self.0
                .clone()
                .send(Message {
                    cmd: CmdArg { cmd, offset, count },
                    sender,
                    func: |conn, cmd| conn.req_packed_commands(cmd.cmd, cmd.offset, cmd.count),
                })
                .map_err(|_| RedisError::from(io::Error::from(io::ErrorKind::BrokenPipe)))
                .and_then(move |_| {
                    receiver.then(|result| {
                        result
                            .unwrap_or_else(|_| {
                                Err(RedisError::from(io::Error::from(io::ErrorKind::BrokenPipe)))
                            })
                            .map(|vec| (self, vec))
                    })
                }),
        )
    }

    fn get_db(&self) -> i64 {
        0
    }
}

impl Clone for Client {
    fn clone(&self) -> Client {
        Client::open(self.initial_nodes.clone()).unwrap()
    }
}

macro_rules! try_future {
    ($e:expr) => {
        try_future!($e, $crate::box_future)
    };
    ($e:expr, $f:expr) => {
        match $e {
            Ok(x) => x,
            Err(err) => return $f(::futures::future::err(err.into())),
        }
    };
}

pub(crate) fn box_future<'a, F>(
    f: F,
) -> Box<dyn Future<Item = F::Item, Error = F::Error> + Send + 'a>
where
    F: Future + Send + 'a,
{
    Box::new(f)
}

// FIXME Don't be public
pub trait ImplRedisFuture<T>: Future<Item = T, Error = RedisError> {}
impl<T, F> ImplRedisFuture<T> for F where F: Future<Item = T, Error = RedisError> {}

fn connect<T>(info: T) -> RedisFuture<redis::r#async::SharedConnection>
where
    T: IntoConnectionInfo,
{
    let connection_info = try_future!(info.into_connection_info());
    let client = try_future!(redis::Client::open(connection_info));
    Box::new(client.get_shared_async_connection())
}

fn connect_and_check<T>(info: T) -> RedisFuture<redis::r#async::SharedConnection>
where
    T: IntoConnectionInfo,
{
    Box::new(connect(info).and_then(|conn| check_connection(conn)))
}

fn check_connection(
    conn: redis::r#async::SharedConnection,
) -> impl ImplRedisFuture<redis::r#async::SharedConnection> {
    let mut cmd = Cmd::new();
    cmd.arg("PING");
    cmd.query_async::<_, String>(conn).map(|(conn, _)| conn)
}

fn get_random_connection<'a>(
    connections: &'a HashMap<String, redis::r#async::SharedConnection>,
    excludes: Option<&'a HashSet<String>>,
) -> (String, redis::r#async::SharedConnection) {
    let mut rng = thread_rng();
    let samples = match excludes {
        Some(excludes) if excludes.len() < connections.len() => {
            let target_keys = connections.keys().filter(|key| !excludes.contains(*key));
            sample_iter(&mut rng, target_keys, 1).unwrap()
        }
        _ => sample_iter(&mut rng, connections.keys(), 1).unwrap(),
    };

    let addr = samples.first().unwrap();
    (addr.to_string(), connections.get(*addr).unwrap().clone())
}

fn slot_for_packed_command(cmd: &[u8]) -> Option<u16> {
    let args = unpack_command(cmd);
    if args.len() > 1 {
        Some(State::<XMODEM>::calculate(&args[1]) % SLOT_SIZE as u16)
    } else {
        None
    }
}

fn unpack_command(cmd: &[u8]) -> Vec<Vec<u8>> {
    let mut args: Vec<Vec<u8>> = Vec::new();

    let cursor = Cursor::new(cmd);
    for line in cursor.lines() {
        if let Ok(line) = line {
            if !line.starts_with("*") && !line.starts_with("$") {
                args.push(line.into_bytes());
            }
        }
    }
    args
}

#[derive(Debug)]
struct Slot {
    start: u16,
    end: u16,
    master: String,
    replicas: Vec<String>,
}

impl Slot {
    pub fn start(&self) -> u16 {
        self.start
    }
    pub fn end(&self) -> u16 {
        self.end
    }
    pub fn master(&self) -> &str {
        &self.master
    }
    #[allow(dead_code)]
    pub fn replicas(&self) -> &Vec<String> {
        &self.replicas
    }
}

// Get slot data from connection.
fn get_slots(
    connection: redis::r#async::SharedConnection,
) -> impl Future<Item = Vec<Slot>, Error = RedisError> {
    let mut cmd = Cmd::new();
    cmd.arg("CLUSTER").arg("SLOTS");
    let packed_command = cmd.get_packed_command();
    connection
        .req_packed_command(packed_command)
        .and_then(|(_connection, value)| {
            // Parse response.
            let mut result = Vec::with_capacity(2);

            if let Value::Bulk(items) = value {
                let mut iter = items.into_iter();
                while let Some(Value::Bulk(item)) = iter.next() {
                    if item.len() < 3 {
                        continue;
                    }

                    let start = if let Value::Int(start) = item[0] {
                        start as u16
                    } else {
                        continue;
                    };

                    let end = if let Value::Int(end) = item[1] {
                        end as u16
                    } else {
                        continue;
                    };

                    let mut nodes: Vec<String> = item
                        .into_iter()
                        .skip(2)
                        .filter_map(|node| {
                            if let Value::Bulk(node) = node {
                                if node.len() < 2 {
                                    return None;
                                }

                                let ip = if let Value::Data(ref ip) = node[0] {
                                    String::from_utf8_lossy(ip)
                                } else {
                                    return None;
                                };

                                let port = if let Value::Int(port) = node[1] {
                                    port
                                } else {
                                    return None;
                                };
                                Some(format!("redis://{}:{}", ip, port))
                            } else {
                                None
                            }
                        })
                        .collect();

                    if nodes.len() < 1 {
                        continue;
                    }

                    let replicas = nodes.split_off(1);
                    result.push(Slot {
                        start,
                        end,
                        master: nodes.pop().unwrap(),
                        replicas,
                    });;
                }
            }

            Ok(result)
        })
}
