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
//! use tokio::{prelude::*, runtime::current_thread::Runtime};
//!
//! use redis_cluster_async::{Client, redis::{Commands, cmd}};
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
//! use tokio::{prelude::*, runtime::current_thread::Runtime};
//!
//! use redis_cluster_async::{Client, redis::{PipelineCommands, pipe}};
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

pub use redis;

use std::{
    collections::{BTreeMap, HashMap, HashSet},
    fmt, io,
    iter::Iterator,
    mem,
    time::Duration,
};

use crc16::*;
use futures::{
    future,
    prelude::*,
    stream,
    sync::{mpsc, oneshot},
    try_ready, StartSend,
};
use log::trace;
use rand::seq::IteratorRandom;
use rand::thread_rng;
use redis::{
    aio::ConnectionLike, Cmd, ConnectionAddr, ConnectionInfo, ErrorKind, IntoConnectionInfo,
    RedisError, RedisFuture, RedisResult, Value,
};

const SLOT_SIZE: usize = 16384;
const DEFAULT_RETRIES: u32 = 16;

/// This is a Redis cluster client.
pub struct Client {
    initial_nodes: Vec<ConnectionInfo>,
    retries: Option<u32>,
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
            retries: Some(DEFAULT_RETRIES),
        })
    }

    /// Set how many times we should retry a query. Set `None` to retry forever.
    /// Default: 16
    pub fn set_retries(&mut self, retries: Option<u32>) -> &mut Self {
        self.retries = retries;
        self
    }

    /// Open and get a Redis cluster connection.
    ///
    /// # Errors
    ///
    /// If it is failed to open connections and to create slots, an error is returned.
    pub fn get_connection(&self) -> impl Future<Item = Connection, Error = RedisError> {
        Connection::new(self.initial_nodes.clone(), self.retries)
    }

    #[doc(hidden)]
    pub fn get_generic_connection<C>(&self) -> impl Future<Item = Connection<C>, Error = RedisError>
    where
        C: ConnectionLike + Connect + Clone + Send + 'static,
    {
        Connection::new(self.initial_nodes.clone(), self.retries)
    }
}

/// This is a connection of Redis cluster.
#[derive(Clone)]
pub struct Connection<C = redis::aio::SharedConnection>(mpsc::Sender<Message<C>>);

impl<C> Connection<C>
where
    C: ConnectionLike + Connect + Clone + Send + 'static,
{
    fn new(
        initial_nodes: Vec<ConnectionInfo>,
        retries: Option<u32>,
    ) -> impl ImplRedisFuture<Connection<C>> {
        Pipeline::new(initial_nodes, retries).map(|pipeline| {
            let (tx, rx) = mpsc::channel(100);
            tokio_executor::spawn(rx.forward(pipeline).map(|_| ()));
            Connection(tx)
        })
    }
}

type SlotMap = BTreeMap<u16, String>;

struct Pipeline<C> {
    connections: HashMap<String, C>,
    slots: SlotMap,
    state: ConnectionState<C>,
    in_flight_requests: Vec<
        Request<
            Box<dyn Future<Item = (String, RedisResult<Response>), Error = Void> + Send>,
            Response,
            C,
        >,
    >,
    retries: Option<u32>,
}

#[derive(Clone)]
struct CmdArg {
    cmd: Vec<u8>,
    offset: usize,
    count: usize,
}

enum Response {
    Single(Value),
    Multiple(Vec<Value>),
}

struct Message<C> {
    cmd: CmdArg,
    sender: oneshot::Sender<RedisResult<Response>>,
    func: fn(C, CmdArg) -> RedisFuture<(C, Response)>,
}

enum ConnectionState<C> {
    PollComplete,
    Recover(RedisFuture<(SlotMap, HashMap<String, C>)>),
}

impl<C> fmt::Debug for ConnectionState<C> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
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

struct RequestInfo<C> {
    cmd: CmdArg,
    slot: Option<u16>,
    func: fn(C, CmdArg) -> RedisFuture<(C, Response)>,
    excludes: HashSet<String>,
}

enum RequestState<F> {
    None,
    Future(F),
    Delay(tokio_timer::Delay),
}

struct Request<F, I, C> {
    retry: u32,
    max_retries: Option<u32>,
    sender: Option<oneshot::Sender<RedisResult<I>>>,
    info: RequestInfo<C>,
    future: RequestState<F>,
}

#[must_use]
enum Next {
    TryNewConnection,
    Delay(Duration),
    Done,
}

enum Void {}

impl<F, I, C> Request<F, I, C>
where
    F: Future<Item = (String, RedisResult<I>), Error = Void>,
    C: ConnectionLike,
{
    fn poll_request(&mut self, connections_len: usize) -> Poll<Next, RedisError> {
        let future = match &mut self.future {
            RequestState::Future(f) => f,
            RequestState::Delay(delay) => {
                return Ok(match delay.poll() {
                    Ok(Async::Ready(_)) | Err(_) => Next::TryNewConnection.into(),
                    Ok(Async::NotReady) => Async::NotReady,
                });
            }
            _ => panic!("Request future must be Some"),
        };
        match future.poll() {
            Ok(Async::Ready((_, Ok(item)))) => {
                trace!("Ok");
                self.respond(Ok(item));
                Ok(Async::Ready(Next::Done))
            }
            Ok(Async::NotReady) => Ok(Async::NotReady),
            Ok(Async::Ready((addr, Err(err)))) => {
                trace!("Request error {} {:?}", err, self.info.cmd.cmd);

                match self.max_retries {
                    Some(max_retries) if self.retry == max_retries => {
                        self.respond(Err(err));
                        return Ok(Async::Ready(Next::Done));
                    }
                    _ => (),
                }
                self.retry = self.retry.saturating_add(1);

                if err.kind() == ErrorKind::ExtensionError {
                    let error_code = err.extension_error_code().unwrap();

                    if error_code == "MOVED" || error_code == "ASK" {
                        // Refresh slots and request again.
                        self.info.excludes.clear();
                        return Err(err);
                    } else if error_code == "TRYAGAIN" || error_code == "CLUSTERDOWN" {
                        // Sleep and retry.
                        let sleep_duration =
                            Duration::from_millis(2u64.pow(self.retry.max(7).min(16)) * 10);
                        self.info.excludes.clear();
                        return Ok(Async::Ready(Next::Delay(sleep_duration)));
                    }
                }

                self.info.excludes.insert(addr);

                if self.info.excludes.len() >= connections_len {
                    self.respond(Err(err));
                    return Ok(Async::Ready(Next::Done));
                }

                Ok(Async::Ready(Next::TryNewConnection))
            }
            Err(void) => match void {},
        }
    }

    fn respond(&mut self, msg: RedisResult<I>) {
        // If `send` errors the receiver has dropped and thus does not care about the message
        let _ = self
            .sender
            .take()
            .expect("Result should only be sent once")
            .send(msg);
    }
}

impl<C> Pipeline<C>
where
    C: ConnectionLike + Connect + Clone + Send + 'static,
{
    fn new(initial_nodes: Vec<ConnectionInfo>, retries: Option<u32>) -> impl ImplRedisFuture<Self> {
        Self::create_initial_connections(&initial_nodes).and_then(move |connections| {
            let mut connection = Some(Pipeline {
                connections,
                slots: Default::default(),
                in_flight_requests: Vec::new(),
                state: ConnectionState::PollComplete,
                retries,
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
    ) -> impl ImplRedisFuture<HashMap<String, C>> {
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
                        "Failed to create initial connections",
                    )));
                }
                Ok(connections)
            })
    }

    // Query a node to discover slot-> master mappings.
    fn refresh_slots(&mut self) -> impl ImplRedisFuture<(SlotMap, HashMap<String, C>)> {
        let slots_future = {
            let samples = self.connections.values().cloned().collect::<Vec<_>>();

            let mut found_slots = false;
            stream::iter_ok(samples)
                .and_then(|conn| get_slots(conn).and_then(Self::build_slot_map).then(Ok))
                // Query connections until we find one that
                .take_while(move |result: &RedisResult<_>| {
                    let take_this = !found_slots;
                    found_slots = result.is_ok();
                    future::ok(take_this)
                })
                // Get the last result which is either Ok or all nodes failed to return slots
                // to us
                .fold(None, |_, result| Ok::<_, RedisError>(Some(result)))
                .and_then(move |opt| opt.expect("No connections to refresh slots from"))
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

    fn build_slot_map(mut slots_data: Vec<Slot>) -> RedisResult<SlotMap> {
        slots_data.sort_by_key(|slot_data| slot_data.start);
        let last_slot = slots_data.iter().try_fold(0, |prev_end, slot_data| {
            if prev_end != slot_data.start() {
                return Err(RedisError::from((
                    ErrorKind::ResponseError,
                    "Slot refresh error.",
                    format!(
                        "Received overlapping slots {} and {}..{}",
                        prev_end, slot_data.start, slot_data.end
                    ),
                )));
            }
            Ok(slot_data.end() + 1)
        })?;

        if usize::from(last_slot) != SLOT_SIZE {
            return Err(RedisError::from((
                ErrorKind::ResponseError,
                "Slot refresh error.",
                format!("Lacks the slots >= {}", last_slot),
            )));
        }
        let slot_map = slots_data
            .iter()
            .map(|slot_data| (slot_data.end(), slot_data.master().to_string()))
            .collect();
        trace!("{:?}", slot_map);
        Ok(slot_map)
    }

    fn get_connection(&self, slot: u16) -> impl Future<Item = (String, C), Error = Void> + 'static {
        if let Some((_, addr)) = self.slots.range(&slot..).next() {
            if self.connections.contains_key(addr) {
                return future::Either::A(future::ok((
                    addr.clone(),
                    self.connections.get(addr).unwrap().clone(),
                )));
            }

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
        &self,
        info: &RequestInfo<C>,
    ) -> impl Future<Item = (String, RedisResult<Response>), Error = Void> {
        // TODO remove clone by changing the ConnectionLike trait
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

impl<C> Sink for Pipeline<C>
where
    C: ConnectionLike + Connect + Clone + Send + 'static,
{
    type SinkItem = Message<C>;
    type SinkError = ();

    fn start_send(&mut self, msg: Message<C>) -> StartSend<Message<C>, Self::SinkError> {
        trace!("start_send");
        let cmd = msg.cmd;

        let excludes = HashSet::new();
        let slot = slot_for_packed_command(&cmd.cmd);

        let info = RequestInfo {
            cmd,
            func: msg.func,
            slot,
            excludes,
        };
        let request = Request {
            max_retries: self.retries,
            retry: 0,
            sender: Some(msg.sender),
            future: RequestState::None,
            info,
        };
        self.in_flight_requests.push(request);
        Ok(AsyncSink::Ready)
    }

    fn poll_complete(&mut self) -> Poll<(), Self::SinkError> {
        trace!("poll_complete: {:?}", self.state);
        loop {
            self.state = match mem::replace(&mut self.state, ConnectionState::PollComplete) {
                ConnectionState::Recover(mut future) => match future.poll() {
                    Ok(Async::Ready((slots, connections))) => {
                        trace!("Recovered with {} connections!", connections.len());
                        self.slots = slots;
                        self.connections = connections;
                        ConnectionState::PollComplete
                    }
                    Ok(Async::NotReady) => {
                        self.state = ConnectionState::Recover(future);
                        trace!("Recover not ready");
                        return Ok(Async::NotReady);
                    }
                    Err(_err) => ConnectionState::Recover(Box::new(self.refresh_slots())),
                },
                ConnectionState::PollComplete => {
                    let mut error = None;
                    let mut i = 0;

                    while i < self.in_flight_requests.len() {
                        if let RequestState::None = self.in_flight_requests[i].future {
                            let future = self.try_request(&self.in_flight_requests[i].info);
                            self.in_flight_requests[i].future =
                                RequestState::Future(Box::new(future));
                        }

                        match self.in_flight_requests[i].poll_request(self.connections.len()) {
                            Ok(Async::NotReady) => {
                                i += 1;
                            }
                            Ok(Async::Ready(next)) => match next {
                                Next::Delay(duration) => {
                                    let mut request = self.in_flight_requests.swap_remove(i);
                                    request.future =
                                        RequestState::Delay(tokio_timer::sleep(duration));
                                    self.in_flight_requests.push(request);
                                }
                                Next::Done => {
                                    self.in_flight_requests.swap_remove(i);
                                }
                                Next::TryNewConnection => {
                                    let mut request = self.in_flight_requests.swap_remove(i);
                                    request.future = RequestState::Future(Box::new(
                                        self.try_request(&request.info),
                                    ));
                                    self.in_flight_requests.push(request);
                                }
                            },
                            Err(err) => {
                                error = Some(err);

                                self.in_flight_requests[i].future = RequestState::None;
                                i += 1;
                            }
                        }
                    }

                    if let Some(err) = error {
                        trace!("Recovering {}", err);
                        ConnectionState::Recover(Box::new(self.refresh_slots()))
                    } else if self.in_flight_requests.is_empty() {
                        return Ok(Async::Ready(()));
                    } else {
                        return Ok(Async::NotReady);
                    }
                }
            }
        }
    }
}

impl<C> ConnectionLike for Connection<C>
where
    C: ConnectionLike + 'static,
{
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
                                .map(|(conn, value)| (conn, Response::Single(value))),
                        )
                    },
                })
                .map_err(|_| {
                    RedisError::from(io::Error::new(
                        io::ErrorKind::BrokenPipe,
                        "redis_cluster: Unable to send command",
                    ))
                })
                .and_then(move |_| {
                    receiver.then(|result| {
                        result
                            .unwrap_or_else(|_| {
                                Err(RedisError::from(io::Error::new(
                                    io::ErrorKind::BrokenPipe,
                                    "redis_cluster: Unable to receive command",
                                )))
                            })
                            .map(|response| {
                                (
                                    self,
                                    match response {
                                        Response::Single(value) => value,
                                        Response::Multiple(_) => unreachable!(),
                                    },
                                )
                            })
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
                    func: |conn, cmd| {
                        Box::new(
                            conn.req_packed_commands(cmd.cmd, cmd.offset, cmd.count)
                                .map(|(conn, values)| (conn, Response::Multiple(values))),
                        )
                    },
                })
                .map_err(|_| RedisError::from(io::Error::from(io::ErrorKind::BrokenPipe)))
                .and_then(move |_| {
                    receiver.then(|result| {
                        result
                            .unwrap_or_else(|_| {
                                Err(RedisError::from(io::Error::from(io::ErrorKind::BrokenPipe)))
                            })
                            .map(|response| {
                                (
                                    self,
                                    match response {
                                        Response::Multiple(values) => values,
                                        Response::Single(_) => unreachable!(),
                                    },
                                )
                            })
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

trait ImplRedisFuture<T>: Future<Item = T, Error = RedisError> {}
impl<T, F> ImplRedisFuture<T> for F where F: Future<Item = T, Error = RedisError> {}

pub trait Connect {
    fn connect<T>(info: T) -> RedisFuture<Self>
    where
        T: IntoConnectionInfo;
}

impl Connect for redis::aio::SharedConnection {
    fn connect<T>(info: T) -> RedisFuture<redis::aio::SharedConnection>
    where
        T: IntoConnectionInfo,
    {
        let connection_info = try_future!(info.into_connection_info());
        let client = try_future!(redis::Client::open(connection_info));
        Box::new(client.get_shared_async_connection())
    }
}

fn connect_and_check<T, C>(info: T) -> RedisFuture<C>
where
    T: IntoConnectionInfo,
    C: ConnectionLike + Connect + Send + 'static,
{
    Box::new(C::connect(info).and_then(|conn| check_connection(conn)))
}

fn check_connection<C>(conn: C) -> impl ImplRedisFuture<C>
where
    C: ConnectionLike + Send + 'static,
{
    let mut cmd = Cmd::new();
    cmd.arg("PING");
    cmd.query_async::<_, String>(conn).map(|(conn, _)| conn)
}

fn get_random_connection<'a, C>(
    connections: &'a HashMap<String, C>,
    excludes: Option<&'a HashSet<String>>,
) -> (String, C)
where
    C: Clone,
{
    debug_assert!(!connections.is_empty());

    let mut rng = thread_rng();
    let sample = match excludes {
        Some(excludes) if excludes.len() < connections.len() => {
            let target_keys = connections.keys().filter(|key| !excludes.contains(*key));
            target_keys.choose(&mut rng)
        }
        _ => connections.keys().choose(&mut rng),
    };

    let addr = sample.expect("No targets to choose from");
    (addr.to_string(), connections.get(addr).unwrap().clone())
}

fn slot_for_packed_command(cmd: &[u8]) -> Option<u16> {
    command_key(cmd).map(|key| {
        let key = sub_key(&key);
        State::<XMODEM>::calculate(&key) % SLOT_SIZE as u16
    })
}

fn command_key(cmd: &[u8]) -> Option<Vec<u8>> {
    // TODO Avoid parsing the entire request to a `redis::Value`
    redis::parse_redis_value(cmd)
        .ok()
        .and_then(|value| match value {
            Value::Bulk(mut args) => {
                if args.len() >= 2 {
                    match args.swap_remove(1) {
                        Value::Data(key) => Some(key),
                        _ => None,
                    }
                } else {
                    None
                }
            }
            _ => None,
        })
}

// If a key contains `{` and `}`, everything between the first occurence is the only thing that
// determines the hash slot
fn sub_key(key: &[u8]) -> &[u8] {
    key.iter()
        .position(|b| *b == b'{')
        .and_then(|open| {
            key[open + 1..]
                .iter()
                .position(|b| *b == b'}')
                .and_then(|close| {
                    if close != 0 {
                        Some(&key[open + 1..open + close + 1])
                    } else {
                        None
                    }
                })
        })
        .unwrap_or(key)
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
fn get_slots<C>(connection: C) -> impl Future<Item = Vec<Slot>, Error = RedisError>
where
    C: ConnectionLike,
{
    trace!("get_slots");
    let mut cmd = Cmd::new();
    cmd.arg("CLUSTER").arg("SLOTS");
    let packed_command = cmd.get_packed_command();
    connection
        .req_packed_command(packed_command)
        .map_err(|err| {
            trace!("get_slots error: {}", err);
            err
        })
        .and_then(|(_connection, value)| {
            trace!("get_slots -> {:#?}", value);
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
                    });
                }
            }

            Ok(result)
        })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn slot() {
        assert_eq!(
            slot_for_packed_command(&[
                42, 50, 13, 10, 36, 54, 13, 10, 69, 88, 73, 83, 84, 83, 13, 10, 36, 49, 54, 13, 10,
                244, 93, 23, 40, 126, 127, 253, 33, 89, 47, 185, 204, 171, 249, 96, 139, 13, 10
            ]),
            Some(964)
        );
        assert_eq!(
            slot_for_packed_command(&[
                42, 54, 13, 10, 36, 51, 13, 10, 83, 69, 84, 13, 10, 36, 49, 54, 13, 10, 36, 241,
                197, 111, 180, 254, 5, 175, 143, 146, 171, 39, 172, 23, 164, 145, 13, 10, 36, 52,
                13, 10, 116, 114, 117, 101, 13, 10, 36, 50, 13, 10, 78, 88, 13, 10, 36, 50, 13, 10,
                80, 88, 13, 10, 36, 55, 13, 10, 49, 56, 48, 48, 48, 48, 48, 13, 10
            ]),
            Some(8352)
        );

        assert_eq!(
            slot_for_packed_command(&[
                42, 54, 13, 10, 36, 51, 13, 10, 83, 69, 84, 13, 10, 36, 49, 54, 13, 10, 169, 233,
                247, 59, 50, 247, 100, 232, 123, 140, 2, 101, 125, 221, 66, 170, 13, 10, 36, 52,
                13, 10, 116, 114, 117, 101, 13, 10, 36, 50, 13, 10, 78, 88, 13, 10, 36, 50, 13, 10,
                80, 88, 13, 10, 36, 55, 13, 10, 49, 56, 48, 48, 48, 48, 48, 13, 10
            ]),
            Some(5210),
        );
    }
}
