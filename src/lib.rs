//! This is a rust implementation for Redis cluster library.
//!
//! This library extends redis-rs library to be able to use cluster.
//! Client impletemts traits of ConnectionLike and Commands.
//! So you can use redis-rs's access methods.
//! If you want more information, read document of redis-rs.
//!
//! Note that this library is currently not have features of Pipeline and Pubsub.
//!
//! # Example
//! ```rust,no_run
//! extern crate redis_cluster_rs;
//!
//! use redis_cluster_rs::{Client, Connection, Commands};
//!
//! fn main() {
//!     let nodes = vec!["redis://127.0.0.1:6379/", "redis://127.0.0.1:6378/", "redis://127.0.0.1:6377/"];
//!     let client = Client::open(nodes).unwrap();
//!     let connection = client.get_connection().unwrap();
//!
//!     let _: () = connection.set("test", "test_data").unwrap();
//!     let res: String = connection.get("test").unwrap();
//!
//!     assert_eq!(res, "test_data");
//! }
//! ```
extern crate crc16;
extern crate rand;
extern crate redis;

use std::cell::RefCell;
use std::collections::{HashMap, HashSet};
use std::io::{BufRead, Cursor};
use std::iter::Iterator;
use std::thread;
use std::time;

use crc16::*;
use rand::thread_rng;
use rand::seq::sample_iter;
use redis::{ConnectionAddr, ConnectionInfo, IntoConnectionInfo, RedisError, Value, Cmd, ErrorKind};

pub use redis::{ConnectionLike, Commands, RedisResult};

const SLOT_SIZE: usize = 16384;

/// This is a Redis cluster client.
pub struct Client {
    initial_nodes: Vec<ConnectionInfo>
}

impl Client {
    /// Connect to a redis cluster server and return a cluster client.
    pub fn open<T: IntoConnectionInfo>(initial_nodes: Vec<T>) -> RedisResult<Client> {
        let mut nodes = Vec::with_capacity(initial_nodes.len());

        for info in initial_nodes {
            let info = info.into_connection_info()?;
            if let ConnectionAddr::Unix(_) = *info.addr {
                return Err(RedisError::from((ErrorKind::InvalidClientConfig,
                                             "This library cannot use unix socket because Redis's cluster command returns only cluster's IP and port.")))
            }
            nodes.push(info);
        }

        Ok(Client {initial_nodes: nodes})
    }

    /// Get a Redis cluster connection.
    pub fn get_connection(&self) -> RedisResult<Connection> {
        let mut connections = HashMap::with_capacity(self.initial_nodes.len());

        for info in self.initial_nodes.iter() {
            let addr = match *info.addr {
                ConnectionAddr::Tcp(ref host, port) => format!("{}:{}", host, port),
                _ => panic!("No reach.")
            };

            let conn = connect(info.clone())?;
            if !check_connection(&conn) {
                return Err(RedisError::from((ErrorKind::IoError, "It is failed to check startup nodes.")));
            }
            connections.insert(addr, conn);
        }

        let connection = Connection {
            connections: RefCell::new(connections),
            slots: RefCell::new(HashMap::with_capacity(SLOT_SIZE))
        };
        connection.refresh_slots()?;

        Ok(connection)
    }
}

/// This is a connection of Redis cluster.
pub struct Connection {
    connections: RefCell<HashMap<String, redis::Connection>>,
    slots: RefCell<HashMap<u16, String>>
}

impl Connection {
    /// Check that all connections it has are available.
    pub fn check_connection(&self) -> bool {
        let connections = self.connections.borrow();
        for conn in connections.values() {
            if !check_connection(&conn) {
                return false;
            }
        }
        true
    }

    // Query a node to discover slot-> master mappings.
    fn refresh_slots(&self) -> RedisResult<()> {
        let mut connections = self.connections.borrow_mut();
        let mut slots = self.slots.borrow_mut();

        *slots = {
            let mut new_slots = HashMap::with_capacity(slots.len());
            let mut rng = thread_rng();
            let samples = sample_iter(&mut rng, connections.values(), connections.len()).ok().unwrap();

            for conn in samples {
                if let Ok(slots_data) = get_slots(&conn) {
                    for slot_data in slots_data {
                        for slot in slot_data.start()..slot_data.end() + 1 {
                            new_slots.insert(slot, slot_data.master().to_string());
                        }
                    }
                    break;
                }
            }

            if new_slots.len() != SLOT_SIZE {
                println!("{}", new_slots.len());
                return Err(RedisError::from((ErrorKind::ResponseError, "Slot refresh error.")));
            }
            new_slots
        };

        *connections = {
            // Remove dead connections and connect to new nodes if necessary
            let mut new_connections = HashMap::with_capacity(connections.len());

            for addr in slots.values() {
                if !new_connections.contains_key(addr) {
                    if connections.contains_key(addr) {
                        let conn = connections.remove(addr).unwrap();
                        if check_connection(&conn) {
                            new_connections.insert(addr.to_string(), conn);
                            continue;
                        }
                    }

                    if let Ok(conn) = connect(addr.as_ref()) {
                        if check_connection(&conn) {
                            new_connections.insert(addr.to_string(), conn);
                        }
                    }
                }
            }
            new_connections
        };

        Ok(())
    }

    fn get_connection<'a>(&self, connections: &'a mut HashMap<String, redis::Connection>, slot: u16) -> (String, &'a redis::Connection) {
        let slots = self.slots.borrow();

        if let Some(addr) = slots.get(&slot) {
            if connections.contains_key(addr) {
                return (addr.clone(), connections.get(addr).unwrap());
            }

            // Create new connection.
            if let Ok(conn) = connect(addr.as_ref()) {
                if check_connection(&conn) {
                    return (addr.to_string(), connections.entry(addr.to_string()).or_insert(conn));
                }
            }
        }

        // Return a random connection
        get_random_connection(connections, None)
    }

    fn request<T, F>(&self, cmd: &[u8], func: F) -> RedisResult<T>
        where F: Fn(&redis::Connection) -> RedisResult<T> {

        let mut retries = 16;
        let mut excludes = HashSet::new();
        let slot = slot_for_packed_command(cmd);

        loop {
            // Get target address and response.
            let (addr, res) = {
                let mut connections = self.connections.borrow_mut();
                let (addr, conn) = if excludes.len() > 0 || slot.is_none() {
                    get_random_connection(&*connections, Some(&excludes))
                } else {
                    self.get_connection(&mut *connections, slot.unwrap())
                };

                (addr, func(conn))
            };

            match res {
                Ok(res) => return Ok(res),
                Err(err) => {
                    retries -= 1;
                    if retries <= 0 {
                        return Err(err);
                    }

                    if err.kind() == ErrorKind::ExtensionError {
                        let error_code = err.extension_error_code().unwrap();

                        if error_code == "MOVED" || error_code == "ASK" {
                            // Refresh slots and request again.
                            self.refresh_slots()?;
                            excludes.clear();
                            continue;

                        } else if error_code == "TRYAGAIN" || error_code == "CLUSTERDOWN" {
                            // Sleep and retry.
                            let sleep_time = 2u64.pow(16 - retries.max(9)) * 10;
                            thread::sleep(time::Duration::from_millis(sleep_time));
                            excludes.clear();
                            continue;
                        }
                    }

                    excludes.insert(addr);

                    let connections = self.connections.borrow();
                    if excludes.len() >= connections.len() {
                        return Err(err);
                    }
                }
            }
        }
    }
}

impl ConnectionLike for Connection {
    fn req_packed_command(&self, cmd: &[u8]) -> RedisResult<Value> {
        self.request(cmd, move |conn| conn.req_packed_command(cmd))
    }

    fn req_packed_commands(&self, cmd: &[u8], offset: usize, count: usize) -> RedisResult<Vec<Value>> {
        self.request(cmd, move |conn| conn.req_packed_commands(cmd, offset, count))
    }

    fn get_db(&self) -> i64 {
        0
    }
}

impl Commands for Connection {}

impl Clone for Client {
    fn clone(&self) -> Client {
        Client::open(self.initial_nodes.clone()).unwrap()
    }
}

fn connect<T: IntoConnectionInfo>(info: T) -> RedisResult<redis::Connection> {
    let connection_info = info.into_connection_info()?;
    let client = redis::Client::open(connection_info)?;
    client.get_connection()
}

fn check_connection(conn: &redis::Connection) -> bool {
    let mut cmd = Cmd::new();
    cmd.arg("PING");
    match cmd.query::<String>(conn) {
        Ok(_) => true,
        Err(_) => false,
    }
}

fn get_random_connection<'a>(connections: &'a HashMap<String, redis::Connection>, excludes: Option<&'a HashSet<String>>) -> (String, &'a redis::Connection) {
    let mut rng = thread_rng();
    let samples = match excludes {
        Some(excludes) if excludes.len() < connections.len() => {
            let target_keys = connections.keys().filter(|key| !excludes.contains(*key));
            sample_iter(&mut rng, target_keys, 1).unwrap()
        },
        _ => sample_iter(&mut rng, connections.keys(), 1).unwrap()
    };

    let addr = samples.first().unwrap();
    (addr.to_string(), connections.get(*addr).unwrap())
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
fn get_slots(connection: &redis::Connection) -> RedisResult<Vec<Slot>> {
    let mut cmd = Cmd::new();
    cmd.arg("CLUSTER").arg("SLOTS");
    let packed_command = cmd.get_packed_command();
    let value = connection.req_packed_command(&packed_command)?;

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

            let mut nodes: Vec<String> = item.into_iter()
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
                        Some(format!("{}:{}", ip, port))
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
                replicas
            });;
        }
    }

    Ok(result)
}
