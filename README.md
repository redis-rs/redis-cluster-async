[![MIT licensed](https://img.shields.io/badge/license-MIT-blue.svg)](./LICENSE) [![](http://meritbadge.herokuapp.com/redis_cluster_async)](https://crates.io/crates/redis_cluster_async)

A Rust crate implementing a [Redis cluster](https://redis.io/topics/cluster-tutorial) client.

Documentation is available at [here](https://docs.rs/redis_cluster_async/*/redis_cluster_async/).

This library builds upon the [redis-rs](https://github.com/mitsuhiko/redis-rs) crate to enable working with a Redis cluster (instead of single Redis nodes).

# Example

```rust
extern crate futures;
extern crate tokio;
use futures::prelude::*;
use redis_cluster_async::{Client, redis::cmd};

fn main() {
    let nodes = vec!["redis://127.0.0.1:6379/", "redis://127.0.0.1:6378/", "redis://127.0.0.1:6377/"];

    let mut runtime = tokio::runtime::Runtime::new().unwrap();

    let client = Client::open(nodes).unwrap();
    let (_, res): (_, String) = runtime.block_on(client.get_connection()
        .and_then(|connection| {
            cmd("SET").arg("test").arg("test_data").clone()
                .query_async(connection)
                .and_then(|(connection, ())| 
                    cmd("GET").arg("test")
                        .query_async(connection)
                )
        })
        )
        .unwrap();

    assert_eq!(res, "test_data");
}
```

# Pipelining

```rust
extern crate futures;
extern crate tokio;

use futures::prelude::*;
use redis_cluster_async::{Client, redis::{PipelineCommands, pipe}};

fn main() {
    let nodes = vec!["redis://127.0.0.1:6379/", "redis://127.0.0.1:6378/", "redis://127.0.0.1:6377/"];

    let mut runtime = tokio::runtime::Runtime::new().unwrap();

    let client = Client::open(nodes).unwrap();
    let _: (_, ()) = runtime.block_on(
        client.get_connection().and_then(|connection| {
            let key = "test";

            pipe()
                .rpush(key, "123").ignore()
                .ltrim(key, -10, -1).ignore()
                .expire(key, 60).ignore()
                .query_async(connection)
        }))
        .unwrap();

}
```

## Acknowledgements

This project is built upon the synchronous redis cluster implementation in https://github.com/atuk721/redis-cluster-rs .

