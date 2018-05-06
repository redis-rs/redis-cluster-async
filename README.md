This is a Rust implementation for Redis cluster library.

Documentation is available at [here](https://docs.rs/redis_cluster_rs/0.1.0/redis_cluster_rs/).

This library extends redis-rs library to be able to use cluster.
Client impletemts traits of ConnectionLike and Commands.
So you can use redis-rs's access methods.
If you want more information, read document of redis-rs.

Note that this library is currently not have features of Pipeline and Pubsub.

# Example

```rust
extern crate redis_cluster_rs;

use redis_cluster_rs::{Client, Connection, Commands};

fn main() {
    let nodes = vec!["redis://127.0.0.1:6379/", "redis://127.0.0.1:6378/", "redis://127.0.0.1:6377/"];
    let client = Client::open(nodes).unwrap();
    let connection = client.get_connection().unwrap();

    let _: () = connection.set("test", "test_data").unwrap();
    let res: String = connection.get("test").unwrap();

    assert_eq!(res, "test_data");
}
```
