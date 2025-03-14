# map-api

A map-like API with set, get and range operations, designed for use in Raft state machines.

## Overview

This library provides a consistent interface for key-value storage with additional features such as sequence numbers, expirable entries, and tombstone support for deleted entries. It's particularly useful for implementing distributed systems that require reliable state management.

## Features

- **Async map-like interface** with `set`, `get`, and `range` operations
- **Versioned entries** with sequence numbers for tracking changes
- **Tombstone support** for tracking deleted entries
- **Expirable entries** with automatic expiration
- **Thread-safe read operations** for concurrent access
- **Generic key-value types** with metadata support
- **Compact storage format** for efficient serialization
- **Multi-level storage** with compaction support

## Installation

Add this to your `Cargo.toml`:

```toml
[dependencies]
map-api = "0.1.0"
```

## Basic Usage

```rust
use std::io;

use futures_util::StreamExt;
use map_api::impls::level::Level;
use map_api::MapApi;
use map_api::MapApiRO;
use map_api::Marked;

#[tokio::main]
async fn main() -> io::Result<()> {
    // Create a map instance using Level implementation with () as metadata type
    let mut map = Level::<()>::default();

    // Set a value
    map.set(
        "key1".to_string(),
        Some(("value1".as_bytes().to_vec(), None)),
    )
    .await?;

    // Set another value
    map.set(
        "key2".to_string(),
        Some(("value2".as_bytes().to_vec(), None)),
    )
    .await?;

    // Delete a key by setting it to None (tombstone)
    map.set("key1".to_string(), None).await?;

    // Get a value
    let value = map.get(&"key2".to_string()).await?;
    if !value.is_not_found() {
        println!("Found value for key2: {:?}", value);
    }

    // Range scan
    let mut range = map.range("".to_string()..).await?;
    while let Some(result) = range.next().await {
        let (key, value) = result?;
        match value {
            Marked::Normal { value, .. } => println!("{}: {:?}", key, value),
            Marked::TombStone { .. } => println!("{}: <deleted>", key),
        }
    }

    Ok(())
}
```

## Core Components

### MapApiRO

The `MapApiRO` trait provides read-only operations for key-value storage:

```rust
#[async_trait::async_trait]
pub trait MapApiRO<K, M>: Send + Sync
where K: MapKey<M>
{
    // Get a value by key
    async fn get(&self, key: &K) -> Result<MarkedOf<K, M>, io::Error>;

    // Iterate over a range of key-value pairs
    async fn range<R>(&self, range: R) -> Result<KVResultStream<K, M>, io::Error>
    where R: RangeBounds<K> + Send + Sync + Clone + 'static;
}
```

### MapApi

The `MapApi` trait extends `MapApiRO` to add write operations:

```rust
#[async_trait::async_trait]
pub trait MapApi<K, M>: MapApiRO<K, M>
where
    K: MapKey<M>,
    M: Unpin,
{
    // Set a value and return the transition (old value, new value)
    async fn set(
        &mut self,
        key: K,
        value: Option<(K::V, Option<M>)>,
    ) -> Result<Transition<MarkedOf<K, M>>, io::Error>;
}
```

### Marked

The `Marked` enum represents values that can be either normal or tombstones (deleted):

```rust
pub enum Marked<M, T = Vec<u8>> {
    // Represents a deleted value
    TombStone {
        internal_seq: u64,
    },
    // Represents a normal value
    Normal {
        internal_seq: u64,
        value: T,
        meta: Option<M>,
    },
}
```

### Implementations

The library provides several implementations of the `MapApi` trait:

- `Level`: A simple in-memory implementation using a `BTreeMap`
- `Immutable`: An immutable view of a `MapApiRO` implementation

## Advanced Usage

### Working with Metadata

You can associate metadata with values by specifying a metadata type:

```rust
// Define a custom metadata type
#[derive(Clone, Debug)]
struct MyMeta {
    created_at: u64,
    owner: String,
}

// Create a map with the custom metadata type
let mut map = Level::<MyMeta>::default();

// Set a value with metadata
map.set(
    "key1".to_string(),
    Some((
        "value1".as_bytes().to_vec(),
        Some(MyMeta {
            created_at: 1234567890,
            owner: "user1".to_string(),
        }),
    )),
)
.await?;
```

### Handling Tombstones

Tombstones represent deleted entries and are included in range scans:

```rust
let mut range = map.range("".to_string()..).await?;
while let Some(result) = range.next().await {
    let (key, value) = result?;
    match value {
        Marked::Normal { value, meta, .. } => {
            println!("Key: {}, Value: {:?}, Meta: {:?}", key, value, meta);
        }
        Marked::TombStone { internal_seq } => {
            println!("Key: {} was deleted (seq: {})", key, internal_seq);
        }
    }
}
```

## Use Cases

This library is particularly useful for:

- Implementing distributed state machines in Raft clusters
- Building replicated key-value stores
- Creating systems that need to track both current and deleted entries
- Implementing multi-level storage with compaction

## License

This project is licensed under the Apache-2.0 License - see the [LICENSE](LICENSE) file for details.

## Contributing

Contributions are welcome! Please feel free to submit a Pull Request.


