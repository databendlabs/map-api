# map-api

A map-like API with set, get and range operations, designed for use in Raft state machines.

## Features

- Async map-like interface with `set`, `get`, and `range` operations
- Support for key-value storage with sequence numbers
- Expirable entries
- Compact storage format
- Thread-safe read operations
- Marked entries support with tombstone support
- Generic key-value types with metadata support
- Multi-level storage with compaction support

## Usage

Add this to your `Cargo.toml`:

```toml
[dependencies]
map-api = "0.1.0"
```

Basic usage example:

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

## API Details

The API is designed to be used in Raft state machines and provides:

- `MapApiRO`: A read-only trait for thread-safe read operations
- `MapApi`: A read-write trait that extends `MapApiRO`
- Support for tombstone entries in range scans
- Generic key-value types with associated metadata
- Async operations for better performance
- Multi-level storage with compaction support
- Support for sequence numbers in entries

## License

This project is licensed under the Apache-2.0 License - see the [LICENSE](LICENSE) file for details.

## Contributing

Contributions are welcome! Please feel free to submit a Pull Request.


