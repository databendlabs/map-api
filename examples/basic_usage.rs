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
