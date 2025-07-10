// Copyright 2021 Datafuse Labs
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

//! Defines the read-only map API interface.
//!
//! The [`MapApiRO`] trait provides read-only operations for key-value storage,
//! including getting a single value by key and iterating over a range of keys.
//! This trait is designed to be thread-safe and can be used in concurrent contexts.

use std::io;
use std::ops::RangeBounds;

use crate::map_key::MapKey;
use crate::KVResultStream;
use crate::MarkedOf;

/// Provides a read-only key-value map API.
///
/// This trait defines the core read operations for a key-value store:
/// - Getting a single value by key
/// - Iterating over a range of key-value pairs
///
/// The trait is designed to be thread-safe (`Send + Sync`) and works with
/// generic key types that implement the [`MapKey`] trait.
///
/// # Type Parameters
///
/// - `K`: The key type, which must implement [`MapKey<M>`]
/// - `M`: The metadata type associated with values
///
/// # Examples
///
/// ```rust,no_run
/// use std::io;
///
/// use map_api::impls::level::Level;
/// use map_api::MapApiRO;
///
/// #[tokio::main]
/// async fn main() -> io::Result<()> {
///     let map = Level::<()>::default();
///
///     // Get a value by key
///     let value = map.get(&"example_key".to_string()).await?;
///
///     // Iterate over a range of keys
///     let range = map.range("a".to_string().."z".to_string()).await?;
///
///     Ok(())
/// }
/// ```
#[async_trait::async_trait]
pub trait MapApiRO<K, M>: Send + Sync
where K: MapKey<M>
{
    // The following does not work, because MapKeyEncode is defined in the application crate,
    // But using `Q` in the defining crate requires `MapKeyEncode`.
    // Because the application crate can not add more constraints to `Q`.
    // async fn get<Q>(&self, key: &Q) -> Result<MarkedOf<K>, io::Error>
    // where
    //     K: Borrow<Q>,
    //     Q: Ord + Send + Sync + ?Sized,
    //     Q: MapKeyEncode;

    /// Get an entry by key.
    ///
    /// Retrieves the value associated with the given key. If the key doesn't exist
    /// or has been deleted (tombstone), a special empty value is returned.
    ///
    /// # Parameters
    ///
    /// - `key`: The key to look up
    ///
    /// # Returns
    ///
    /// - `Ok(Marked::Normal { ... })`: If the key exists and has a value
    /// - `Ok(Marked::TombStone { ... })`: If the key has been deleted (tombstone)
    /// - `Ok(Marked::empty())`: If the key doesn't exist
    /// - `Err(io::Error)`: If an I/O error occurred during the operation
    async fn get(&self, key: &K) -> Result<MarkedOf<K, M>, io::Error>;

    /// Iterate over a range of entries by keys.
    ///
    /// Returns a stream of key-value pairs within the specified range. The stream
    /// includes both normal values and tombstone entries (deleted keys).
    ///
    /// # Parameters
    ///
    /// - `range`: A range of keys to iterate over
    ///
    /// # Returns
    ///
    /// - `Ok(stream)`: A stream of key-value pairs within the range
    /// - `Err(io::Error)`: If an I/O error occurred during the operation
    ///
    /// # Notes
    ///
    /// The returned stream includes tombstone entries ([`Marked::TombStone`](crate::marked::SeqMarked::TombStone)),
    /// which represent keys that have been deleted. This is useful for replication and
    /// synchronization purposes.
    async fn range<R>(&self, range: R) -> Result<KVResultStream<K, M>, io::Error>
    where R: RangeBounds<K> + Send + Sync + Clone + 'static;
}

#[async_trait::async_trait]
impl<K, M, T> MapApiRO<K, M> for &T
where
    T: MapApiRO<K, M>,
    K: MapKey<M>,
{
    async fn get(&self, key: &K) -> Result<MarkedOf<K, M>, io::Error> {
        (**self).get(key).await
    }

    async fn range<R>(&self, range: R) -> Result<KVResultStream<K, M>, io::Error>
    where R: RangeBounds<K> + Send + Sync + Clone + 'static {
        (**self).range(range).await
    }
}

mod impls {
    use std::io;
    use std::ops::RangeBounds;

    use futures_util::StreamExt;

    use crate::map_api_ro::MapApiRO;
    use crate::map_key::MapKey;
    use crate::marked::SeqMarked;
    use crate::KVResultStream;

    /// Dummy implementation of [`MapApiRO`] for `()`.
    /// So that () can be used as a placeholder where a [`MapApiRO`] is expected.
    #[async_trait::async_trait]
    impl<K, M> MapApiRO<K, M> for ()
    where
        K: MapKey<M>,
        M: Send + 'static,
    {
        async fn get(&self, _key: &K) -> Result<SeqMarked<M, K::V>, io::Error> {
            Ok(SeqMarked::empty())
        }

        async fn range<R>(&self, _range: R) -> Result<KVResultStream<K, M>, io::Error>
        where R: RangeBounds<K> + Send + Sync + Clone + 'static {
            Ok(futures::stream::iter([]).boxed())
        }
    }
}
