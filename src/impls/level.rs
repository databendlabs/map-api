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

//! Provides a simple in-memory implementation of the Map API.
//!
//! The [`Level`] struct is a basic implementation of the [`MapApi`] trait
//! that stores key-value pairs in memory using a [`BTreeMap`]. It's primarily
//! intended for testing and demonstration purposes.

use std::collections::BTreeMap;
use std::io;
use std::ops::RangeBounds;

use futures_util::StreamExt;
use log::warn;

use crate::KVResultStream;
use crate::MapApi;
use crate::MapApiRO;
use crate::MapKey;
use crate::Marked;
use crate::MarkedOf;
use crate::Transition;

/// A simple in-memory implementation of the Map API using a BTreeMap.
///
/// This implementation stores key-value pairs in memory and maintains a sequence
/// number for versioning. It's primarily intended for testing and demonstration
/// purposes, not for production use.
///
/// # Type Parameters
///
/// - `M`: The metadata type associated with values, defaults to `()`
///
/// # Implementation Details
///
/// The `Level` struct contains:
/// - A sequence counter (`u64`) for assigning sequence numbers to entries
/// - A [`BTreeMap`] for storing key-value pairs
///
/// # Examples
///
/// ```
/// use std::io;
///
/// use map_api::impls::level::Level;
/// use map_api::MapApi;
/// use map_api::MapApiRO;
///
/// #[tokio::main]
/// async fn main() -> io::Result<()> {
///     // Create a new Level instance
///     let mut map = Level::<()>::default();
///
///     // Set a value
///     map.set(
///         "key1".to_string(),
///         Some(("value1".as_bytes().to_vec(), None)),
///     )
///     .await?;
///
///     // Get the value
///     let value = map.get(&"key1".to_string()).await?;
///
///     Ok(())
/// }
/// ```
#[derive(Debug, Clone, Default)]
pub struct Level<M = ()>(u64, BTreeMap<String, Marked<M>>);

impl<M> Level<M> {
    // Only used in tests
    #[allow(dead_code)]
    pub(crate) fn new_level(&self) -> Self {
        Self(self.0, Default::default())
    }
}

#[async_trait::async_trait]
impl<M> MapApiRO<String, M> for Level<M>
where M: Clone + Send + Sync + 'static
{
    /// Get a value by key.
    ///
    /// Retrieves the value associated with the given key from the in-memory store.
    /// If the key doesn't exist, returns an empty `Marked` value.
    async fn get(&self, key: &String) -> Result<MarkedOf<String, M>, io::Error> {
        let got = self.1.get(key).cloned().unwrap_or(Marked::empty());
        Ok(got)
    }

    async fn range<R>(&self, range: R) -> Result<KVResultStream<String, M>, io::Error>
    where R: RangeBounds<String> + Clone + Send + Sync + 'static {
        // Level is borrowed. It has to copy the result to make the returning stream static.
        let vec = self
            .1
            .range(range)
            .map(|(k, v)| (k.clone(), v.clone()))
            .collect::<Vec<_>>();

        if vec.len() > 1000 {
            warn!(
                "Level::<String>::range() returns big range of len={}",
                vec.len()
            );
        }

        let strm = futures::stream::iter(vec).map(Ok).boxed();
        Ok(strm)
    }
}

#[async_trait::async_trait]
impl<M> MapApi<String, M> for Level<M>
where M: Clone + Unpin + Send + Sync + 'static
{
    async fn set(
        &mut self,
        key: String,
        value: Option<(<String as MapKey<M>>::V, Option<M>)>,
    ) -> Result<Transition<MarkedOf<String, M>>, io::Error> {
        // The chance it is the bottom level is very low in a loaded system.
        // Thus, we always tombstone the key if it is None.

        let marked = if let Some((v, meta)) = value {
            self.0 += 1;
            let seq = self.0;
            Marked::new_with_meta(seq, v, meta)
        } else {
            // Do not increase the sequence number, just use the max seq for all tombstone.
            let seq = self.0;
            Marked::new_tombstone(seq)
        };

        let prev = self.1.get(&key).cloned().unwrap_or(Marked::empty());
        self.1.insert(key, marked.clone());
        Ok((prev, marked))
    }
}
