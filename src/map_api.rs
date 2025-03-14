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

//! Defines the read-write map API interface.
//!
//! The [`MapApi`] trait extends [`MapApiRO`] to provide write operations
//! for key-value storage. This trait is designed for use in state machines,
//! particularly in distributed systems like Raft.

use std::io;

use crate::map_api_ro::MapApiRO;
use crate::map_key::MapKey;
use crate::BeforeAfter;

/// Provides a read-write key-value map API, used to access state machine data.
///
/// This trait extends [`MapApiRO`] to add write operations to the key-value store.
/// It's designed for use in state machines, particularly in distributed systems
/// like Raft, where tracking state transitions is important.
///
/// # Type Parameters
///
/// - `K`: The key type, which must implement [`MapKey<M>`]
/// - `M`: The metadata type associated with values, which must be [`Unpin`]
///
/// # Examples
///
/// ```rust,no_run
/// use std::io;
///
/// use map_api::impls::level::Level;
/// use map_api::MapApi;
///
/// #[tokio::main]
/// async fn main() -> io::Result<()> {
///     let mut map = Level::<()>::default();
///
///     // Set a value
///     let transition = map
///         .set(
///             "example_key".to_string(),
///             Some(("example_value".as_bytes().to_vec(), None)),
///         )
///         .await?;
///
///     // Delete a value (set to None creates a tombstone)
///     let transition = map.set("example_key".to_string(), None).await?;
///
///     Ok(())
/// }
/// ```
#[async_trait::async_trait]
pub trait MapApi<K, M>: MapApiRO<K, M>
where
    K: MapKey<M>,
    M: Unpin,
{
    /// Set an entry and returns the old value and the new value.
    ///
    /// This method either creates, updates, or deletes a key-value pair:
    /// - If `value` is `Some((value, meta))`, the key is set to that value with optional metadata
    /// - If `value` is `None`, the key is marked as deleted (tombstone)
    ///
    /// # Parameters
    ///
    /// - `key`: The key to set
    /// - `value`: The value to set, or `None` to delete the key
    ///
    /// # Returns
    ///
    /// A [`BeforeAfter`] containing:
    /// - The old value (before the operation)
    /// - The new value (after the operation)
    ///
    /// # Notes
    ///
    /// This method returns both the old and new values as a transition, which is
    /// useful for tracking state changes in distributed systems.
    async fn set(
        &mut self,
        key: K,
        value: Option<(K::V, Option<M>)>,
    ) -> Result<BeforeAfter<crate::MarkedOf<K, M>>, io::Error>;
}
