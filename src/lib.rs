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

//! # Map API
//!
//! A map-like API with set, get and range operations, designed for use in Raft state machines.
//!
//! This library provides a consistent interface for key-value storage with additional features
//! such as sequence numbers, expirable entries, and tombstone support for deleted entries.
//!
//! ## Core Components
//!
//! - [`MapApiRO`]: Read-only operations for key-value access
//! - [`MapApi`]: Read-write operations extending [`MapApiRO`]
//! - [`SeqMarked`]: A wrapper for values that can represent both normal values and tombstones
//!
//! ## Usage Example
//!
//! ```rust,no_run
//! use std::io;
//!
//! use futures_util::StreamExt;
//! use map_api::impls::level::Level;
//! use map_api::MapApi;
//! use map_api::MapApiRO;
//! use map_api::SeqMarked;
//!
//! #[tokio::main]
//! async fn main() -> io::Result<()> {
//!     // Create a map instance
//!     let mut map = Level::default();
//!
//!     // Set a value
//!     map.set("key1".to_string(), Some("value1".as_bytes().to_vec()))
//!         .await?;
//!
//!     // Get a value
//!     let value = map.get(&"key1".to_string()).await?;
//!
//!     // Range scan
//!     let mut range = map.range("".to_string()..).await?;
//!     while let Some(result) = range.next().await {
//!         let (key, value) = result?;
//!         // Process key-value pairs
//!     }
//!
//!     Ok(())
//! }
//! ```

#![feature(coroutines)]

use std::io;

use futures_util::stream::BoxStream;

pub mod compact;
pub mod impls;
pub mod map_api;
pub mod map_api_ro;
pub mod map_key;
pub mod map_value;
pub mod match_seq;
pub mod mvcc;
pub mod seq_value;
pub mod util;

pub use seq_marked::Expirable;
pub use seq_marked::Marked;
pub use seq_marked::SeqMarked;
pub use seq_marked::SeqV;
pub use seq_marked::SeqValue;

pub use crate::map_api::MapApi;
pub use crate::map_api_ro::MapApiRO;
pub use crate::map_key::MapKey;
pub use crate::map_value::MapValue;

#[deprecated(since = "0.2.0", note = "Use `BeforeAfter` instead")]
pub type Transition<T> = BeforeAfter<T>;

/// Represents a transition from one state to another.
///
/// This type is a tuple containing two elements:
/// - The first element represents the state before the transition (initial state)
/// - The second element represents the state after the transition (resulting state)
///
/// It's commonly used to track changes in values or system states.
pub type BeforeAfter<T> = (T, T);

/// A boxed stream that yields `Result` of key-value pairs or an `io::Error`.
/// The stream is 'static to ensure it can live for the entire duration of the program.
pub type IOResultStream<T> = BoxStream<'static, Result<T, io::Error>>;

/// The value type of a key type.
pub type ValueOf<K> = <K as MapKey>::V;

/// A Marked value type of key type.
/// `M` represents the meta information associated with the value.
pub type SeqMarkedOf<K> = SeqMarked<<K as MapKey>::V>;
// alias, for backward compat
pub type MarkedOf<K> = SeqMarked<<K as MapKey>::V>;

/// A key-value pair used in a map.
/// `M` represents the meta information associated with the value.
pub type MapKV<K> = (K, SeqMarkedOf<K>);

/// A stream of result of key-value returned by `range()`.
/// `M` represents the meta information associated with the value.
pub type KVResultStream<K> = IOResultStream<MapKV<K>>;
