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

//! Multi-Version Concurrency Control (MVCC) for versioned key-value storage.
//!
//! Provides **Read Committed** isolation with atomic transactions and snapshot consistency.
//! Data is organized by namespaces with support for concurrent read-write operations.
//!
//! # Architecture
//!
//! - **[`Table`]**: In-memory versioned storage with sequence-based ordering
//! - **[`View`]**: Read-write transaction with staged changes and commit capability
//! - **[`Snapshot`]**: Read-only point-in-time view with fixed sequence boundary
//! - **Scoped APIs**: Namespace-bound convenience methods ([`ScopedApi`], [`ScopedGet`], etc.)
//!
//! # Key Features
//!
//! - **Snapshot Isolation**: Each transaction sees consistent data from start time
//! - **Atomic Commits**: All changes in a transaction commit together or fail together
//! - **Namespace Partitioning**: Logical separation of data domains
//! - **Streaming Range Queries**: Memory-efficient iteration over large datasets
//!
//! # Usage
//!
//! ```rust,ignore
//! // Basic transaction workflow
//! let table = Table::new();
//! let mut view = View::new(table);
//!
//! // Stage changes
//! view.set(namespace, "key1".to_string(), Some("value1".to_string()));
//! view.set(namespace, "key2".to_string(), None); // deletion
//!
//! // Read with staged changes visible
//! let current = view.get(namespace, "key1".to_string()).await?;
//!
//! // Atomic commit
//! let updated_table = view.commit().await?;
//! ```

pub mod commit;
pub mod key;
pub mod scoped_api;
pub mod scoped_get;
pub mod scoped_range;
pub mod scoped_read;
pub mod scoped_seq_bounded_get;
pub mod scoped_seq_bounded_into_range;
pub mod scoped_seq_bounded_range;
pub mod scoped_seq_bounded_range_iter;
pub mod scoped_set;
pub mod seq_bounded_get;
pub mod seq_bounded_into_range;
pub mod seq_bounded_range;
pub mod seq_bounded_range_iter;
pub mod seq_bounded_read;
pub mod snapshot;
pub mod snapshot_seq;
pub mod table;
pub mod value;
pub mod view;
pub mod view_namespace;

#[cfg(test)]
mod namespace_view_no_seq_increase_test;

pub use self::commit::Commit;
pub use self::key::ViewKey;
pub use self::scoped_api::ScopedApi;
pub use self::scoped_get::ScopedGet;
pub use self::scoped_range::ScopedRange;
pub use self::scoped_read::ScopedRead;
pub use self::scoped_seq_bounded_get::ScopedSeqBoundedGet;
pub use self::scoped_seq_bounded_into_range::ScopedSeqBoundedIntoRange;
pub use self::scoped_seq_bounded_range::ScopedSeqBoundedRange;
pub use self::scoped_seq_bounded_range_iter::ScopedSeqBoundedRangeIter;
pub use self::scoped_set::ScopedSet;
pub use self::seq_bounded_get::SeqBoundedGet;
pub use self::seq_bounded_range::SeqBoundedRange;
pub use self::snapshot::Snapshot;
pub use self::snapshot_seq::SnapshotSeq;
pub use self::table::Table;
pub use self::table::TablesSnapshot;
pub use self::value::ViewValue;
pub use self::view::View;
pub use self::view_namespace::ViewNamespace;
