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

//! Multi-Version Concurrency Control (MVCC) implementation.
//!
//! Provides snapshot isolation for concurrent access to versioned key-value data.
//! Operations are organized by namespaces and support both read-only and read-write access patterns.
//!
//! # Core Components
//!
//! - **[`Table`]**: In-memory storage for versioned key-value pairs
//! - **[`View`]**: Read-write transactional view with staged changes  
//! - **Snapshot traits**: Read-only access at specific sequence points
//! - **Scoped traits**: Namespace-bound operations for convenience
//!
//! # Usage
//!
//! ```rust,ignore
//! // Create storage and view
//! let table = Table::new();
//! let mut view = View::new(table);
//!
//! // Perform operations
//! view.set(namespace, key, Some(value));
//! let result = view.get(namespace, key).await?;
//!
//! // Commit changes
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
pub use self::scoped_seq_bounded_into_range::ScopedSnapshotIntoRange;
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
