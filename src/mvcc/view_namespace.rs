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

use std::fmt;

/// Trait for namespace identifiers in MVCC operations.
///
/// Namespaces partition data into logical domains and control sequence number allocation behavior.
/// Each namespace can independently decide whether operations increment the global sequence counter.
///
/// # Type Requirements
/// - **Copy + Clone**: Efficient parameter passing and storage
/// - **Ord**: Deterministic ordering for consistent iteration
/// - **Debug**: Troubleshooting and logging support
/// - **Send + Sync**: Thread-safe concurrent access
/// - **Unpin + 'static**: Async compatibility and stable lifetime
///
/// # Sequence Number Strategy
///
/// The [`increments_seq()`] method controls whether insertions in this namespace
/// advance the global sequence counter. This enables related data to share sequence
/// numbers for consistency.
///
/// # Examples
/// ```rust,ignore
/// #[derive(Clone, Copy, Debug, Ord, PartialOrd, Eq, PartialEq)]
/// enum MyNamespace {
///     PrimaryData,    // increments_seq() = true
///     SecondaryIndex, // increments_seq() = false (shares seq with primary)
/// }
///
/// impl ViewNamespace for MyNamespace {
///     fn increments_seq(&self) -> bool {
///         matches!(self, MyNamespace::PrimaryData)
///     }
/// }
/// ```
///
/// [`increments_seq()`]: Self::increments_seq
pub trait ViewNamespace
where Self: Clone + Copy + Ord + fmt::Debug + Send + Sync + Unpin + 'static
{
    /// Whether insertions in this namespace increment the global sequence number.
    ///
    /// Primary data typically returns `true` to advance the sequence, while related
    /// data (like secondary indices) can return `false` to share the sequence number
    /// with their associated primary record.
    ///
    /// # Example Relationship
    /// - Primary record: `(key, seq=5) = "value"`
    /// - Index record: `(index_key, seq=5) = key` (shares sequence with primary)
    fn increments_seq(&self) -> bool;
}
