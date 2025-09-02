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
/// Namespaces partition data and control sequence number allocation behavior.
pub trait ViewNamespace
where Self: Clone + Copy + Ord + fmt::Debug + Send + Sync + Unpin + 'static
{
    /// Whether insertions in this namespace increment the sequence number.
    ///
    /// Related data (like secondary indices) can share sequence numbers by returning `false`.
    ///
    /// # Example
    /// Primary record: `(key, seq=5) = "value"`
    /// Index record: `(index_key, seq=5) = key` (shares sequence)
    fn if_increase_seq(&self) -> bool;
}
