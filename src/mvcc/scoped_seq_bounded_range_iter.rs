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

//! Namespace-scoped range iteration with snapshot isolation.

use std::ops::RangeBounds;

use seq_marked::SeqMarked;

use crate::mvcc::ViewKey;
use crate::mvcc::ViewValue;

/// Read-only range view bound to a namespace with snapshot isolation.
///
/// Operations are bounded by `snapshot_seq`, ensuring only data with sequences ≤ `snapshot_seq`
/// is visible. Pre-scoped to eliminate namespace parameters.
///
/// ⚠️ **Tombstone Anomaly**: May observe different deletion states for keys with identical sequences.
pub trait ScopedSeqBoundedRangeIter<K, V>
where
    K: ViewKey,
    V: ViewValue,
{
    /// Returns an iterator of key-value pairs within the specified range.
    ///
    /// Returns the most recent visible version for each key with sequence ≤ `snapshot_seq`.
    /// Keys are returned in sorted order, including tombstones.
    fn range_iter<R>(
        &self,
        range: R,
        snapshot_seq: u64,
    ) -> impl Iterator<Item = (&K, SeqMarked<&V>)> + Send
    where
        R: RangeBounds<K> + Clone + 'static;
}
