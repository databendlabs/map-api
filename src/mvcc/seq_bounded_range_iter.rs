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

use std::ops::RangeBounds;

use seq_marked::SeqMarked;

use crate::mvcc::ViewKey;
use crate::mvcc::ViewNamespace;
use crate::mvcc::ViewValue;

/// Multi-version range iterator with snapshot isolation.
pub trait SeqBoundedRangeIter<S, K, V>
where
    S: ViewNamespace,
    K: ViewKey,
    V: ViewValue,
{
    /// Returns iterator of key-value pairs within range at snapshot sequence.
    ///
    /// Returns most recent version for each key with sequence ≤ `snapshot_seq`.
    /// Keys returned in sorted order, including tombstones.
    fn range_iter<R>(
        &self,
        space: S,
        range: R,
        snapshot_seq: u64,
    ) -> impl Iterator<Item = (&K, SeqMarked<&V>)>
    where
        R: RangeBounds<K> + Clone + 'static;
}
