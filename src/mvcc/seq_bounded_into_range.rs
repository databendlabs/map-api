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

//! Range-based read operations for multi-version key-value storage with namespace isolation.

use std::io;
use std::ops::RangeBounds;

use seq_marked::SeqMarked;

use crate::mvcc::ViewKey;
use crate::mvcc::ViewNamespace;
use crate::mvcc::ViewValue;
use crate::IOResultStream;

/// Read-only multi-version range view with snapshot isolation.
///
/// Operations are bounded by `snapshot_seq` to ensure consistent temporal views.
/// Requires namespace parameters for each operation.
///
/// ⚠️ **Tombstone Anomaly**: May observe different deletion states due to sequence reuse.
#[async_trait::async_trait]
pub trait SeqBoundedIntoRange<S, K, V>
where
    Self: Send + Sync,
    S: ViewNamespace,
    K: ViewKey,
    V: ViewValue,
{
    /// Returns an async stream of key-value pairs within the specified range.
    ///
    /// Returns the most recent visible version for each key with sequence ≤ `snapshot_seq`.
    /// Keys are returned in sorted order, including tombstones.
    async fn into_range<R>(
        self,
        space: S,
        range: R,
        snapshot_seq: u64,
    ) -> Result<IOResultStream<(K, SeqMarked<V>)>, io::Error>
    where
        R: RangeBounds<K> + Send + Sync + Clone + 'static;
}
