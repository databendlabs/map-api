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

use std::io;
use std::ops::RangeBounds;

use seq_marked::SeqMarked;

use crate::mvcc::ViewKey;
use crate::mvcc::ViewValue;
use crate::IOResultStream;

/// Read-only range view bound to a namespace that consumes self with snapshot isolation.
///
/// Operations are bounded by `snapshot_seq`, ensuring only data with sequences ≤ `snapshot_seq`
/// is visible. Pre-scoped to eliminate namespace parameters.
///
/// ⚠️ **Tombstone Anomaly**: May observe different deletion states for keys with identical sequences.
#[async_trait::async_trait]
pub trait ScopedRange<K, V>
where
    Self: Send + Sync,
    K: ViewKey,
    V: ViewValue,
{
    /// Returns an async stream of key-value pairs within the specified range, consuming self.
    async fn range<R>(&self, range: R) -> Result<IOResultStream<(K, SeqMarked<V>)>, io::Error>
    where R: RangeBounds<K> + Send + Sync + Clone + 'static;
}
