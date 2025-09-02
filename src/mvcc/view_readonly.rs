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

use seq_marked::InternalSeq;
use seq_marked::SeqMarked;

use crate::mvcc::ViewKey;
use crate::mvcc::ViewNamespace;
use crate::mvcc::ViewValue;
use crate::IOResultStream;

/// Read-only view providing snapshot isolation at a specific sequence point.
///
/// All operations see a consistent snapshot as of `base_seq()`. Safe for concurrent access.
///
/// ⚠️ **Tombstone Anomaly**: Deletion operations may reuse sequence numbers,
/// causing inconsistent visibility at sequence boundaries.
#[async_trait::async_trait]
pub trait ViewReadonly<S, K, V>
where
    Self: Send + Sync,
    S: ViewNamespace,
    K: ViewKey,
    V: ViewValue,
{
    /// Maximum sequence number visible in this view.
    fn view_seq(&self) -> InternalSeq;

    /// Get value for key in the specified namespace.
    async fn get(&self, space: S, key: K) -> Result<SeqMarked<V>, io::Error>;

    /// Get multiple keys atomically. Defaults to sequential `get()` calls.
    async fn mget(&self, space: S, keys: Vec<K>) -> Result<Vec<SeqMarked<V>>, io::Error> {
        let mut results = Vec::with_capacity(keys.len());
        for key in keys {
            let result = self.get(space, key).await?;
            results.push(result);
        }
        Ok(results)
    }

    /// Stream key-value pairs within the specified range in sorted order.
    async fn range<R>(
        &self,
        space: S,
        range: R,
    ) -> Result<IOResultStream<(K, SeqMarked<V>)>, io::Error>
    where
        R: RangeBounds<K> + Send + Sync + Clone + 'static;
}
