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
use crate::mvcc::ViewNameSpace;
use crate::mvcc::ViewValue;
use crate::IOResultStream;

/// Read-only view of MVCC data at a specific point in time.
///
/// # Isolation Level: Snapshot Isolation (Read-Only)
///
/// This trait provides **Snapshot Isolation** for read-only transactions with strong consistency guarantees:
///
/// ## ✅ Guarantees
/// - **Point-in-Time Consistency**: All reads see data as of a single, consistent point in time (`view_seq`)
/// - **Repeatable Reads**: Same query always returns identical results within the same view
/// - **No Dirty Reads**: Never sees uncommitted data from other transactions
/// - **No Phantom Reads**: Range queries return consistent results across multiple calls
/// - **Concurrent Safety**: Multiple read-only views can operate concurrently without coordination
///
/// ## ⚠️ Tombstone Anomaly
/// **Important**: Due to historical implementation, tombstone (deletion) operations may be visible
/// at sequence boundaries in unexpected ways:
///
/// - Tombstone insertions reuse the last sequence number instead of generating a new one
/// - Views created exactly at a tombstone's sequence may see newer deletions with the same sequence
/// - This can cause the same view to see different tombstone states depending on timing
///
/// ```rust,ignore
/// // Both views created at seq=100
/// let view1 = create_view_at_seq(100); // Before tombstone insertion
/// let view2 = create_view_at_seq(100); // After tombstone insertion
///
/// // view1.get("key") might return Some(value)
/// // view2.get("key") might return None (tombstone)
/// // Even though both have the same view_seq!
/// ```
///
/// ## Best Practices
///
/// 1. **Use for read-only workloads**: Excellent isolation for analytical queries and reports
/// 2. **Safe concurrency**: No coordination needed between read-only views
/// 3. **Long-running reads**: Suitable for consistent snapshots across time
/// 4. **Pair with read-write transactions carefully**: See [`View`](crate::mvcc::view::View) for write transaction isolation
///
/// ## Performance Characteristics
///
/// - **Memory**: Views hold references to data, minimal memory overhead
/// - **CPU**: Point queries are O(log n), range queries are O(log n + results)  
/// - **I/O**: May involve async I/O depending on implementation
/// - **Concurrency**: Fully concurrent with other read-only views
#[async_trait::async_trait]
pub trait ViewReadonly<S, K, V>
where
    Self: Send + Sync,
    S: ViewNameSpace,
    K: ViewKey,
    V: ViewValue,
{
    /// Return the last seq(inclusive) this view can see.
    fn base_seq(&self) -> InternalSeq;

    /// Gets the value for a key at this view's sequence point.
    ///
    /// ⚠️ **Tombstone Anomaly**: Due to tombstone sequence reuse, consecutive calls within
    /// the same view may observe different deletion states for keys with the same sequence.
    /// See trait documentation for details.
    async fn get(&self, space: S, key: K) -> Result<SeqMarked<V>, io::Error>;

    /// Convenience method to get multiple keys at once.
    ///
    /// This is a default implementation that calls `get` for each key. Implementors
    /// can override this method if they have a more efficient batch implementation.
    async fn mget(&self, space: S, keys: Vec<K>) -> Result<Vec<SeqMarked<V>>, io::Error> {
        let mut results = Vec::with_capacity(keys.len());
        for key in keys {
            let result = self.get(space, key).await?;
            results.push(result);
        }
        Ok(results)
    }

    async fn range<R>(
        &self,
        space: S,
        range: R,
    ) -> Result<IOResultStream<(K, SeqMarked<V>)>, io::Error>
    where
        R: RangeBounds<K> + Send + Sync + Clone + 'static;
}
