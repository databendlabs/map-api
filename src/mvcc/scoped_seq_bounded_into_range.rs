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

//! Namespace-scoped range operations that consume self with snapshot isolation.

use std::io;
use std::ops::RangeBounds;

use futures_util::StreamExt;
use seq_marked::SeqMarked;

use crate::mvcc::scoped_seq_bounded_range_iter::ScopedSeqBoundedRangeIter;
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
pub trait ScopedSeqBoundedIntoRange<K, V>
where
    Self: Send + Sync,
    K: ViewKey,
    V: ViewValue,
{
    /// Returns an async stream of key-value pairs within the specified range, consuming self.
    ///
    /// Returns the most recent visible version for each key with sequence ≤ `snapshot_seq`.
    /// Keys are returned in sorted order, including tombstones.
    async fn into_range<R>(
        self,
        range: R,
        snapshot_seq: u64,
    ) -> Result<IOResultStream<(K, SeqMarked<V>)>, io::Error>
    where
        R: RangeBounds<K> + Send + Sync + Clone + 'static;
}

#[async_trait::async_trait]
impl<K, V, Owned> ScopedSeqBoundedIntoRange<K, V> for Owned
where
    K: ViewKey,
    V: ViewValue,
    Owned: ScopedSeqBoundedRangeIter<K, V> + Send + Sync + 'static,
{
    async fn into_range<R>(
        self,
        range: R,
        snapshot_seq: u64,
    ) -> Result<IOResultStream<(K, SeqMarked<V>)>, io::Error>
    where
        R: RangeBounds<K> + Send + Sync + Clone + 'static,
    {
        let strm = owned_range_iter_to_stream(self, range, snapshot_seq);
        Ok(strm.boxed())
    }
}

#[futures_async_stream::try_stream(boxed, ok = (K, SeqMarked<V>), error = io::Error)]
pub(crate) async fn owned_range_iter_to_stream<K, V, R, Owned>(
    table: Owned,
    range: R,
    snapshot_seq: u64,
) where
    K: ViewKey,
    V: ViewValue,
    R: RangeBounds<K> + Clone + Send + Sync + 'static,
    Owned: ScopedSeqBoundedRangeIter<K, V> + Send + Sync + 'static,
{
    let it = table.range_iter(range, snapshot_seq);

    for (k, v) in it {
        yield (k.clone(), v.cloned());
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use futures_util::TryStreamExt;
    use seq_marked::SeqMarked;

    use crate::mvcc::scoped_seq_bounded_into_range::ScopedSeqBoundedIntoRange;
    use crate::mvcc::Table;

    #[tokio::test]
    async fn test_into_range() {
        let mut t = Table::<u64, u64>::new();
        t.insert(1, 5, 1).unwrap();

        let at = Arc::new(t);

        let strm = at.clone().into_range(.., 5).await.unwrap();
        let got = strm.try_collect::<Vec<_>>().await.unwrap();
        assert_eq!(got, vec![(1u64, SeqMarked::new_normal(5, 1u64)),]);

        let strm = at.into_range(.., 2).await.unwrap();
        let got = strm.try_collect::<Vec<_>>().await.unwrap();
        assert_eq!(got, vec![]);
    }
}
