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

use std::collections::BTreeMap;
use std::io;
use std::ops::RangeBounds;

use futures_util::StreamExt;
use seq_marked::SeqMarked;

use crate::mvcc::Table;
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
pub trait SeqBoundedRange<S, K, V>
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
    async fn range<R>(
        &self,
        space: S,
        range: R,
        snapshot_seq: u64,
    ) -> Result<IOResultStream<(K, SeqMarked<V>)>, io::Error>
    where
        R: RangeBounds<K> + Send + Sync + Clone + 'static;
}

#[async_trait::async_trait]
impl<S, K, V> SeqBoundedRange<S, K, V> for BTreeMap<S, Table<K, V>>
where
    Self: Send + Sync,
    S: ViewNamespace,
    K: ViewKey,
    V: ViewValue,
{
    async fn range<R>(
        &self,
        space: S,
        range: R,
        snapshot_seq: u64,
    ) -> Result<IOResultStream<(K, SeqMarked<V>)>, io::Error>
    where
        R: RangeBounds<K> + Send + Sync + Clone + 'static,
    {
        let Some(table) = self.get(&space) else {
            let strm = futures::stream::empty();
            return Ok(strm.boxed());
        };

        let it = table.range(range, snapshot_seq);
        let vec = it.map(|(k, v)| (k.clone(), v.cloned())).collect::<Vec<_>>();
        let strm = futures::stream::iter(vec).map(Ok);
        Ok(strm.boxed())
    }
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;

    use futures_util::StreamExt;

    use super::*;

    #[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
    struct TestNamespace(u8);

    impl ViewNamespace for TestNamespace {
        fn increments_seq(&self) -> bool {
            true
        }
    }

    #[tokio::test]
    async fn test_empty_map() {
        let map: BTreeMap<TestNamespace, Table<String, String>> = BTreeMap::new();
        let result = SeqBoundedRange::range(&map, TestNamespace(1), .., 5)
            .await
            .unwrap();
        let items: Vec<_> = result.collect().await;
        assert!(items.is_empty());
    }

    #[tokio::test]
    async fn test_nonexistent_namespace() {
        let mut map = BTreeMap::new();
        map.insert(TestNamespace(1), create_test_table());

        let result = SeqBoundedRange::range(&map, TestNamespace(2), .., 10)
            .await
            .unwrap();
        let items: Vec<_> = result.collect().await;
        assert!(items.is_empty());
    }

    #[tokio::test]
    async fn test_existing_table_full_range() {
        let mut map = BTreeMap::new();
        map.insert(TestNamespace(1), create_test_table());

        let result = SeqBoundedRange::range(&map, TestNamespace(1), .., 5)
            .await
            .unwrap();
        let items: Vec<_> = result.collect().await;
        let extracted: Vec<_> = items.into_iter().map(|item| item.unwrap()).collect();

        let expected = vec![
            (k("k1"), SeqMarked::new_normal(1, v("v1"))),
            (k("k2"), SeqMarked::new_normal(3, v("v2"))),
            (k("k3"), SeqMarked::new_normal(5, v("v3"))),
        ];
        assert_eq!(extracted, expected);
    }

    #[tokio::test]
    async fn test_bounded_range() {
        let mut map = BTreeMap::new();
        map.insert(TestNamespace(1), create_test_table());

        let result = SeqBoundedRange::range(&map, TestNamespace(1), k("k2")..=k("k3"), 5)
            .await
            .unwrap();
        let items: Vec<_> = result.collect().await;
        let extracted: Vec<_> = items.into_iter().map(|item| item.unwrap()).collect();

        let expected = vec![
            (k("k2"), SeqMarked::new_normal(3, v("v2"))),
            (k("k3"), SeqMarked::new_normal(5, v("v3"))),
        ];
        assert_eq!(extracted, expected);
    }

    #[tokio::test]
    async fn test_different_snapshot_seq() {
        let mut map = BTreeMap::new();
        map.insert(TestNamespace(1), create_test_table());

        let result_early = SeqBoundedRange::range(&map, TestNamespace(1), .., 2)
            .await
            .unwrap();
        let items_early: Vec<_> = result_early.collect().await;
        let extracted_early: Vec<_> = items_early.into_iter().map(|item| item.unwrap()).collect();

        let result_late = SeqBoundedRange::range(&map, TestNamespace(1), .., 10)
            .await
            .unwrap();
        let items_late: Vec<_> = result_late.collect().await;
        let extracted_late: Vec<_> = items_late.into_iter().map(|item| item.unwrap()).collect();

        let expected_early = vec![(k("k1"), SeqMarked::new_normal(1, v("v1")))];
        let expected_late = vec![
            (k("k1"), SeqMarked::new_normal(1, v("v1"))),
            (k("k2"), SeqMarked::new_normal(3, v("v2"))),
            (k("k3"), SeqMarked::new_normal(5, v("v3"))),
        ];

        assert_eq!(extracted_early, expected_early);
        assert_eq!(extracted_late, expected_late);
    }

    fn create_test_table() -> Table<String, String> {
        let mut table = Table::new();
        table.insert(k("k1"), 1, v("v1")).unwrap();
        table.insert(k("k2"), 3, v("v2")).unwrap();
        table.insert(k("k3"), 5, v("v3")).unwrap();
        table
    }

    fn k(s: &str) -> String {
        s.to_string()
    }

    fn v(s: &str) -> String {
        s.to_string()
    }
}
