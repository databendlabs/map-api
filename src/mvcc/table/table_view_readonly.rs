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

use std::collections::BTreeMap;
use std::io::Error;
use std::ops::RangeBounds;

use futures::StreamExt;
use seq_marked::InternalSeq;
use seq_marked::SeqMarked;

use crate::mvcc::key::ViewKey;
use crate::mvcc::table::Table;
use crate::mvcc::value::ViewValue;
use crate::mvcc::view_namespace::ViewNamespace;
use crate::mvcc::view_readonly::ViewReadonly;
use crate::IOResultStream;

/// Implement ViewReadonly for a [`Table`] containing multiple key spaces.
pub struct TableViewReadonly<S, K, V>
where
    S: ViewNamespace,
    K: ViewKey,
    V: ViewValue,
{
    /// The sequence number marking the visibility boundary for key-value pairs(equal or lower than this value can be seen).
    ///
    /// Note: This is incomplete barrier if deletion insertion does not increase the seq.
    pub(crate) base_seq: InternalSeq,

    /// The data in each key space.
    pub(crate) tables: BTreeMap<S, Table<K, V>>,
}

impl<S, K, V> TableViewReadonly<S, K, V>
where
    S: ViewNamespace,
    K: ViewKey,
    V: ViewValue,
{
    pub fn new(tables: BTreeMap<S, Table<K, V>>) -> Self {
        Self {
            base_seq: InternalSeq::new(0),
            tables,
        }
    }
}

#[async_trait::async_trait]
impl<S, K, V> ViewReadonly<S, K, V> for TableViewReadonly<S, K, V>
where
    S: ViewNamespace,
    K: ViewKey,
    V: ViewValue,
{
    fn view_seq(&self) -> InternalSeq {
        self.base_seq
    }

    async fn get(&self, space: S, key: K) -> Result<SeqMarked<V>, Error> {
        let table = self.tables.get(&space);

        let Some(table) = table else {
            return Ok(SeqMarked::new_not_found());
        };

        let value = table.get(key, *self.base_seq).cloned();
        Ok(value)
    }

    async fn range<R>(
        &self,
        space: S,
        range: R,
    ) -> Result<IOResultStream<(K, SeqMarked<V>)>, Error>
    where
        R: RangeBounds<K> + Send + Sync + Clone + 'static,
    {
        let table = self.tables.get(&space);

        let Some(table) = table else {
            let strm = futures::stream::empty();
            return Ok(strm.boxed());
        };

        let pairs: Vec<_> = table
            .range(range, *self.base_seq)
            .map(|x| Ok((x.0.clone(), x.1.cloned())))
            .collect();

        let strm = futures::stream::iter(pairs);
        Ok(strm.boxed())
    }
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;

    use seq_marked::SeqMarked;

    use super::*;

    // Test types that implement the required traits
    #[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
    enum TestSpace {
        Space1,
        Space2,
        Space3,
    }

    impl ViewNamespace for TestSpace {
        fn increments_seq(&self) -> bool {
            true
        }
    }

    #[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
    struct TestKey(String);

    #[derive(Debug, Clone, PartialEq, Eq)]
    struct TestValue(String);

    // Helper functions
    fn key(s: &str) -> TestKey {
        TestKey(s.to_string())
    }

    fn value(s: &str) -> TestValue {
        TestValue(s.to_string())
    }

    fn create_test_table() -> Table<TestKey, TestValue> {
        let mut table = Table::new();
        table.insert(key("k1"), 1, value("v1")).unwrap();
        table.insert(key("k2"), 2, value("v2")).unwrap();
        table.insert(key("k3"), 3, value("v3")).unwrap();
        table.insert_tombstone(key("k4"), 4).unwrap();

        // Add a key with both normal value and tombstone
        table.insert(key("k5"), 5, value("v5")).unwrap();
        table.insert_tombstone(key("k5"), 6).unwrap();

        // Add a key whose tombstone is newer than its normal record
        table.insert(key("k6"), 7, value("v6")).unwrap();
        table.insert_tombstone(key("k6"), 8).unwrap();

        table
    }

    #[tokio::test]
    async fn test_new() {
        let mut tables = BTreeMap::new();
        tables.insert(TestSpace::Space1, create_test_table());

        let view = TableViewReadonly::new(tables);
        assert_eq!(view.view_seq(), InternalSeq::new(0));
        assert_eq!(view.tables.len(), 1);
    }

    #[tokio::test]
    async fn test_base_seq() {
        let mut tables = BTreeMap::new();
        tables.insert(TestSpace::Space1, create_test_table());

        let mut view = TableViewReadonly::new(tables);
        view.base_seq = InternalSeq::new(5);

        assert_eq!(view.view_seq(), InternalSeq::new(5));
    }

    #[tokio::test]
    async fn test_mget_existing_space() {
        let mut tables = BTreeMap::new();
        tables.insert(TestSpace::Space1, create_test_table());

        let mut view = TableViewReadonly::new(tables);
        view.base_seq = InternalSeq::new(10);

        let keys = vec![
            key("k1"),
            key("k2"),
            key("k3"),
            key("k4"),
            key("k5"),
            key("k6"),
        ];
        let result = view.mget(TestSpace::Space1, keys).await.unwrap();

        assert_eq!(result.len(), 6);
        assert_eq!(result[0], SeqMarked::new_normal(1, value("v1")));
        assert_eq!(result[1], SeqMarked::new_normal(2, value("v2")));
        assert_eq!(result[2], SeqMarked::new_normal(3, value("v3")));
        assert_eq!(result[3], SeqMarked::new_tombstone(4));
        assert_eq!(result[4], SeqMarked::new_tombstone(6)); // Latest version is tombstone
        assert_eq!(result[5], SeqMarked::new_tombstone(8)); // Latest version is tombstone
    }

    #[tokio::test]
    async fn test_mget_nonexistent_space() {
        let tables = BTreeMap::new();
        let mut view = TableViewReadonly::<_, _, String>::new(tables);
        view.base_seq = InternalSeq::new(10);

        let keys = vec![key("k1"), key("k2")];
        let result = view.mget(TestSpace::Space1, keys).await.unwrap();

        assert_eq!(result.len(), 2);
        assert!(result[0].is_not_found());
        assert!(result[1].is_not_found());
    }

    #[tokio::test]
    async fn test_mget_empty_keys() {
        let mut tables = BTreeMap::new();
        tables.insert(TestSpace::Space1, create_test_table());

        let mut view = TableViewReadonly::new(tables);
        view.base_seq = InternalSeq::new(10);

        let result = view.mget(TestSpace::Space1, vec![]).await.unwrap();
        assert_eq!(result.len(), 0);
    }

    #[tokio::test]
    async fn test_mget_with_base_seq_limit() {
        let mut tables = BTreeMap::new();
        tables.insert(TestSpace::Space1, create_test_table());

        let mut view = TableViewReadonly::new(tables);
        view.base_seq = InternalSeq::new(2); // Only see up to seq 2

        let keys = vec![
            key("k1"),
            key("k2"),
            key("k3"),
            key("k4"),
            key("k5"),
            key("k6"),
        ];
        let result = view.mget(TestSpace::Space1, keys).await.unwrap();

        assert_eq!(result.len(), 6);
        assert_eq!(result[0], SeqMarked::new_normal(1, value("v1")));
        assert_eq!(result[1], SeqMarked::new_normal(2, value("v2")));
        assert!(result[2].is_not_found()); // seq 3 > base_seq 2
        assert!(result[3].is_not_found()); // seq 4 > base_seq 2
        assert!(result[4].is_not_found()); // seq 5 > base_seq 2
        assert!(result[5].is_not_found()); // seq 7 > base_seq 2
    }

    #[tokio::test]
    async fn test_mget_with_tombstone_base_seq() {
        let mut tables = BTreeMap::new();
        tables.insert(TestSpace::Space1, create_test_table());

        let mut view = TableViewReadonly::new(tables);
        view.base_seq = InternalSeq::new(4); // See up to tombstone seq 4

        let keys = vec![
            key("k1"),
            key("k2"),
            key("k3"),
            key("k4"),
            key("k5"),
            key("k6"),
        ];
        let result = view.mget(TestSpace::Space1, keys).await.unwrap();

        assert_eq!(result.len(), 6);
        assert_eq!(result[0], SeqMarked::new_normal(1, value("v1")));
        assert_eq!(result[1], SeqMarked::new_normal(2, value("v2")));
        assert_eq!(result[2], SeqMarked::new_normal(3, value("v3")));
        assert_eq!(result[3], SeqMarked::new_tombstone(4));
        assert!(result[4].is_not_found()); // seq 5 > base_seq 4
        assert!(result[5].is_not_found()); // seq 7 > base_seq 4
    }

    #[tokio::test]
    async fn test_mget_with_tombstone_base_seq_after_tombstone() {
        let mut tables = BTreeMap::new();
        tables.insert(TestSpace::Space1, create_test_table());

        let mut view = TableViewReadonly::new(tables);
        view.base_seq = InternalSeq::new(6); // See up to tombstone seq 6

        let keys = vec![
            key("k1"),
            key("k2"),
            key("k3"),
            key("k4"),
            key("k5"),
            key("k6"),
        ];
        let result = view.mget(TestSpace::Space1, keys).await.unwrap();

        assert_eq!(result.len(), 6);
        assert_eq!(result[0], SeqMarked::new_normal(1, value("v1")));
        assert_eq!(result[1], SeqMarked::new_normal(2, value("v2")));
        assert_eq!(result[2], SeqMarked::new_normal(3, value("v3")));
        assert_eq!(result[3], SeqMarked::new_tombstone(4));
        assert_eq!(result[4], SeqMarked::new_tombstone(6)); // Can see the tombstone
        assert!(result[5].is_not_found()); // seq 7 > base_seq 6
    }

    #[tokio::test]
    async fn test_mget_with_tombstone_base_seq_after_all_tombstones() {
        let mut tables = BTreeMap::new();
        tables.insert(TestSpace::Space1, create_test_table());

        let mut view = TableViewReadonly::new(tables);
        view.base_seq = InternalSeq::new(8); // See up to tombstone seq 8

        let keys = vec![
            key("k1"),
            key("k2"),
            key("k3"),
            key("k4"),
            key("k5"),
            key("k6"),
        ];
        let result = view.mget(TestSpace::Space1, keys).await.unwrap();

        assert_eq!(result.len(), 6);
        assert_eq!(result[0], SeqMarked::new_normal(1, value("v1")));
        assert_eq!(result[1], SeqMarked::new_normal(2, value("v2")));
        assert_eq!(result[2], SeqMarked::new_normal(3, value("v3")));
        assert_eq!(result[3], SeqMarked::new_tombstone(4));
        assert_eq!(result[4], SeqMarked::new_tombstone(6)); // Can see the tombstone
        assert_eq!(result[5], SeqMarked::new_tombstone(8)); // Can see the tombstone
    }

    #[tokio::test]
    async fn test_mget_key_with_both_value_and_tombstone() {
        let mut tables = BTreeMap::new();
        tables.insert(TestSpace::Space1, create_test_table());

        let mut view = TableViewReadonly::new(tables);

        // Test viewing at seq 5 (before tombstone)
        view.base_seq = InternalSeq::new(5);
        let keys = vec![key("k5")];
        let result = view.mget(TestSpace::Space1, keys.clone()).await.unwrap();
        assert_eq!(result.len(), 1);
        assert_eq!(result[0], SeqMarked::new_normal(5, value("v5")));

        // Test viewing at seq 6 (after tombstone)
        view.base_seq = InternalSeq::new(6);
        let result = view.mget(TestSpace::Space1, keys.clone()).await.unwrap();
        assert_eq!(result.len(), 1);
        assert_eq!(result[0], SeqMarked::new_tombstone(6));

        // Test viewing at seq 7 (after tombstone)
        view.base_seq = InternalSeq::new(7);
        let result = view.mget(TestSpace::Space1, keys).await.unwrap();
        assert_eq!(result.len(), 1);
        assert_eq!(result[0], SeqMarked::new_tombstone(6)); // Still see tombstone
    }

    #[tokio::test]
    async fn test_mget_key_with_newer_tombstone() {
        let mut tables = BTreeMap::new();
        tables.insert(TestSpace::Space1, create_test_table());

        let mut view = TableViewReadonly::new(tables);

        // Test viewing at seq 7 (before tombstone)
        view.base_seq = InternalSeq::new(7);
        let keys = vec![key("k6")];
        let result = view.mget(TestSpace::Space1, keys.clone()).await.unwrap();
        assert_eq!(result.len(), 1);
        assert_eq!(result[0], SeqMarked::new_normal(7, value("v6")));

        view.base_seq = InternalSeq::new(8);
        let result = view.mget(TestSpace::Space1, keys.clone()).await.unwrap();
        assert_eq!(result.len(), 1);
        assert_eq!(result[0], SeqMarked::new_tombstone(8)); // Latest version is tombstone

        // Test viewing at seq 9 (after tombstone)
        view.base_seq = InternalSeq::new(9);
        let result = view.mget(TestSpace::Space1, keys).await.unwrap();
        assert_eq!(result.len(), 1);
        assert_eq!(result[0], SeqMarked::new_tombstone(8)); // Still see tombstone
    }

    #[tokio::test]
    async fn test_range_existing_space() {
        let mut tables = BTreeMap::new();
        tables.insert(TestSpace::Space1, create_test_table());

        let mut view = TableViewReadonly::new(tables);
        view.base_seq = InternalSeq::new(10);

        let range = key("k1")..=key("k6");
        let mut stream = view.range(TestSpace::Space1, range).await.unwrap();

        let mut results = Vec::new();
        while let Some(result) = stream.next().await {
            results.push(result.unwrap());
        }

        assert_eq!(results.len(), 6);
        assert_eq!(
            results[0],
            (key("k1"), SeqMarked::new_normal(1, value("v1")))
        );
        assert_eq!(
            results[1],
            (key("k2"), SeqMarked::new_normal(2, value("v2")))
        );
        assert_eq!(
            results[2],
            (key("k3"), SeqMarked::new_normal(3, value("v3")))
        );
        assert_eq!(results[3], (key("k4"), SeqMarked::new_tombstone(4)));
        assert_eq!(results[4], (key("k5"), SeqMarked::new_tombstone(6))); // Latest version is tombstone
        assert_eq!(results[5], (key("k6"), SeqMarked::new_tombstone(8))); // Latest version is tombstone
    }

    #[tokio::test]
    async fn test_range_nonexistent_space() {
        let tables = BTreeMap::new();
        let mut view = TableViewReadonly::<_, _, String>::new(tables);
        view.base_seq = InternalSeq::new(10);

        let range = key("k1")..=key("k3");
        let mut stream = view.range(TestSpace::Space1, range).await.unwrap();

        let result = stream.next().await;
        assert!(result.is_none());
    }

    #[tokio::test]
    async fn test_range_empty_range() {
        let mut tables = BTreeMap::new();
        tables.insert(TestSpace::Space1, create_test_table());

        let mut view = TableViewReadonly::new(tables);
        view.base_seq = InternalSeq::new(10);

        // Range that doesn't match any keys
        let range = key("kx")..=key("ky");
        let mut stream = view.range(TestSpace::Space1, range).await.unwrap();

        let result = stream.next().await;
        assert!(result.is_none());
    }

    #[tokio::test]
    async fn test_range_with_base_seq_limit() {
        let mut tables = BTreeMap::new();
        tables.insert(TestSpace::Space1, create_test_table());

        let mut view = TableViewReadonly::new(tables);
        view.base_seq = InternalSeq::new(2); // Only see up to seq 2

        let range = key("k1")..=key("k6");
        let mut stream = view.range(TestSpace::Space1, range).await.unwrap();

        let mut results = Vec::new();
        while let Some(result) = stream.next().await {
            results.push(result.unwrap());
        }

        assert_eq!(results.len(), 2);
        assert_eq!(
            results[0],
            (key("k1"), SeqMarked::new_normal(1, value("v1")))
        );
        assert_eq!(
            results[1],
            (key("k2"), SeqMarked::new_normal(2, value("v2")))
        );
        // k3, k4, k5, and k6 are filtered out due to base_seq limit
    }

    #[tokio::test]
    async fn test_range_with_tombstone_base_seq() {
        let mut tables = BTreeMap::new();
        tables.insert(TestSpace::Space1, create_test_table());

        let mut view = TableViewReadonly::new(tables);
        view.base_seq = InternalSeq::new(4);

        let range = key("k1")..=key("k6");
        let mut stream = view.range(TestSpace::Space1, range).await.unwrap();

        let mut results = Vec::new();
        while let Some(result) = stream.next().await {
            results.push(result.unwrap());
        }

        assert_eq!(results.len(), 4);
        assert_eq!(
            results[0],
            (key("k1"), SeqMarked::new_normal(1, value("v1")))
        );
        assert_eq!(
            results[1],
            (key("k2"), SeqMarked::new_normal(2, value("v2")))
        );
        assert_eq!(
            results[2],
            (key("k3"), SeqMarked::new_normal(3, value("v3")))
        );
        assert_eq!(results[3], (key("k4"), SeqMarked::new_tombstone(4)));
        // k5 and k6 are filtered out due to base_seq limit (seq 5,7 > base_seq 4)
    }

    #[tokio::test]
    async fn test_range_with_tombstone_base_seq_after_tombstone() {
        let mut tables = BTreeMap::new();
        tables.insert(TestSpace::Space1, create_test_table());

        let mut view = TableViewReadonly::new(tables);
        view.base_seq = InternalSeq::new(6);

        let range = key("k1")..=key("k6");
        let mut stream = view.range(TestSpace::Space1, range).await.unwrap();

        let mut results = Vec::new();
        while let Some(result) = stream.next().await {
            results.push(result.unwrap());
        }

        assert_eq!(results.len(), 5);
        assert_eq!(
            results[0],
            (key("k1"), SeqMarked::new_normal(1, value("v1")))
        );
        assert_eq!(
            results[1],
            (key("k2"), SeqMarked::new_normal(2, value("v2")))
        );
        assert_eq!(
            results[2],
            (key("k3"), SeqMarked::new_normal(3, value("v3")))
        );
        assert_eq!(results[3], (key("k4"), SeqMarked::new_tombstone(4)));
        assert_eq!(results[4], (key("k5"), SeqMarked::new_tombstone(6))); // Can see the tombstone
                                                                          // k6 is filtered out due to base_seq limit (seq 7 > base_seq 6)
    }

    #[tokio::test]
    async fn test_range_with_tombstone_base_seq_after_all_tombstones() {
        let mut tables = BTreeMap::new();
        tables.insert(TestSpace::Space1, create_test_table());

        let mut view = TableViewReadonly::new(tables);
        view.base_seq = InternalSeq::new(8);

        let range = key("k1")..=key("k6");
        let mut stream = view.range(TestSpace::Space1, range).await.unwrap();

        let mut results = Vec::new();
        while let Some(result) = stream.next().await {
            results.push(result.unwrap());
        }

        assert_eq!(results.len(), 6);
        assert_eq!(
            results[0],
            (key("k1"), SeqMarked::new_normal(1, value("v1")))
        );
        assert_eq!(
            results[1],
            (key("k2"), SeqMarked::new_normal(2, value("v2")))
        );
        assert_eq!(
            results[2],
            (key("k3"), SeqMarked::new_normal(3, value("v3")))
        );
        assert_eq!(results[3], (key("k4"), SeqMarked::new_tombstone(4)));
        assert_eq!(results[4], (key("k5"), SeqMarked::new_tombstone(6))); // Can see the tombstone
        assert_eq!(results[5], (key("k6"), SeqMarked::new_tombstone(8))); // Can see the tombstone
    }

    #[tokio::test]
    async fn test_multiple_spaces() {
        let mut tables = BTreeMap::new();
        tables.insert(TestSpace::Space1, create_test_table());

        let mut table2 = Table::new();
        table2.insert(key("a1"), 1, value("av1")).unwrap();
        table2.insert(key("a2"), 2, value("av2")).unwrap();
        tables.insert(TestSpace::Space2, table2);

        let mut view = TableViewReadonly::new(tables);
        view.base_seq = InternalSeq::new(10);

        // Test Space1
        let keys1 = vec![key("k1"), key("k2")];
        let result1 = view.mget(TestSpace::Space1, keys1).await.unwrap();
        assert_eq!(result1.len(), 2);
        assert_eq!(result1[0], SeqMarked::new_normal(1, value("v1")));
        assert_eq!(result1[1], SeqMarked::new_normal(2, value("v2")));

        // Test Space2
        let keys2 = vec![key("a1"), key("a2")];
        let result2 = view.mget(TestSpace::Space2, keys2).await.unwrap();
        assert_eq!(result2.len(), 2);
        assert_eq!(result2[0], SeqMarked::new_normal(1, value("av1")));
        assert_eq!(result2[1], SeqMarked::new_normal(2, value("av2")));

        // Test nonexistent Space3
        let keys3 = vec![key("x1")];
        let result3 = view.mget(TestSpace::Space3, keys3).await.unwrap();
        assert_eq!(result3.len(), 1);
        assert!(result3[0].is_not_found());
    }

    #[tokio::test]
    async fn test_edge_cases() {
        let mut tables = BTreeMap::new();
        let mut table = Table::new();

        // Insert values with various sequence numbers
        table.insert(key("k1"), 1, value("v1")).unwrap();
        table.insert_tombstone(key("k3"), 50).unwrap();
        table.insert(key("k2"), 100, value("v100")).unwrap();

        tables.insert(TestSpace::Space1, table);

        let mut view = TableViewReadonly::new(tables);
        view.base_seq = InternalSeq::new(75); // Between 50 and 100

        let keys = vec![key("k1"), key("k2"), key("k3")];
        let result = view.mget(TestSpace::Space1, keys).await.unwrap();

        assert_eq!(result.len(), 3);
        assert_eq!(result[0], SeqMarked::new_normal(1, value("v1")));
        assert!(result[1].is_not_found()); // seq 100 > base_seq 75
        assert_eq!(result[2], SeqMarked::new_tombstone(50));
    }

    #[tokio::test]
    async fn test_zero_base_seq() {
        let mut tables = BTreeMap::new();
        tables.insert(TestSpace::Space1, create_test_table());

        let mut view = TableViewReadonly::new(tables);
        view.base_seq = InternalSeq::new(0); // Can't see anything

        let keys = vec![key("k1"), key("k2")];
        let result = view.mget(TestSpace::Space1, keys).await.unwrap();

        assert_eq!(result.len(), 2);
        assert!(result[0].is_not_found());
        assert!(result[1].is_not_found());
    }

    #[tokio::test]
    async fn test_not_found_base_seq() {
        let mut tables = BTreeMap::new();
        tables.insert(TestSpace::Space1, create_test_table());

        let mut view = TableViewReadonly::new(tables);
        view.base_seq = InternalSeq::new(0); // Can't see anything

        let keys = vec![key("k1"), key("k2")];
        let result = view.mget(TestSpace::Space1, keys).await.unwrap();

        assert_eq!(result.len(), 2);
        assert!(result[0].is_not_found());
        assert!(result[1].is_not_found());
    }

    // Additional corner case tests

    #[tokio::test]
    async fn test_key_with_multiple_versions() {
        let mut tables = BTreeMap::new();
        let mut table = Table::new();

        // Create a key with multiple normal values and tombstones
        table.insert(key("k1"), 1, value("v1")).unwrap();
        table.insert(key("k1"), 3, value("v1_updated")).unwrap();
        table.insert_tombstone(key("k1"), 5).unwrap();
        table.insert(key("k1"), 7, value("v1_resurrected")).unwrap(); // Resurrection
        table.insert_tombstone(key("k1"), 9).unwrap(); // Final tombstone

        tables.insert(TestSpace::Space1, table);
        let mut view = TableViewReadonly::new(tables);

        // Test at different view sequences
        view.base_seq = InternalSeq::new(1);
        let result = view.mget(TestSpace::Space1, vec![key("k1")]).await.unwrap();
        assert_eq!(result[0], SeqMarked::new_normal(1, value("v1")));

        view.base_seq = InternalSeq::new(3);
        let result = view.mget(TestSpace::Space1, vec![key("k1")]).await.unwrap();
        assert_eq!(result[0], SeqMarked::new_normal(3, value("v1_updated")));

        view.base_seq = InternalSeq::new(5);
        let result = view.mget(TestSpace::Space1, vec![key("k1")]).await.unwrap();
        assert_eq!(result[0], SeqMarked::new_tombstone(5));

        view.base_seq = InternalSeq::new(7);
        let result = view.mget(TestSpace::Space1, vec![key("k1")]).await.unwrap();
        assert_eq!(result[0], SeqMarked::new_normal(7, value("v1_resurrected")));

        view.base_seq = InternalSeq::new(9);
        let result = view.mget(TestSpace::Space1, vec![key("k1")]).await.unwrap();
        assert_eq!(result[0], SeqMarked::new_tombstone(9));
    }

    #[tokio::test]
    async fn test_range_unbounded() {
        let mut tables = BTreeMap::new();
        tables.insert(TestSpace::Space1, create_test_table());

        let mut view = TableViewReadonly::new(tables);
        view.base_seq = InternalSeq::new(10);

        // Test unbounded range (..)
        let mut stream = view.range(TestSpace::Space1, ..).await.unwrap();
        let mut results = Vec::new();
        while let Some(result) = stream.next().await {
            results.push(result.unwrap());
        }

        assert_eq!(results.len(), 6);
        assert_eq!(
            results[0],
            (key("k1"), SeqMarked::new_normal(1, value("v1")))
        );
        assert_eq!(results[5], (key("k6"), SeqMarked::new_tombstone(8)));
    }

    #[tokio::test]
    async fn test_range_from_unbounded() {
        let mut tables = BTreeMap::new();
        tables.insert(TestSpace::Space1, create_test_table());

        let mut view = TableViewReadonly::new(tables);
        view.base_seq = InternalSeq::new(10);

        // Test range from k3 to end
        let mut stream = view.range(TestSpace::Space1, key("k3")..).await.unwrap();
        let mut results = Vec::new();
        while let Some(result) = stream.next().await {
            results.push(result.unwrap());
        }

        assert_eq!(results.len(), 4); // k3, k4, k5, k6
        assert_eq!(
            results[0],
            (key("k3"), SeqMarked::new_normal(3, value("v3")))
        );
        assert_eq!(results[3], (key("k6"), SeqMarked::new_tombstone(8)));
    }

    #[tokio::test]
    async fn test_range_to_unbounded() {
        let mut tables = BTreeMap::new();
        tables.insert(TestSpace::Space1, create_test_table());

        let mut view = TableViewReadonly::new(tables);
        view.base_seq = InternalSeq::new(10);

        // Test range from start to k3 (exclusive)
        let mut stream = view.range(TestSpace::Space1, ..key("k4")).await.unwrap();
        let mut results = Vec::new();
        while let Some(result) = stream.next().await {
            results.push(result.unwrap());
        }

        assert_eq!(results.len(), 3); // k1, k2, k3
        assert_eq!(
            results[0],
            (key("k1"), SeqMarked::new_normal(1, value("v1")))
        );
        assert_eq!(
            results[2],
            (key("k3"), SeqMarked::new_normal(3, value("v3")))
        );
    }

    #[tokio::test]
    async fn test_range_exclusive() {
        let mut tables = BTreeMap::new();
        tables.insert(TestSpace::Space1, create_test_table());

        let mut view = TableViewReadonly::new(tables);
        view.base_seq = InternalSeq::new(10);

        // Test exclusive range k2..k5
        let mut stream = view
            .range(TestSpace::Space1, key("k2")..key("k5"))
            .await
            .unwrap();
        let mut results = Vec::new();
        while let Some(result) = stream.next().await {
            results.push(result.unwrap());
        }

        assert_eq!(results.len(), 3); // k2, k3, k4 (k5 excluded)
        assert_eq!(
            results[0],
            (key("k2"), SeqMarked::new_normal(2, value("v2")))
        );
        assert_eq!(results[2], (key("k4"), SeqMarked::new_tombstone(4)));
    }

    #[tokio::test]
    async fn test_range_single_point() {
        let mut tables = BTreeMap::new();
        tables.insert(TestSpace::Space1, create_test_table());

        let mut view = TableViewReadonly::new(tables);
        view.base_seq = InternalSeq::new(10);

        // Test single point range k3..=k3
        let mut stream = view
            .range(TestSpace::Space1, key("k3")..=key("k3"))
            .await
            .unwrap();
        let mut results = Vec::new();
        while let Some(result) = stream.next().await {
            results.push(result.unwrap());
        }

        assert_eq!(results.len(), 1);
        assert_eq!(
            results[0],
            (key("k3"), SeqMarked::new_normal(3, value("v3")))
        );
    }

    #[tokio::test]
    async fn test_range_empty_result() {
        let mut tables = BTreeMap::new();
        tables.insert(TestSpace::Space1, create_test_table());

        let mut view = TableViewReadonly::new(tables);
        view.base_seq = InternalSeq::new(10);

        // Test range that should be empty
        let mut stream = view
            .range(TestSpace::Space1, key("k5")..key("k5"))
            .await
            .unwrap();
        let result = stream.next().await;
        assert!(result.is_none());
    }

    #[tokio::test]
    async fn test_empty_table_space() {
        let mut tables = BTreeMap::new();
        tables.insert(TestSpace::Space1, Table::<_, String>::new()); // Empty table

        let mut view = TableViewReadonly::new(tables);
        view.base_seq = InternalSeq::new(10);

        // Test mget on empty table
        let keys = vec![key("k1"), key("k2")];
        let result = view.mget(TestSpace::Space1, keys).await.unwrap();
        assert_eq!(result.len(), 2);
        assert!(result[0].is_not_found());
        assert!(result[1].is_not_found());

        // Test range on empty table
        let mut stream = view.range(TestSpace::Space1, ..).await.unwrap();
        let result = stream.next().await;
        assert!(result.is_none());
    }

    #[tokio::test]
    async fn test_base_seq_boundary_conditions() {
        let mut tables = BTreeMap::new();
        let mut table = Table::new();

        // Create entries at boundary sequence numbers
        table
            .insert(key("k1"), u64::MAX - 1, value("v_max_minus_1"))
            .unwrap();
        table.insert_tombstone(key("k2"), u64::MAX).unwrap();

        tables.insert(TestSpace::Space1, table);
        let mut view = TableViewReadonly::new(tables);

        //
        view.base_seq = InternalSeq::new(u64::MAX);
        let keys = vec![key("k1"), key("k2")];
        let result = view.mget(TestSpace::Space1, keys).await.unwrap();

        assert_eq!(result.len(), 2);
        assert_eq!(
            result[0],
            SeqMarked::new_normal(u64::MAX - 1, value("v_max_minus_1"))
        );
        assert_eq!(result[1], SeqMarked::new_tombstone(u64::MAX));
    }

    #[tokio::test]
    async fn test_range_with_filtered_out_keys() {
        let mut tables = BTreeMap::new();
        tables.insert(TestSpace::Space1, create_test_table());

        let mut view = TableViewReadonly::new(tables);
        view.base_seq = InternalSeq::new(3); // Only see k1, k2, k3

        // Test range that includes filtered keys
        let mut stream = view
            .range(TestSpace::Space1, key("k1")..=key("k6"))
            .await
            .unwrap();
        let mut results = Vec::new();
        while let Some(result) = stream.next().await {
            results.push(result.unwrap());
        }

        // Should only see k1, k2, k3 (k4, k5, k6 filtered out)
        assert_eq!(results.len(), 3);
        assert_eq!(
            results[0],
            (key("k1"), SeqMarked::new_normal(1, value("v1")))
        );
        assert_eq!(
            results[1],
            (key("k2"), SeqMarked::new_normal(2, value("v2")))
        );
        assert_eq!(
            results[2],
            (key("k3"), SeqMarked::new_normal(3, value("v3")))
        );
    }

    #[tokio::test]
    async fn test_mget_mixed_found_and_not_found() {
        let mut tables = BTreeMap::new();
        tables.insert(TestSpace::Space1, create_test_table());

        let mut view = TableViewReadonly::new(tables);
        view.base_seq = InternalSeq::new(10);

        // Mix of existing and non-existing keys
        let keys = vec![
            key("k1"), // exists
            key("kx"), // doesn't exist
            key("k3"), // exists
            key("ky"), // doesn't exist
            key("k5"), // exists (tombstone)
        ];
        let result = view.mget(TestSpace::Space1, keys).await.unwrap();

        assert_eq!(result.len(), 5);
        assert_eq!(result[0], SeqMarked::new_normal(1, value("v1")));
        assert!(result[1].is_not_found());
        assert_eq!(result[2], SeqMarked::new_normal(3, value("v3")));
        assert!(result[3].is_not_found());
        assert_eq!(result[4], SeqMarked::new_tombstone(6));
    }

    #[tokio::test]
    async fn test_key_ordering_in_range() {
        let mut tables = BTreeMap::new();
        let mut table = Table::new();

        // Insert keys in non-lexicographic order to test sorting
        table.insert(key("key10"), 1, value("v10")).unwrap();
        table.insert(key("key2"), 2, value("v2")).unwrap();
        table.insert(key("key1"), 3, value("v1")).unwrap();

        tables.insert(TestSpace::Space1, table);
        let mut view = TableViewReadonly::new(tables);
        view.base_seq = InternalSeq::new(10);

        let mut stream = view.range(TestSpace::Space1, ..).await.unwrap();
        let mut results = Vec::new();
        while let Some(result) = stream.next().await {
            results.push(result.unwrap());
        }

        // Should be ordered lexicographically: key1, key10, key2
        assert_eq!(results.len(), 3);
        assert_eq!(
            results[0],
            (key("key1"), SeqMarked::new_normal(3, value("v1")))
        );
        assert_eq!(
            results[1],
            (key("key10"), SeqMarked::new_normal(1, value("v10")))
        );
        assert_eq!(
            results[2],
            (key("key2"), SeqMarked::new_normal(2, value("v2")))
        );
    }
}
