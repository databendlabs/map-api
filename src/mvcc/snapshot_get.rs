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

//! Snapshot-based read operations for multi-version key-value storage with namespace isolation.

use std::io;

use seq_marked::SeqMarked;

use crate::mvcc::ViewKey;
use crate::mvcc::ViewNamespace;
use crate::mvcc::ViewValue;
/// Read-only multi-version view with snapshot isolation.
///
/// Operations are bounded by `snapshot_seq` to ensure consistent temporal views.
/// Requires namespace parameters for each operation.
///
/// ⚠️ **Tombstone Anomaly**: May observe different deletion states due to sequence reuse.
#[async_trait::async_trait]
pub trait SnapshotGet<S, K, V>
where
    Self: Send + Sync,
    S: ViewNamespace,
    K: ViewKey,
    V: ViewValue,
{
    /// Gets the value for a key at the given snapshot sequence.
    ///
    /// Returns the most recent version with sequence ≤ `snapshot_seq`,
    /// or `SeqMarked::new_not_found()` if no such version exists.
    async fn get(&self, space: S, key: K, snapshot_seq: u64) -> Result<SeqMarked<V>, io::Error>;

    /// Gets multiple keys at the given snapshot sequence.
    ///
    /// Results maintain the same order as input keys. Default implementation calls `get()` sequentially.
    async fn mget(
        &self,
        space: S,
        keys: Vec<K>,
        snapshot_seq: u64,
    ) -> Result<Vec<SeqMarked<V>>, io::Error> {
        let mut values = Vec::with_capacity(keys.len());
        for key in keys {
            let value = self.get(space, key, snapshot_seq).await?;
            values.push(value);
        }
        Ok(values)
    }
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;

    use seq_marked::SeqMarked;

    use super::*;
    use crate::mvcc::table::Table;
    use crate::mvcc::ViewNamespace;

    #[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
    struct TestNamespace(u8);

    #[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
    struct TestKey(String);

    #[derive(Debug, Clone, PartialEq, Eq)]
    struct TestValue(String);

    impl ViewNamespace for TestNamespace {
        fn increments_seq(&self) -> bool {
            true
        }
    }

    fn namespace(id: u8) -> TestNamespace {
        TestNamespace(id)
    }

    fn key(s: &str) -> TestKey {
        TestKey(s.to_string())
    }

    fn value(s: &str) -> TestValue {
        TestValue(s.to_string())
    }

    // Mock implementation for testing
    struct MockSnapshotReader {
        // Each namespace has its own Table
        tables: BTreeMap<TestNamespace, Table<TestKey, TestValue>>,
    }

    impl MockSnapshotReader {
        fn new() -> Self {
            let mut tables = BTreeMap::new();

            // Namespace 1
            let mut table1 = Table::new();
            table1.insert(key("key1"), 1, value("value1")).unwrap();
            table1.insert(key("key2"), 2, value("value2")).unwrap();
            table1.insert_tombstone(key("key3"), 3).unwrap();
            table1.insert(key("key4"), 10, value("value4")).unwrap();
            tables.insert(namespace(1), table1);

            // Namespace 2
            let mut table2 = Table::new();
            table2.insert(key("key1"), 5, value("ns2_value1")).unwrap();
            table2.insert(key("key5"), 7, value("value5")).unwrap();
            tables.insert(namespace(2), table2);

            Self { tables }
        }
    }

    #[async_trait::async_trait]
    impl SnapshotGet<TestNamespace, TestKey, TestValue> for MockSnapshotReader {
        async fn get(
            &self,
            space: TestNamespace,
            key: TestKey,
            snapshot_seq: u64,
        ) -> Result<SeqMarked<TestValue>, io::Error> {
            match self.tables.get(&space) {
                Some(table) => {
                    let result = table.get(key, snapshot_seq);
                    Ok(result.cloned()) // Convert SeqMarked<&TestValue> to SeqMarked<TestValue>
                }
                None => Ok(SeqMarked::new_not_found()),
            }
        }
    }

    #[tokio::test]
    async fn test_snapshot_reader_get_basic_operations() {
        let reader = MockSnapshotReader::new();
        let snapshot_seq = 10;

        // Test existing key
        let result = reader
            .get(namespace(1), key("key1"), snapshot_seq)
            .await
            .unwrap();
        assert_eq!(result, SeqMarked::new_normal(1, value("value1")));
        assert!(*result.internal_seq() <= snapshot_seq);

        // Test tombstone key
        let result = reader
            .get(namespace(1), key("key3"), snapshot_seq)
            .await
            .unwrap();
        assert_eq!(result, SeqMarked::new_tombstone(3));
        assert!(*result.internal_seq() <= snapshot_seq);

        // Test non-existent key
        let result = reader
            .get(namespace(1), key("nonexistent"), snapshot_seq)
            .await
            .unwrap();
        assert_eq!(result, SeqMarked::new_not_found());

        // Test non-existent namespace
        let result = reader
            .get(namespace(99), key("key1"), snapshot_seq)
            .await
            .unwrap();
        assert_eq!(result, SeqMarked::new_not_found());
    }

    #[tokio::test]
    async fn test_snapshot_reader_get_sequence_filter() {
        let reader = MockSnapshotReader::new();

        // key4 has seq 10, should be visible with snapshot_seq 10
        let snapshot_seq1 = 10;
        let result = reader
            .get(namespace(1), key("key4"), snapshot_seq1)
            .await
            .unwrap();
        assert_eq!(result, SeqMarked::new_normal(10, value("value4")));
        assert!(*result.internal_seq() <= snapshot_seq1);

        // key4 has seq 10, should not be visible with snapshot_seq 9
        let snapshot_seq2 = 9;
        let result = reader
            .get(namespace(1), key("key4"), snapshot_seq2)
            .await
            .unwrap();
        assert_eq!(result, SeqMarked::new_not_found());
        // Note: not_found doesn't have a meaningful sequence to check
    }

    #[tokio::test]
    async fn test_snapshot_reader_get_different_namespaces() {
        let reader = MockSnapshotReader::new();
        let snapshot_seq = 10;

        // Same key in different namespaces should return different values
        let result1 = reader
            .get(namespace(1), key("key1"), snapshot_seq)
            .await
            .unwrap();
        assert_eq!(result1, SeqMarked::new_normal(1, value("value1")));
        assert!(*result1.internal_seq() <= snapshot_seq);

        let result2 = reader
            .get(namespace(2), key("key1"), snapshot_seq)
            .await
            .unwrap();
        assert_eq!(result2, SeqMarked::new_normal(5, value("ns2_value1")));
        assert!(*result2.internal_seq() <= snapshot_seq);
    }

    #[tokio::test]
    async fn test_snapshot_reader_mget_operations() {
        let reader = MockSnapshotReader::new();

        // Test basic mget with mixed results (existing, not found)
        let keys = vec![key("key1"), key("key2"), key("nonexistent")];
        let snapshot_seq = 10;
        let results = reader.mget(namespace(1), keys, snapshot_seq).await.unwrap();

        assert_eq!(results.len(), 3);
        assert_eq!(results[0], SeqMarked::new_normal(1, value("value1")));
        assert!(*results[0].internal_seq() <= snapshot_seq);
        assert_eq!(results[1], SeqMarked::new_normal(2, value("value2")));
        assert!(*results[1].internal_seq() <= snapshot_seq);
        assert_eq!(results[2], SeqMarked::new_not_found());

        // Test empty key vector
        let empty_keys = vec![];
        let results = reader
            .mget(namespace(1), empty_keys, snapshot_seq)
            .await
            .unwrap();
        assert_eq!(results.len(), 0);

        // Test sequence filtering in mget
        let keys = vec![key("key1"), key("key4")]; // key1 seq=1, key4 seq=10
        let snapshot_seq = 5;
        let results = reader.mget(namespace(1), keys, snapshot_seq).await.unwrap();

        assert_eq!(results.len(), 2);
        assert_eq!(results[0], SeqMarked::new_normal(1, value("value1"))); // visible
        assert!(*results[0].internal_seq() <= snapshot_seq);
        assert_eq!(results[1], SeqMarked::new_not_found()); // not visible due to seq filter
    }
}
