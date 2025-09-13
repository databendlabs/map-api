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

//! Namespace-scoped read operations with snapshot isolation.

use std::io;

use seq_marked::SeqMarked;

use crate::mvcc::ViewKey;
use crate::mvcc::ViewValue;
/// Read-only view bound to a namespace with snapshot isolation.
///
/// Operations are bounded by `snapshot_seq`, ensuring only data with sequences ≤ `snapshot_seq`
/// is visible. Pre-scoped to eliminate namespace parameters.
///
/// ⚠️ **Tombstone Anomaly**: May observe different deletion states for keys with identical sequences.
#[async_trait::async_trait]
pub trait ScopedSeqBoundedGet<K, V>
where
    Self: Send + Sync,
    K: ViewKey,
    V: ViewValue,
{
    /// Retrieves the value for a specific key at the given snapshot sequence.
    ///
    /// Returns the most recent version with sequence ≤ `snapshot_seq`,
    /// or `SeqMarked::new_not_found()` if no such version exists.
    async fn get(&self, key: K, snapshot_seq: u64) -> Result<SeqMarked<V>, io::Error>;

    /// Retrieves multiple keys atomically at the given snapshot sequence.
    ///
    /// Results maintain the same order as input keys. Default implementation calls `get()` sequentially.
    async fn get_many(
        &self,
        keys: Vec<K>,
        snapshot_seq: u64,
    ) -> Result<Vec<SeqMarked<V>>, io::Error> {
        let mut values = Vec::with_capacity(keys.len());
        for key in keys {
            let value = self.get(key, snapshot_seq).await?;
            values.push(value);
        }
        Ok(values)
    }
}

#[cfg(test)]
mod tests {
    use seq_marked::SeqMarked;

    use super::*;
    use crate::mvcc::table::Table;

    #[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
    struct TestKey(String);

    #[derive(Debug, Clone, PartialEq, Eq)]
    struct TestValue(String);

    fn key(s: &str) -> TestKey {
        TestKey(s.to_string())
    }

    fn value(s: &str) -> TestValue {
        TestValue(s.to_string())
    }

    // Mock implementation for testing scoped snapshot get
    struct MockScopedSnapshotReader {
        table: Table<TestKey, TestValue>,
    }

    impl MockScopedSnapshotReader {
        fn new() -> Self {
            let mut table = Table::new();
            table.insert(key("k1"), 1, value("v1")).unwrap();
            table.insert(key("k2"), 2, value("v2")).unwrap();
            table.insert_tombstone(key("k3"), 3).unwrap();
            table.insert(key("k5"), 5, value("v5")).unwrap();
            table.insert(key("k6"), 8, value("v6")).unwrap();
            table.insert(key("k4"), 10, value("v4")).unwrap();

            Self { table }
        }
    }

    #[async_trait::async_trait]
    impl ScopedSeqBoundedGet<TestKey, TestValue> for MockScopedSnapshotReader {
        async fn get(
            &self,
            key: TestKey,
            snapshot_seq: u64,
        ) -> Result<SeqMarked<TestValue>, io::Error> {
            let result = self.table.get(key, snapshot_seq);
            Ok(result.cloned())
        }
    }

    #[tokio::test]
    async fn test_get_basic() {
        let reader = MockScopedSnapshotReader::new();
        let res = reader.get(key("k1"), 10).await.unwrap();
        assert_eq!(res, SeqMarked::new_normal(1, value("v1")));
    }

    #[tokio::test]
    async fn test_get_many_mixed_results() {
        let reader = MockScopedSnapshotReader::new();
        let keys = vec![key("k1"), key("nx"), key("k3"), key("k5")];
        let res = reader.get_many(keys, 10).await.unwrap();
        let expected = vec![
            SeqMarked::new_normal(1, value("v1")),
            SeqMarked::new_not_found(),
            SeqMarked::new_tombstone(3),
            SeqMarked::new_normal(5, value("v5")),
        ];
        assert_eq!(res, expected);
    }

    #[tokio::test]
    async fn test_get_many_empty() {
        let reader = MockScopedSnapshotReader::new();
        let res = reader.get_many(vec![], 10).await.unwrap();
        assert_eq!(res, vec![]);
    }

    #[tokio::test]
    async fn test_get_many_duplicates() {
        let reader = MockScopedSnapshotReader::new();
        let keys = vec![key("k1"), key("k1"), key("k2")];
        let res = reader.get_many(keys, 10).await.unwrap();
        let expected = vec![
            SeqMarked::new_normal(1, value("v1")),
            SeqMarked::new_normal(1, value("v1")),
            SeqMarked::new_normal(2, value("v2")),
        ];
        assert_eq!(res, expected);
    }

    #[tokio::test]
    async fn test_get_many_sequence_filtering() {
        let reader = MockScopedSnapshotReader::new();
        let keys = vec![key("k1"), key("k4"), key("k5")];
        let res = reader.get_many(keys, 4).await.unwrap();
        let expected = vec![
            SeqMarked::new_normal(1, value("v1")),
            SeqMarked::new_not_found(), // seq 10 > 4
            SeqMarked::new_not_found(), // seq 5 > 4
        ];
        assert_eq!(res, expected);
    }

    #[tokio::test]
    async fn test_get_many_maintains_order() {
        let reader = MockScopedSnapshotReader::new();
        let keys = vec![key("k6"), key("k1"), key("k5")];
        let res = reader.get_many(keys, 10).await.unwrap();
        let expected = vec![
            SeqMarked::new_normal(8, value("v6")),
            SeqMarked::new_normal(1, value("v1")),
            SeqMarked::new_normal(5, value("v5")),
        ];
        assert_eq!(res, expected);
    }
}
