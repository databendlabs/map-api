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

use crate::mvcc::snapshot::Snapshot;
use crate::mvcc::table::Table;

pub type Tables<S, K, V> = BTreeMap<S, Table<K, V>>;

pub type TablesSnapshot<S, K, V> = Snapshot<S, K, V, BTreeMap<S, Table<K, V>>>;

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;

    use futures_util::StreamExt;
    use seq_marked::InternalSeq;
    use seq_marked::SeqMarked;

    use super::*;
    use crate::mvcc::ViewNamespace;

    // Test types that implement the required traits
    #[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
    enum TestSpace {
        Space1,
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
    async fn test_view_seq() {
        let mut tables = BTreeMap::new();
        tables.insert(TestSpace::Space1, create_test_table());

        let view = TablesSnapshot::new(InternalSeq::new(5), tables);

        assert_eq!(view.snapshot_seq(), InternalSeq::new(5));
    }

    #[tokio::test]
    async fn test_mget_existing_space() {
        let mut tables = BTreeMap::new();
        tables.insert(TestSpace::Space1, create_test_table());

        let view = TablesSnapshot::new(InternalSeq::new(10), tables);

        let keys = vec![
            key("k1"),
            key("k2"),
            key("k3"),
            key("k4"),
            key("k5"),
            key("k6"),
        ];
        let result = view.get_many(TestSpace::Space1, keys).await.unwrap();

        assert_eq!(result.len(), 6);
        assert_eq!(result[0], SeqMarked::new_normal(1, value("v1")));
        assert_eq!(result[1], SeqMarked::new_normal(2, value("v2")));
        assert_eq!(result[2], SeqMarked::new_normal(3, value("v3")));
        assert_eq!(result[3], SeqMarked::new_tombstone(4));
        assert_eq!(result[4], SeqMarked::new_tombstone(6)); // Latest version is tombstone
        assert_eq!(result[5], SeqMarked::new_tombstone(8)); // Latest version is tombstone
    }

    #[tokio::test]
    async fn test_mget_with_tombstone_base_seq_after_tombstone() {
        let mut tables = BTreeMap::new();
        tables.insert(TestSpace::Space1, create_test_table());

        let view = TablesSnapshot::new(InternalSeq::new(6), tables);

        let keys = vec![
            key("k1"),
            key("k2"),
            key("k3"),
            key("k4"),
            key("k5"),
            key("k6"),
        ];
        let result = view.get_many(TestSpace::Space1, keys).await.unwrap();

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

        let view = TablesSnapshot::new(InternalSeq::new(8), tables);

        let keys = vec![
            key("k1"),
            key("k2"),
            key("k3"),
            key("k4"),
            key("k5"),
            key("k6"),
        ];
        let result = view.get_many(TestSpace::Space1, keys).await.unwrap();

        assert_eq!(result.len(), 6);
        assert_eq!(result[0], SeqMarked::new_normal(1, value("v1")));
        assert_eq!(result[1], SeqMarked::new_normal(2, value("v2")));
        assert_eq!(result[2], SeqMarked::new_normal(3, value("v3")));
        assert_eq!(result[3], SeqMarked::new_tombstone(4));
        assert_eq!(result[4], SeqMarked::new_tombstone(6)); // Can see the tombstone
        assert_eq!(result[5], SeqMarked::new_tombstone(8)); // Can see the tombstone
    }

    #[tokio::test]
    async fn test_range_existing_space() {
        let mut tables = BTreeMap::new();
        tables.insert(TestSpace::Space1, create_test_table());

        let view = TablesSnapshot::new(InternalSeq::new(10), tables);

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
}
