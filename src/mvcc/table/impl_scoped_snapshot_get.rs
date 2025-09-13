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

use seq_marked::SeqMarked;

use super::Table;
use crate::mvcc::scoped_seq_bounded_get::ScopedSeqBoundedGet;
use crate::mvcc::ViewKey;
use crate::mvcc::ViewValue;

#[async_trait::async_trait]
impl<K, V> ScopedSeqBoundedGet<K, V> for Table<K, V>
where
    K: ViewKey,
    V: ViewValue,
{
    async fn get(&self, key: K, snapshot_seq: u64) -> Result<SeqMarked<V>, io::Error> {
        let result = self.get(key, snapshot_seq);
        Ok(result.cloned())
    }
}

#[cfg(test)]
mod tests {
    use seq_marked::SeqMarked;

    use super::*;
    use crate::mvcc::scoped_seq_bounded_get::ScopedSeqBoundedGet;

    #[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
    struct TestKey(String);

    #[derive(Debug, Clone, PartialEq, Eq)]
    struct TestValue(String);

    fn key(s: &str) -> TestKey {
        TestKey(s.to_string())
    }

    fn value(s: &str) -> TestValue {
        TestValue(s.to_string())
    }

    fn create_test_table() -> Table<TestKey, TestValue> {
        let mut table = Table::new();
        table.insert(key("key1"), 1, value("value1")).unwrap();
        table.insert(key("key2"), 2, value("value2")).unwrap();
        table.insert_tombstone(key("key3"), 3).unwrap();
        table.insert(key("key1"), 5, value("value1_v2")).unwrap(); // Multiple versions - must be after seq 3
        table.insert(key("key4"), 10, value("value4")).unwrap();
        table
    }

    #[tokio::test]
    async fn test_scoped_snapshot_reader_get_existing() {
        let table = create_test_table();
        let snapshot_seq = 10;

        let result = ScopedSeqBoundedGet::get(&table, key("key1"), snapshot_seq)
            .await
            .unwrap();
        assert_eq!(result, SeqMarked::new_normal(5, value("value1_v2"))); // Most recent version
        assert!(*result.internal_seq() <= snapshot_seq);
    }

    #[tokio::test]
    async fn test_scoped_snapshot_reader_get_tombstone() {
        let table = create_test_table();
        let snapshot_seq = 10;

        let result = ScopedSeqBoundedGet::get(&table, key("key3"), snapshot_seq)
            .await
            .unwrap();
        assert_eq!(result, SeqMarked::new_tombstone(3));
        assert!(*result.internal_seq() <= snapshot_seq);
    }

    #[tokio::test]
    async fn test_scoped_snapshot_reader_get_not_found() {
        let table = create_test_table();
        let snapshot_seq = 10;

        let result = ScopedSeqBoundedGet::get(&table, key("nonexistent"), snapshot_seq)
            .await
            .unwrap();
        assert_eq!(result, SeqMarked::new_not_found());
    }

    #[tokio::test]
    async fn test_scoped_snapshot_reader_get_sequence_filter() {
        let table = create_test_table();

        // key4 has seq 10, should be visible with snapshot_seq 10
        let snapshot_seq1 = 10;
        let result = ScopedSeqBoundedGet::get(&table, key("key4"), snapshot_seq1)
            .await
            .unwrap();
        assert_eq!(result, SeqMarked::new_normal(10, value("value4")));
        assert!(*result.internal_seq() <= snapshot_seq1);

        // key4 has seq 10, should not be visible with snapshot_seq 9
        let snapshot_seq2 = 9;
        let result = ScopedSeqBoundedGet::get(&table, key("key4"), snapshot_seq2)
            .await
            .unwrap();
        assert_eq!(result, SeqMarked::new_not_found());

        // key1 with snapshot_seq 3 should return version 1, not version 5
        let snapshot_seq3 = 3;
        let result = ScopedSeqBoundedGet::get(&table, key("key1"), snapshot_seq3)
            .await
            .unwrap();
        assert_eq!(result, SeqMarked::new_normal(1, value("value1")));
        assert!(*result.internal_seq() <= snapshot_seq3);
    }

    #[tokio::test]
    async fn test_scoped_snapshot_reader_mget() {
        let table = create_test_table();
        let keys = vec![key("key1"), key("key2"), key("nonexistent")];
        let snapshot_seq = 10;

        let results = ScopedSeqBoundedGet::get_many(&table, keys, snapshot_seq)
            .await
            .unwrap();

        assert_eq!(results.len(), 3);
        assert_eq!(results[0], SeqMarked::new_normal(5, value("value1_v2")));
        assert!(*results[0].internal_seq() <= snapshot_seq);
        assert_eq!(results[1], SeqMarked::new_normal(2, value("value2")));
        assert!(*results[1].internal_seq() <= snapshot_seq);
        assert_eq!(results[2], SeqMarked::new_not_found());
    }

    #[tokio::test]
    async fn test_scoped_snapshot_reader_mget_empty() {
        let table = create_test_table();
        let keys = vec![];
        let snapshot_seq = 10;

        let results = ScopedSeqBoundedGet::get_many(&table, keys, snapshot_seq)
            .await
            .unwrap();
        assert_eq!(results.len(), 0);
    }

    #[tokio::test]
    async fn test_scoped_snapshot_reader_mget_sequence_filter() {
        let table = create_test_table();
        let keys = vec![key("key1"), key("key4")]; // key1 has multiple versions, key4 seq=10
        let snapshot_seq = 5;

        let results = ScopedSeqBoundedGet::get_many(&table, keys, snapshot_seq)
            .await
            .unwrap();

        assert_eq!(results.len(), 2);
        assert_eq!(results[0], SeqMarked::new_normal(5, value("value1_v2"))); // Latest visible version
        assert!(*results[0].internal_seq() <= snapshot_seq);
        assert_eq!(results[1], SeqMarked::new_not_found()); // key4 not visible due to seq filter
    }

    #[tokio::test]
    async fn test_scoped_snapshot_reader_multiversion_behavior() {
        let mut table = Table::new();

        // Insert multiple versions of the same key
        table.insert(key("key"), 1, value("v1")).unwrap();
        table.insert(key("key"), 3, value("v3")).unwrap();
        table.insert(key("key"), 5, value("v5")).unwrap();

        // Test different snapshot points
        let result = ScopedSeqBoundedGet::get(&table, key("key"), 2)
            .await
            .unwrap();
        assert_eq!(result, SeqMarked::new_normal(1, value("v1")));

        let result = ScopedSeqBoundedGet::get(&table, key("key"), 4)
            .await
            .unwrap();
        assert_eq!(result, SeqMarked::new_normal(3, value("v3")));

        let result = ScopedSeqBoundedGet::get(&table, key("key"), 10)
            .await
            .unwrap();
        assert_eq!(result, SeqMarked::new_normal(5, value("v5")));
    }
}
