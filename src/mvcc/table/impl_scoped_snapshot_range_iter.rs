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

use std::ops::RangeBounds;

use seq_marked::SeqMarked;

use super::Table;
use crate::mvcc::scoped_snapshot_range_iter::ScopedSnapshotRangeIter;
use crate::mvcc::ViewKey;
use crate::mvcc::ViewValue;

impl<K, V> ScopedSnapshotRangeIter<K, V> for Table<K, V>
where
    K: ViewKey,
    V: ViewValue,
{
    fn range_iter<R>(
        &self,
        range: R,
        snapshot_seq: u64,
    ) -> impl Iterator<Item = (&K, SeqMarked<&V>)>
    where
        R: RangeBounds<K> + Clone + 'static,
    {
        self.range(range, snapshot_seq)
    }
}

impl<K, V, T> ScopedSnapshotRangeIter<K, V> for T
where
    K: ViewKey,
    V: ViewValue,
    T: AsRef<Table<K, V>>,
{
    fn range_iter<R>(
        &self,
        range: R,
        snapshot_seq: u64,
    ) -> impl Iterator<Item = (&K, SeqMarked<&V>)>
    where
        R: RangeBounds<K> + Clone + 'static,
    {
        self.as_ref().range_iter(range, snapshot_seq)
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use seq_marked::SeqMarked;

    use super::*;
    use crate::mvcc::scoped_snapshot_range_iter::ScopedSnapshotRangeIter;

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

    #[test]
    fn test_scoped_snapshot_range_iter_all_as_ref() {
        let table = create_test_table();
        let table = Arc::new(table);
        let snapshot_seq = 10;

        let iter = ScopedSnapshotRangeIter::range_iter(&table, .., snapshot_seq);

        assert_eq!(iter.collect::<Vec<_>>(), vec![
            (&key("key1"), SeqMarked::new_normal(5, &value("value1_v2"))),
            (&key("key2"), SeqMarked::new_normal(2, &value("value2"))),
            (&key("key3"), SeqMarked::new_tombstone(3)),
            (&key("key4"), SeqMarked::new_normal(10, &value("value4"))),
        ]);
    }

    #[test]
    fn test_scoped_snapshot_range_iter_all() {
        let table = create_test_table();
        let snapshot_seq = 10;

        let iter = ScopedSnapshotRangeIter::range_iter(&table, .., snapshot_seq);
        let results: Vec<_> = iter.collect();

        assert_eq!(results, vec![
            (&key("key1"), SeqMarked::new_normal(5, &value("value1_v2"))),
            (&key("key2"), SeqMarked::new_normal(2, &value("value2"))),
            (&key("key3"), SeqMarked::new_tombstone(3)),
            (&key("key4"), SeqMarked::new_normal(10, &value("value4"))),
        ]);
    }

    #[test]
    fn test_scoped_snapshot_range_iter_bounded() {
        let table = create_test_table();
        let snapshot_seq = 10;

        let iter =
            ScopedSnapshotRangeIter::range_iter(&table, key("key1")..=key("key2"), snapshot_seq);
        let results: Vec<_> = iter.collect();

        assert_eq!(results, vec![
            (&key("key1"), SeqMarked::new_normal(5, &value("value1_v2"))),
            (&key("key2"), SeqMarked::new_normal(2, &value("value2"))),
        ]);
    }

    #[test]
    fn test_scoped_snapshot_range_iter_sequence_filter() {
        let table = create_test_table();
        let snapshot_seq = 5;

        let iter = ScopedSnapshotRangeIter::range_iter(&table, .., snapshot_seq);
        let results: Vec<_> = iter.collect();

        assert_eq!(results, vec![
            (&key("key1"), SeqMarked::new_normal(5, &value("value1_v2"))),
            (&key("key2"), SeqMarked::new_normal(2, &value("value2"))),
            (&key("key3"), SeqMarked::new_tombstone(3)),
        ]);
    }

    #[test]
    fn test_scoped_snapshot_range_iter_empty_table() {
        let table: Table<TestKey, TestValue> = Table::new();
        let snapshot_seq = 10;

        let iter = ScopedSnapshotRangeIter::range_iter(&table, .., snapshot_seq);
        let results: Vec<_> = iter.collect();

        assert_eq!(results.len(), 0);
    }

    #[test]
    fn test_scoped_snapshot_range_iter_multiversion_behavior() {
        let mut table = Table::new();

        // Insert multiple versions of the same key
        table.insert(key("key"), 1, value("v1")).unwrap();
        table.insert(key("key"), 3, value("v3")).unwrap();
        table.insert(key("key"), 5, value("v5")).unwrap();

        // Test different snapshot points
        let iter = ScopedSnapshotRangeIter::range_iter(&table, .., 2);
        let results: Vec<_> = iter.collect();

        assert_eq!(results, vec![(
            &key("key"),
            SeqMarked::new_normal(1, &value("v1"))
        ),]);

        let iter = ScopedSnapshotRangeIter::range_iter(&table, .., 4);
        let results: Vec<_> = iter.collect();

        assert_eq!(results, vec![(
            &key("key"),
            SeqMarked::new_normal(3, &value("v3"))
        ),]);

        let iter = ScopedSnapshotRangeIter::range_iter(&table, .., 10);
        let results: Vec<_> = iter.collect();

        assert_eq!(results, vec![(
            &key("key"),
            SeqMarked::new_normal(5, &value("v5"))
        ),]);
    }
}
