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
use std::io;

use seq_marked::InternalSeq;

use crate::mvcc::commit::Commit;
use crate::mvcc::key::ViewKey;
use crate::mvcc::table::table_view_readonly::TableViewReadonly;
use crate::mvcc::table::Table;
use crate::mvcc::value::ViewValue;
use crate::mvcc::view_namespace::ViewNamespace;

#[async_trait::async_trait]
impl<S, K, V> Commit<S, K, V> for TableViewReadonly<S, K, V>
where
    S: ViewNamespace,
    K: ViewKey,
    V: ViewValue,
{
    async fn commit(
        &mut self,
        last_seq: InternalSeq,
        changes: BTreeMap<S, Table<K, V>>,
    ) -> Result<(), io::Error> {
        for (space, table_changes) in changes {
            let t = self.tables.entry(space).or_default();
            t.apply(table_changes);

            // Applied but does not visible.
            // self.base_seq = t.last_seq.internal_seq();
        }

        // Update the base sequence to the last sequence from the view
        self.base_seq = last_seq;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;

    use seq_marked::InternalSeq;
    use seq_marked::SeqMarked;

    use super::*;

    #[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
    enum TestSpace {
        Space1,
        Space2,
    }

    impl ViewNamespace for TestSpace {
        fn if_increase_seq(&self) -> bool {
            true
        }
    }

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

    fn test_view() -> TableViewReadonly<TestSpace, TestKey, TestValue> {
        let mut table = Table::new();
        table.insert(key("k1"), 1, value("v1")).unwrap();
        table.insert(key("k2"), 2, value("v2")).unwrap();

        let mut tables = BTreeMap::new();
        tables.insert(TestSpace::Space1, table);
        TableViewReadonly::new(tables)
    }

    #[tokio::test]
    async fn test_commit_empty() {
        let mut view = test_view();

        view.commit(InternalSeq::new(0), BTreeMap::new())
            .await
            .unwrap();

        assert_eq!(view.base_seq, InternalSeq::new(0));
        assert_eq!(view.tables.len(), 1);
    }

    #[tokio::test]
    async fn test_commit_new_space() {
        let mut view = test_view();
        let mut table = Table::new();
        table.insert(key("k1"), 10, value("v10")).unwrap();

        let mut changes = BTreeMap::new();
        changes.insert(TestSpace::Space2, table);

        view.commit(InternalSeq::new(7), changes).await.unwrap();

        assert_eq!(view.tables.len(), 2);
        let result = view.tables[&TestSpace::Space2].get(key("k1"), 100);
        assert_eq!(result, SeqMarked::new_normal(10, &value("v10")));
    }

    #[tokio::test]
    async fn test_commit_existing_space() {
        let mut view = test_view();
        let mut table = Table::new();
        table.insert(key("k1"), 5, value("v1_new")).unwrap(); // update
        table.insert(key("k3"), 6, value("v3")).unwrap(); // insert
        table.insert_tombstone(key("k2"), 7).unwrap(); // delete

        let mut changes = BTreeMap::new();
        changes.insert(TestSpace::Space1, table);

        view.commit(InternalSeq::new(7), changes).await.unwrap();

        let space1 = &view.tables[&TestSpace::Space1];
        assert_eq!(
            space1.get(key("k1"), 100),
            SeqMarked::new_normal(5, &value("v1_new"))
        );
        assert_eq!(space1.get(key("k2"), 100), SeqMarked::new_tombstone(7));
        assert_eq!(
            space1.get(key("k3"), 100),
            SeqMarked::new_normal(6, &value("v3"))
        );
    }

    #[tokio::test]
    async fn test_commit_updates_base_seq() {
        let mut view = test_view();
        view.base_seq = InternalSeq::new(42);

        let mut table = Table::new();
        table.insert(key("k1"), 100, value("v")).unwrap();
        let mut changes = BTreeMap::new();
        changes.insert(TestSpace::Space1, table);

        view.commit(InternalSeq::new(7), changes).await.unwrap();

        assert_eq!(view.base_seq, InternalSeq::new(7)); // updated to last_seq
    }

    #[tokio::test]
    async fn test_commit_versioning() {
        let mut view = test_view();
        let mut table = Table::new();
        table.insert(key("k1"), 10, value("new")).unwrap();

        let mut changes = BTreeMap::new();
        changes.insert(TestSpace::Space1, table);

        view.commit(InternalSeq::new(7), changes).await.unwrap();

        let space1 = &view.tables[&TestSpace::Space1];
        // High seq sees new version
        assert_eq!(
            space1.get(key("k1"), 100),
            SeqMarked::new_normal(10, &value("new"))
        );
        // Low seq sees original
        assert_eq!(
            space1.get(key("k1"), 5),
            SeqMarked::new_normal(1, &value("v1"))
        );
    }

    #[tokio::test]
    #[should_panic(expected = "assertion failed: self.last_seq <= last_seq")]
    async fn test_commit_sequence_conflict() {
        let mut view = test_view();

        // First commit with high sequence
        let mut table1 = Table::new();
        table1.insert(key("k1"), 10, value("v10")).unwrap();
        let mut changes1 = BTreeMap::new();
        changes1.insert(TestSpace::Space1, table1);
        view.commit(InternalSeq::new(10), changes1).await.unwrap();

        // Verify the first commit's data is accessible
        let space1 = &view.tables[&TestSpace::Space1];
        assert_eq!(
            space1.get(key("k1"), 100),
            SeqMarked::new_normal(10, &value("v10"))
        );
        // Original data should still be accessible at appropriate sequence
        assert_eq!(
            space1.get(key("k1"), 5),
            SeqMarked::new_normal(1, &value("v1"))
        );

        // Second commit with lower sequence - should panic
        let mut table2 = Table::new();
        table2.insert(key("k2"), 5, value("v5")).unwrap(); // last_seq = 5 < 10
        let mut changes2 = BTreeMap::new();
        changes2.insert(TestSpace::Space1, table2);
        view.commit(InternalSeq::new(5), changes2).await.unwrap(); // PANIC
    }

    #[tokio::test]
    async fn test_commit_empty_table() {
        let mut view = test_view();
        let empty_table = Table::new(); // last_seq = 0

        let mut changes = BTreeMap::new();
        changes.insert(TestSpace::Space1, empty_table);

        // Should panic because existing table has last_seq > 0
        let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            futures::executor::block_on(view.commit(InternalSeq::new(7), changes))
        }));

        assert!(result.is_err()); // Should panic
    }

    #[tokio::test]
    async fn test_commit_only_tombstones() {
        let mut view = test_view();
        let mut table = Table::new();
        table.insert_tombstone(key("k1"), 5).unwrap();
        table.insert_tombstone(key("k_new"), 6).unwrap();

        let mut changes = BTreeMap::new();
        changes.insert(TestSpace::Space1, table);

        view.commit(InternalSeq::new(7), changes).await.unwrap();

        let space1 = &view.tables[&TestSpace::Space1];
        assert_eq!(space1.get(key("k1"), 100), SeqMarked::new_tombstone(5));
        assert_eq!(space1.get(key("k_new"), 100), SeqMarked::new_tombstone(6));
    }

    #[tokio::test]
    async fn test_commit_multiple_same_space() {
        let mut view = test_view();

        // First commit
        let mut table1 = Table::new();
        table1.insert(key("k3"), 5, value("v3")).unwrap();
        let mut changes1 = BTreeMap::new();
        changes1.insert(TestSpace::Space1, table1);
        view.commit(InternalSeq::new(5), changes1).await.unwrap();

        // Verify first commit's data is accessible
        let space1 = &view.tables[&TestSpace::Space1];
        assert_eq!(
            space1.get(key("k3"), 100),
            SeqMarked::new_normal(5, &value("v3"))
        );
        // Original test data should still be there
        assert_eq!(
            space1.get(key("k1"), 100),
            SeqMarked::new_normal(1, &value("v1"))
        );
        assert_eq!(
            space1.get(key("k2"), 100),
            SeqMarked::new_normal(2, &value("v2"))
        );

        // Second commit to same space with higher seq
        let mut table2 = Table::new();
        table2.insert(key("k4"), 10, value("v4")).unwrap();
        let mut changes2 = BTreeMap::new();
        changes2.insert(TestSpace::Space1, table2);
        view.commit(InternalSeq::new(10), changes2).await.unwrap();

        let space1 = &view.tables[&TestSpace::Space1];
        assert_eq!(
            space1.get(key("k3"), 100),
            SeqMarked::new_normal(5, &value("v3"))
        );
        assert_eq!(
            space1.get(key("k4"), 100),
            SeqMarked::new_normal(10, &value("v4"))
        );
    }

    #[tokio::test]
    async fn test_commit_zero_sequences() {
        // Test edge case with zero sequence numbers
        let mut tables = BTreeMap::new();
        tables.insert(TestSpace::Space1, Table::<TestKey, TestValue>::new()); // empty table, last_seq = 0
        let mut view = TableViewReadonly::new(tables);

        let table = Table::<TestKey, TestValue>::new(); // also last_seq = 0
        let mut changes = BTreeMap::new();
        changes.insert(TestSpace::Space1, table);

        view.commit(InternalSeq::new(7), changes).await.unwrap(); // Should not panic (0 <= 0)

        // Verify the committed empty table state
        assert_eq!(view.tables.len(), 1);
        let space1 = &view.tables[&TestSpace::Space1];
        // Empty table should have last_seq of 0
        assert_eq!(space1.last_seq, SeqMarked::zero());
        // Querying any key should return not_found
        assert!(space1.get(key("any_key"), 100).is_not_found());
    }

    #[tokio::test]
    async fn test_commit_max_sequence() {
        // Test edge case with maximum sequence numbers
        let mut view = test_view();
        let mut table = Table::new();
        table.insert(key("k1"), u64::MAX, value("max_seq")).unwrap();

        let mut changes = BTreeMap::new();
        changes.insert(TestSpace::Space1, table);

        view.commit(InternalSeq::new(7), changes).await.unwrap();

        let space1 = &view.tables[&TestSpace::Space1];
        assert_eq!(
            space1.get(key("k1"), u64::MAX),
            SeqMarked::new_normal(u64::MAX, &value("max_seq"))
        );
    }
}
