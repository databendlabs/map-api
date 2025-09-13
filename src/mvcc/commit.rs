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

use crate::mvcc::key::ViewKey;
use crate::mvcc::table::Table;
use crate::mvcc::value::ViewValue;
use crate::mvcc::view_namespace::ViewNamespace;

/// Commits staged changes to persistent storage.
#[async_trait::async_trait]
pub trait Commit<S, K, V>
where
    S: ViewNamespace,
    K: ViewKey,
    V: ViewValue,
{
    /// Apply staged changes to underlying storage.
    ///
    /// # Parameters
    /// - `last_seq`: Highest sequence number in the changes
    /// - `changes`: Pending modifications organized by namespace
    async fn commit(
        &mut self,
        last_seq: InternalSeq,
        changes: BTreeMap<S, Table<K, V>>,
    ) -> Result<(), io::Error>;
}

#[async_trait::async_trait]
impl<S, K, V> Commit<S, K, V> for BTreeMap<S, Table<K, V>>
where
    Self: Send + Sync,
    S: ViewNamespace,
    K: ViewKey,
    V: ViewValue,
{
    async fn commit(
        &mut self,
        last_seq: InternalSeq,
        changes: BTreeMap<S, Table<K, V>>,
    ) -> Result<(), io::Error> {
        let _ = last_seq;
        for (space, table) in changes {
            // insert the table if absent
            self.entry(space).or_default();

            let dst = self.get_mut(&space).unwrap();

            dst.apply(table);
        }
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
        A,
        B,
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

    fn k(s: &str) -> TestKey {
        TestKey(s.to_string())
    }

    fn v(s: &str) -> TestValue {
        TestValue(s.to_string())
    }

    fn test_btreemap() -> BTreeMap<TestSpace, Table<TestKey, TestValue>> {
        let mut table = Table::new();
        table.insert(k("k1"), 1, v("v1")).unwrap();
        table.insert(k("k2"), 2, v("v2")).unwrap();

        let mut map = BTreeMap::new();
        map.insert(TestSpace::A, table);
        map
    }

    #[tokio::test]
    async fn test_commit_empty() {
        let mut map = test_btreemap();

        map.commit(InternalSeq::new(0), BTreeMap::new())
            .await
            .unwrap();

        assert_eq!(map.len(), 1);
        let table = &map[&TestSpace::A];
        assert_eq!(table.get(k("k1"), 100), SeqMarked::new_normal(1, &v("v1")));
    }

    #[tokio::test]
    async fn test_commit_new_space() {
        let mut map = test_btreemap();
        let mut table = Table::new();
        table.insert(k("k3"), 5, v("v3")).unwrap();

        let mut changes = BTreeMap::new();
        changes.insert(TestSpace::B, table);

        map.commit(InternalSeq::new(10), changes).await.unwrap();

        assert_eq!(map.len(), 2);
        let table_b = &map[&TestSpace::B];
        assert_eq!(
            table_b.get(k("k3"), 100),
            SeqMarked::new_normal(5, &v("v3"))
        );
    }

    #[tokio::test]
    async fn test_commit_existing_space() {
        let mut map = test_btreemap();
        let mut table = Table::new();
        table.insert(k("k1"), 5, v("v1_new")).unwrap();
        table.insert(k("k3"), 6, v("v3")).unwrap();
        table.insert_tombstone(k("k2"), 7).unwrap();

        let mut changes = BTreeMap::new();
        changes.insert(TestSpace::A, table);

        map.commit(InternalSeq::new(10), changes).await.unwrap();

        let table_a = &map[&TestSpace::A];
        assert_eq!(
            table_a.get(k("k1"), 100),
            SeqMarked::new_normal(5, &v("v1_new"))
        );
        assert_eq!(table_a.get(k("k2"), 100), SeqMarked::new_tombstone(7));
        assert_eq!(
            table_a.get(k("k3"), 100),
            SeqMarked::new_normal(6, &v("v3"))
        );
    }

    #[tokio::test]
    async fn test_commit_multiple_spaces() {
        let mut map = test_btreemap();

        let mut table_a = Table::new();
        table_a.insert(k("k3"), 10, v("v3")).unwrap();

        let mut table_b = Table::new();
        table_b.insert(k("k4"), 11, v("v4")).unwrap();

        let mut changes = BTreeMap::new();
        changes.insert(TestSpace::A, table_a);
        changes.insert(TestSpace::B, table_b);

        map.commit(InternalSeq::new(15), changes).await.unwrap();

        assert_eq!(map.len(), 2);
        let table_a = &map[&TestSpace::A];
        let table_b = &map[&TestSpace::B];

        assert_eq!(
            table_a.get(k("k3"), 100),
            SeqMarked::new_normal(10, &v("v3"))
        );
        assert_eq!(
            table_b.get(k("k4"), 100),
            SeqMarked::new_normal(11, &v("v4"))
        );
    }

    #[tokio::test]
    async fn test_commit_versioning() {
        let mut map = test_btreemap();
        let mut table = Table::new();
        table.insert(k("k1"), 10, v("new")).unwrap();

        let mut changes = BTreeMap::new();
        changes.insert(TestSpace::A, table);

        map.commit(InternalSeq::new(15), changes).await.unwrap();

        let table_a = &map[&TestSpace::A];
        assert_eq!(
            table_a.get(k("k1"), 100),
            SeqMarked::new_normal(10, &v("new"))
        );
        assert_eq!(table_a.get(k("k1"), 5), SeqMarked::new_normal(1, &v("v1")));
    }

    #[tokio::test]
    #[should_panic(expected = "assertion failed: self.last_seq <= last_seq")]
    async fn test_commit_sequence_conflict() {
        let mut map = test_btreemap();

        let mut table1 = Table::new();
        table1.insert(k("k3"), 10, v("v3")).unwrap();
        let mut changes1 = BTreeMap::new();
        changes1.insert(TestSpace::A, table1);
        map.commit(InternalSeq::new(15), changes1).await.unwrap();

        let mut table2 = Table::new();
        table2.insert(k("k4"), 5, v("v4")).unwrap();
        let mut changes2 = BTreeMap::new();
        changes2.insert(TestSpace::A, table2);
        map.commit(InternalSeq::new(20), changes2).await.unwrap();
    }

    #[tokio::test]
    async fn test_commit_empty_tables() {
        let mut map = BTreeMap::new();
        let empty_table = Table::<TestKey, TestValue>::new();

        let mut changes = BTreeMap::new();
        changes.insert(TestSpace::A, empty_table);

        map.commit(InternalSeq::new(0), changes).await.unwrap();

        assert_eq!(map.len(), 1);
        let table_a = &map[&TestSpace::A];
        assert_eq!(table_a.last_seq, SeqMarked::zero());
        assert!(table_a.get(k("any"), 100).is_not_found());
    }

    #[tokio::test]
    async fn test_commit_only_tombstones() {
        let mut map = test_btreemap();
        let mut table = Table::new();
        table.insert_tombstone(k("k1"), 5).unwrap();
        table.insert_tombstone(k("k_new"), 6).unwrap();

        let mut changes = BTreeMap::new();
        changes.insert(TestSpace::A, table);

        map.commit(InternalSeq::new(10), changes).await.unwrap();

        let table_a = &map[&TestSpace::A];
        assert_eq!(table_a.get(k("k1"), 100), SeqMarked::new_tombstone(5));
        assert_eq!(table_a.get(k("k_new"), 100), SeqMarked::new_tombstone(6));
    }
}
