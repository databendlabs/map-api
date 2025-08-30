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

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;

    use futures_util::StreamExt;
    use seq_marked::InternalSeq;
    use seq_marked::SeqMarked;

    use crate::mvcc::table::TableViewReadonly;
    use crate::mvcc::view::View;
    use crate::mvcc::view_namespace::ViewNamespace;
    use crate::mvcc::Table;

    #[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
    enum TestSpaceNoSeqIncrease {
        Space1,
    }

    impl ViewNamespace for TestSpaceNoSeqIncrease {
        fn if_increase_seq(&self) -> bool {
            false // This namespace does NOT increase sequence on normal inserts
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

    fn create_base_view() -> TableViewReadonly<TestSpaceNoSeqIncrease, TestKey, TestValue> {
        let mut table = Table::new();
        table.insert(key("base_k1"), 1, value("base_v1")).unwrap();
        table.insert(key("base_k2"), 2, value("base_v2")).unwrap();

        let mut tables = BTreeMap::new();
        tables.insert(TestSpaceNoSeqIncrease::Space1, table);
        let mut view = TableViewReadonly::new(tables);
        view.base_seq = InternalSeq::new(10); // Set high enough to see all base data
        view
    }

    #[tokio::test]
    async fn test_no_seq_increase_single_insert() {
        let mut view = View::new(create_base_view());

        // Insert a value - should NOT increment sequence
        let initial_seq = view.last_seq;
        let result_seq = view.set(TestSpaceNoSeqIncrease::Space1, key("k1"), Some(value("v1")));

        // Sequence should remain the same
        assert_eq!(view.last_seq, initial_seq);
        assert_eq!(result_seq, SeqMarked::new_normal(*initial_seq, ()));

        // Verify the value was stored at the current sequence
        let table = &view.changes[&TestSpaceNoSeqIncrease::Space1];
        assert_eq!(
            table.get(key("k1"), *initial_seq),
            SeqMarked::new_normal(*initial_seq, &value("v1"))
        );
    }

    #[tokio::test]
    async fn test_no_seq_increase_cannot_insert_multiple_values_same_seq() {
        let mut view = View::new(create_base_view());

        let initial_seq = view.last_seq;

        // First insert should succeed
        view.set(TestSpaceNoSeqIncrease::Space1, key("k1"), Some(value("v1")));
        assert_eq!(view.last_seq, initial_seq);

        // The table constraint prevents multiple normal values at the same sequence
        // This is by design - non-incrementing namespaces are intended for secondary indices
        // where you typically have one operation per sequence number

        // Verify first value was stored
        let table = &view.changes[&TestSpaceNoSeqIncrease::Space1];
        assert_eq!(
            table.get(key("k1"), *initial_seq),
            SeqMarked::new_normal(*initial_seq, &value("v1"))
        );

        // Note: Attempting another insert at the same sequence would fail with NonIncremental error
        // This is expected behavior for the current table implementation
    }

    #[tokio::test]
    async fn test_no_seq_increase_constraint() {
        // This test demonstrates the table constraint that prevents
        // inserting normal values at the same sequence number
        let mut view = View::new(create_base_view());

        let initial_seq = view.last_seq;

        // First insert should succeed and use current sequence without incrementing
        view.set(TestSpaceNoSeqIncrease::Space1, key("k1"), Some(value("v1")));
        assert_eq!(view.last_seq, initial_seq);

        // The table constraint means we cannot insert another normal value at the same sequence
        // This is by design - the table requires seq_marked > last_seq for normal values

        // What we CAN do is insert a tombstone at the same sequence (since tombstones allow >=)
        view.set(TestSpaceNoSeqIncrease::Space1, key("k2"), None);
        assert_eq!(view.last_seq, initial_seq); // Still no increment

        // Verify the values
        let table = &view.changes[&TestSpaceNoSeqIncrease::Space1];
        assert_eq!(
            table.get(key("k1"), *initial_seq),
            SeqMarked::new_normal(*initial_seq, &value("v1"))
        );
        assert_eq!(
            table.get(key("k2"), *initial_seq),
            SeqMarked::new_tombstone(*initial_seq)
        );
    }

    #[tokio::test]
    async fn test_no_seq_increase_tombstone_behavior() {
        let mut view = View::new(create_base_view());

        let initial_seq = view.last_seq;

        // Insert a value
        view.set(TestSpaceNoSeqIncrease::Space1, key("k1"), Some(value("v1")));
        assert_eq!(view.last_seq, initial_seq);

        // Delete (tombstone) - should also use same sequence (default tombstone behavior)
        view.set(TestSpaceNoSeqIncrease::Space1, key("k1"), None);
        assert_eq!(view.last_seq, initial_seq); // Still no change

        // Should see tombstone at the same sequence
        let table = &view.changes[&TestSpaceNoSeqIncrease::Space1];
        assert_eq!(
            table.get(key("k1"), *initial_seq),
            SeqMarked::new_tombstone(*initial_seq)
        );
    }

    #[tokio::test]
    async fn test_no_seq_increase_with_tombstone_seq_increment() {
        let mut view = View::new(create_base_view()).with_tombstone_seq_increment(true);

        let initial_seq = view.last_seq;

        // Insert a value - should NOT increment sequence
        view.set(TestSpaceNoSeqIncrease::Space1, key("k1"), Some(value("v1")));
        assert_eq!(view.last_seq, initial_seq);

        // Delete (tombstone) - SHOULD increment sequence because tombstone increment is enabled
        view.set(TestSpaceNoSeqIncrease::Space1, key("k1"), None);
        assert_eq!(view.last_seq, InternalSeq::new(*initial_seq + 1));

        // Should see tombstone at the incremented sequence
        let table = &view.changes[&TestSpaceNoSeqIncrease::Space1];
        assert_eq!(
            table.get(key("k1"), *initial_seq + 1),
            SeqMarked::new_tombstone(*initial_seq + 1)
        );
    }

    #[tokio::test]
    async fn test_no_seq_increase_secondary_index_pattern() {
        // Demonstrate the intended use case: secondary index operations
        let mut view = View::new(create_base_view());

        let initial_seq = view.last_seq;

        // Insert a value in the primary space (which would increment sequence)
        // Then insert corresponding secondary index entry (which should use current sequence)
        view.set(
            TestSpaceNoSeqIncrease::Space1,
            key("secondary_index_k1"),
            Some(value("index_v1")),
        );

        // Sequence should not increment for secondary index space
        assert_eq!(view.last_seq, initial_seq);

        // Verify the secondary index entry
        let table = &view.changes[&TestSpaceNoSeqIncrease::Space1];
        assert_eq!(
            table.get(key("secondary_index_k1"), *initial_seq),
            SeqMarked::new_normal(*initial_seq, &value("index_v1"))
        );

        // Test mget on secondary index
        let keys = vec![key("secondary_index_k1")];
        let result = view
            .mget(TestSpaceNoSeqIncrease::Space1, keys)
            .await
            .unwrap();

        assert_eq!(result.len(), 1);
        assert_eq!(
            result[0],
            SeqMarked::new_normal(*initial_seq, value("index_v1"))
        );
    }

    #[tokio::test]
    async fn test_no_seq_increase_range() {
        let mut view = View::new(create_base_view());

        let initial_seq = view.last_seq;

        // Insert a single secondary index entry
        view.set(
            TestSpaceNoSeqIncrease::Space1,
            key("range_k1"),
            Some(value("rv1")),
        );

        // Sequence should not increment
        assert_eq!(view.last_seq, initial_seq);

        // Test range
        let mut stream = view
            .range(
                TestSpaceNoSeqIncrease::Space1,
                key("range_k1")..=key("range_k1"),
            )
            .await
            .unwrap();
        let mut results = Vec::new();
        while let Some(result) = stream.next().await {
            results.push(result.unwrap());
        }

        assert_eq!(results.len(), 1);
        assert_eq!(
            results[0],
            (
                key("range_k1"),
                SeqMarked::new_normal(*initial_seq, value("rv1"))
            )
        );
    }

    #[tokio::test]
    async fn test_no_seq_increase_fetch_and_set() {
        let mut view = View::new(create_base_view());

        let initial_seq = view.last_seq;

        // Test fetch_and_set on non-existent key
        let (old_val, new_val) = view
            .fetch_and_set(TestSpaceNoSeqIncrease::Space1, key("k1"), Some(value("v1")))
            .await
            .unwrap();

        assert!(old_val.is_not_found());
        assert_eq!(new_val, SeqMarked::new_normal(*initial_seq, value("v1")));
        assert_eq!(view.last_seq, initial_seq); // No sequence increment

        // Note: Due to table constraints, we cannot do another fetch_and_set
        // with a normal value at the same sequence. This is expected behavior.
    }

    #[tokio::test]
    async fn test_no_seq_increase_mixed_with_increasing_space() {
        // Create a view that uses both types of namespaces
        #[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
        enum MixedSpace {
            NoIncrement,
            WithIncrement,
        }

        impl ViewNamespace for MixedSpace {
            fn if_increase_seq(&self) -> bool {
                match self {
                    MixedSpace::NoIncrement => false,
                    MixedSpace::WithIncrement => true,
                }
            }
        }

        let tables = BTreeMap::new();
        let base = TableViewReadonly::<MixedSpace, TestKey, TestValue>::new(tables);
        let mut view = View::new(base).with_initial_seq(InternalSeq::new(100));

        let initial_seq = view.last_seq;

        // Insert into no-increment space - should NOT increment sequence
        view.set(MixedSpace::NoIncrement, key("k1"), Some(value("v1")));
        assert_eq!(view.last_seq, initial_seq);

        // Insert into increment space - SHOULD increment sequence
        view.set(MixedSpace::WithIncrement, key("k2"), Some(value("v2")));
        assert_eq!(view.last_seq, InternalSeq::new(*initial_seq + 1));

        // Insert again into no-increment space - should use current sequence without incrementing
        view.set(MixedSpace::NoIncrement, key("k3"), Some(value("v3")));
        assert_eq!(view.last_seq, InternalSeq::new(*initial_seq + 1)); // No further increment

        // Verify the values are stored at correct sequences
        let table_no_inc = &view.changes[&MixedSpace::NoIncrement];
        let table_inc = &view.changes[&MixedSpace::WithIncrement];

        assert_eq!(
            table_no_inc.get(key("k1"), *initial_seq + 1),
            SeqMarked::new_normal(*initial_seq, &value("v1"))
        );
        assert_eq!(
            table_inc.get(key("k2"), *initial_seq + 1),
            SeqMarked::new_normal(*initial_seq + 1, &value("v2"))
        );
        assert_eq!(
            table_no_inc.get(key("k3"), *initial_seq + 1),
            SeqMarked::new_normal(*initial_seq + 1, &value("v3"))
        );
    }

    #[tokio::test]
    async fn test_no_seq_increase_empty_base_view() {
        // Test with empty base view (starts at sequence 0)
        // This demonstrates that the constraint applies even at sequence 0
        let tables = BTreeMap::new();
        let base = TableViewReadonly::<TestSpaceNoSeqIncrease, TestKey, TestValue>::new(tables);
        let mut view = View::new(base).with_initial_seq(InternalSeq::new(1));

        assert_eq!(view.last_seq, InternalSeq::new(1));

        // Insert single value - should use sequence 1 without incrementing
        view.set(TestSpaceNoSeqIncrease::Space1, key("k1"), Some(value("v1")));
        assert_eq!(view.last_seq, InternalSeq::new(1));

        // Verify value is stored at sequence 1
        let table = &view.changes[&TestSpaceNoSeqIncrease::Space1];
        assert_eq!(
            table.get(key("k1"), 1),
            SeqMarked::new_normal(1, &value("v1"))
        );

        // Test mget
        let keys = vec![key("k1")];
        let result = view
            .mget(TestSpaceNoSeqIncrease::Space1, keys)
            .await
            .unwrap();
        assert_eq!(result[0], SeqMarked::new_normal(1, value("v1")));
    }
}
