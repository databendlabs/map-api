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
use std::fmt;
use std::io;
use std::ops::RangeBounds;

use futures_util::StreamExt;
use log::debug;
use seq_marked::InternalSeq;
use seq_marked::SeqMarked;
use stream_more::KMerge;
use stream_more::StreamMore;

use crate::compact::compact_seq_marked_pair;
use crate::mvcc::commit::Commit;
use crate::mvcc::key::ViewKey;
use crate::mvcc::seq_bounded_read::SeqBoundedRead;
use crate::mvcc::snapshot::Snapshot;
use crate::mvcc::table::Table;
use crate::mvcc::value::ViewValue;
use crate::mvcc::view_namespace::ViewNamespace;
use crate::util;
use crate::IOResultStream;

/// A read-write view that includes uncommitted changes from the current transaction.
///
/// Unlike `ViewRo`, this view's `get()` and `range()` methods return data that includes
/// any modifications made within this transaction.
///
/// # Isolation Level: Read Committed
///
/// This MVCC implementation provides **Read Committed** isolation level with the following guarantees:
///
/// ## ✅ Guarantees
/// - **Dirty Read Prevention**: Reads never see uncommitted data from other transactions
/// - **Write Consistency**: All writes within a transaction are atomic on commit
/// - **Snapshot Isolation**: Each transaction sees a consistent snapshot at start time
/// - **Staged Changes**: Local modifications are visible within the same transaction
///
/// ## ❌ Limitations  
/// - **Phantom Reads**: Range queries may see different results if re-executed
/// - **Non-Repeatable Reads**: Point queries may return different values if re-executed
/// - **Not Serializable**: Concurrent transactions may produce results impossible in any serial execution
///
/// ## ⚠️ Tombstone Anomaly
/// **Critical**: Due to historical implementation (tombstone insertions reuse sequence numbers),
/// delete operations from concurrent transactions may be visible within the same sequence boundary.
/// This can cause unexpected visibility of deletions.
///
/// ```text
/// Transaction A at seq=100: sees key "x" = "value"
/// Transaction B at seq=100: deletes key "x" (tombstone at seq=100)
/// Transaction A may now see key "x" as deleted, violating isolation
/// ```
///
/// # Concurrency Guidelines
///
/// ## ✅ Safe Patterns
/// ```rust,ignore
/// // Read-only transactions: Fully concurrent
/// let view = View::new(base_snapshot);
/// let result = view.get(space, key).await?; // Safe: no modifications
///
/// // Independent key spaces: Can run concurrently
/// // Transaction A: modifies space1
/// // Transaction B: modifies space2
///
/// // Non-overlapping key sets: Can run concurrently
/// // Transaction A: modifies keys [1..100]
/// // Transaction B: modifies keys [200..300]
/// ```
///
/// ## ⚠️ Requires Coordination
/// ```rust,ignore
/// // Overlapping writes: Serialize at application level
/// // Both transactions modify the same keys
///
/// // Read-modify-write: Use optimistic locking
/// let current = view.get(space, key).await?;
/// if current.seq() == expected_seq {
///     view.set(space, key, new_value);
///     view.commit().await?;
/// } else {
///     // Retry or abort
/// }
///
/// // Critical sections: Use external coordination (mutex, etc.)
/// ```
///
/// ## 🚫 Unsafe Patterns
/// ```rust,ignore
/// // DON'T: Assume repeatable reads
/// let value1 = view.get(space, key).await?;
/// // ... other operations ...
/// let value2 = view.get(space, key).await?;
/// // value1 != value2 possible!
///
/// // DON'T: Assume phantom read protection
/// let count1 = view.range(space, ..).count();
/// // ... other operations ...
/// let count2 = view.range(space, ..).count();
/// // count1 != count2 possible!
/// ```
///
/// # Recommendations
///
/// 1. **Serialize conflicting transactions** at the application level
/// 2. **Use optimistic locking** for read-modify-write patterns  
/// 3. **Keep transactions short** to minimize contention windows
/// 4. **Prefer read-only transactions** when possible for maximum concurrency
/// 5. **Test thoroughly** with concurrent workloads to identify race conditions
///
/// # Type Parameters
/// - `S`: the key space (AKA column family)
/// - `K`: key type  
/// - `V`: value type
/// - `BaseView`: the base view that this view is based on.
///   `BaseView` is usually a `ViewRo` that is created from the snapshot of the underlying storage.
pub struct View<S, K, V, D>
where
    S: ViewNamespace,
    K: ViewKey,
    V: ViewValue,
    D: SeqBoundedRead<S, K, V> + Commit<S, K, V>,
{
    /// Whether to increase the seq for tombstone insertion.
    ///
    /// For backward compatibility, the default is false.
    pub(crate) increase_seq_for_tombstone: bool,

    /// The changes that are staged for commit in each key space.
    pub(crate) changes: BTreeMap<S, Table<K, V>>,

    /// The [`InternalSeq`] of the latest update, which is stored in `changes`.
    ///
    /// This seq will be updated to the underlaying [`Table`] when the transaction is committed.
    pub(crate) last_seq: InternalSeq,

    pub(crate) snapshot: Snapshot<S, K, V, D>,
}

impl<S, K, V, D> fmt::Debug for View<S, K, V, D>
where
    S: ViewNamespace,
    K: ViewKey,
    V: ViewValue,
    D: SeqBoundedRead<S, K, V> + Commit<S, K, V>,
    D: fmt::Debug,
{
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("View")
            .field(
                "increase_seq_for_tombstone",
                &self.increase_seq_for_tombstone,
            )
            .field("changes", &self.changes)
            .field("last_seq", &self.last_seq)
            .field("snapshot", &self.snapshot)
            .finish()
    }
}

impl<S, K, V, D> View<S, K, V, D>
where
    S: ViewNamespace,
    K: ViewKey,
    V: ViewValue,
    D: SeqBoundedRead<S, K, V> + Commit<S, K, V>,
{
    pub fn new(snapshot: Snapshot<S, K, V, D>) -> Self {
        let seq = snapshot.snapshot_seq();
        Self {
            increase_seq_for_tombstone: false,
            changes: BTreeMap::new(),
            last_seq: seq,
            snapshot,
        }
    }

    pub fn with_tombstone_seq_increment(mut self, enable: bool) -> Self {
        self.increase_seq_for_tombstone = enable;
        self
    }

    pub fn with_initial_seq(mut self, seq: InternalSeq) -> Self {
        self.last_seq = seq;
        self
    }

    /// Return the reference to the snapshot this view is based on.
    pub fn snapshot(&self) -> &Snapshot<S, K, V, D> {
        &self.snapshot
    }

    fn current_normal_seq(&self) -> SeqMarked<()> {
        debug!("current_normal_seq: last_seq: {}", self.last_seq);
        SeqMarked::new_normal(*self.last_seq, ())
    }

    /// Return the next seq for inserting a normal value.
    fn next_normal_seq(&mut self) -> SeqMarked<()> {
        self.last_seq += 1;
        debug!("next_normal_seq: last_seq become: {}", self.last_seq);
        SeqMarked::new_normal(*self.last_seq, ())
    }

    /// Return the next seq for inserting a tombstone.
    ///
    /// For historical reasons, deletions do not increment the sequence number.
    /// Instead, the current sequence is converted to a tombstone marker.
    ///
    /// TODO: need to updated to use a new seq for tombstone.
    fn next_tombstone_seq(&mut self) -> SeqMarked<()> {
        if self.increase_seq_for_tombstone {
            self.last_seq += 1;
        }
        debug!("next_tombstone_seq: last_seq become: {}", self.last_seq);
        SeqMarked::new_tombstone(*self.last_seq)
    }

    #[deprecated(since = "0.4.2", note = "use snapshot() instead")]
    pub fn base(&self) -> &Snapshot<S, K, V, D> {
        &self.snapshot
    }

    /// Inserting a tombstone does not increase the seq, but instead, it use the last used seq.
    /// This is for a historical reason, the first version does not increase seq when deleting an item.
    pub fn set(&mut self, space: S, key: K, value: Option<V>) -> SeqMarked<()> {
        debug!(
            "View::set: space: {:?}, key: {:?}, value: {:?}",
            space, key, value
        );
        let seq = if value.is_none() {
            // delete by insert a tombstone
            self.next_tombstone_seq()
        } else {
            // insert
            if space.increments_seq() {
                self.next_normal_seq()
            } else {
                self.current_normal_seq()
            }
        };

        let seq_num = *seq.internal_seq();

        let update_table = self.changes.entry(space).or_default();

        if let Some(v) = value {
            update_table.insert(key, seq_num, v).unwrap();
        } else {
            update_table.insert_tombstone(key, seq_num).unwrap();
        }

        seq
    }

    pub async fn get(&self, space: S, key: K) -> Result<SeqMarked<V>, io::Error> {
        let last_seq = self.last_seq;

        let update_table = self.changes.get(&space);
        let updated = if let Some(update_table) = update_table {
            update_table.get(key.clone(), *last_seq).cloned()
        } else {
            SeqMarked::new_not_found()
        };

        if !updated.is_not_found() {
            return Ok(updated);
        }

        let base = self.snapshot.get(space, key).await?;

        let compacted = compact_seq_marked_pair(updated, base);

        Ok(compacted)
    }

    /// Convenience method to get multiple keys at once.
    ///
    /// This implementation calls `get` for each key, combining staged changes
    /// with the base view data for each key individually.
    pub async fn get_many(&self, space: S, keys: Vec<K>) -> Result<Vec<SeqMarked<V>>, io::Error> {
        let mut results = Vec::with_capacity(keys.len());
        for key in keys {
            let result = self.get(space, key).await?;
            results.push(result);
        }
        Ok(results)
    }

    /// Fetch the current value of a key and set it to a new value atomically.
    ///
    /// Returns a tuple of (old_value, new_value) where:
    /// - `old_value` is the previous value (or `SeqMarked::new_not_found()` if key didn't exist)
    /// - `new_value` is the newly set value (or `SeqMarked::new_tombstone()` if deleted)
    ///
    /// This is useful for atomic get-then-set operations.
    pub async fn fetch_and_set(
        &mut self,
        space: S,
        key: K,
        value: Option<V>,
    ) -> Result<(SeqMarked<V>, SeqMarked<V>), io::Error> {
        let old_value = self.get(space, key.clone()).await?;

        if old_value.is_not_found() && value.is_none() {
            // No such entry at all, no need to create a tombstone for delete
            return Ok((old_value, SeqMarked::new_tombstone(0)));
        }

        let order_key = self.set(space, key, value.clone());
        let new_value = match value {
            Some(v) => order_key.map(|_| v),
            None => SeqMarked::new_tombstone(*order_key.internal_seq()),
        };
        Ok((old_value, new_value))
    }

    pub async fn range<R>(
        &self,
        space: S,
        range: R,
    ) -> Result<IOResultStream<(K, SeqMarked<V>)>, io::Error>
    where
        R: RangeBounds<K> + Send + Sync + Clone + 'static,
    {
        let last_seq = self.last_seq;

        let base_strm = self.snapshot.range(space, range.clone()).await?;

        let update_table = self.changes.get(&space);
        let Some(update_table) = update_table else {
            return Ok(base_strm);
        };

        let updated_strm = update_table
            .range(range, *last_seq)
            .map(|(k, v)| (k.clone(), v.cloned()))
            .collect::<Vec<_>>();

        let strm = futures::stream::iter(updated_strm).map(Ok).boxed();

        let kmerge = KMerge::by(util::by_key_seq);
        let kmerge = kmerge.merge(base_strm).merge(strm);

        let coalesce = kmerge.coalesce(util::merge_kv_results);

        Ok(coalesce.boxed())
    }

    pub async fn commit(self) -> Result<D, io::Error> {
        let d = self.snapshot.commit(self.last_seq, self.changes).await?;
        Ok(d)
    }
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;

    use futures::StreamExt;
    use seq_marked::InternalSeq;
    use seq_marked::SeqMarked;

    use super::*;
    use crate::mvcc::seq_bounded_get::SeqBoundedGet;
    use crate::mvcc::table::Tables;
    use crate::mvcc::table::TablesSnapshot;

    #[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
    enum TestSpace {
        Space1,
        Space2,
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

    fn key(s: &str) -> TestKey {
        TestKey(s.to_string())
    }
    fn value(s: &str) -> TestValue {
        TestValue(s.to_string())
    }

    fn create_base_snapshot() -> TablesSnapshot<TestSpace, TestKey, TestValue> {
        let mut table = Table::new();
        table.insert(key("base_k1"), 1, value("base_v1")).unwrap();
        table.insert(key("base_k2"), 2, value("base_v2")).unwrap();

        let mut tables = BTreeMap::new();
        tables.insert(TestSpace::Space1, table);

        TablesSnapshot::new(InternalSeq::new(10), tables)
    }

    fn create_view(
        base: TablesSnapshot<TestSpace, TestKey, TestValue>,
    ) -> View<TestSpace, TestKey, TestValue, Tables<TestSpace, TestKey, TestValue>> {
        View::new(base)
    }

    fn create_view_with_tombstone_seq(
        base: TablesSnapshot<TestSpace, TestKey, TestValue>,
    ) -> View<TestSpace, TestKey, TestValue, Tables<TestSpace, TestKey, TestValue>> {
        View::new(base).with_tombstone_seq_increment(true)
    }

    #[tokio::test]
    async fn test_set_normal_value() {
        let mut view = create_view(create_base_snapshot());

        view.set(TestSpace::Space1, key("k1"), Some(value("v1")));

        assert_eq!(view.last_seq, InternalSeq::new(11));
        assert_eq!(view.changes.len(), 1);

        // Verify by reading changes
        let table = &view.changes[&TestSpace::Space1];
        let result = table.get(key("k1"), 11);
        assert_eq!(result, SeqMarked::new_normal(11, &value("v1")));
    }

    #[tokio::test]
    async fn test_set_tombstone_no_seq_increase() {
        let mut view = create_view(create_base_snapshot());

        // First insert
        view.set(TestSpace::Space1, key("k1"), Some(value("v1")));
        assert_eq!(view.last_seq, InternalSeq::new(11));

        // Delete (tombstone) - should not increase seq
        view.set(TestSpace::Space1, key("k2"), None);
        assert_eq!(view.last_seq, InternalSeq::new(11)); // unchanged

        // Verify both operations by reading changes
        let table = &view.changes[&TestSpace::Space1];
        let result1 = table.get(key("k1"), 11);
        assert_eq!(result1, SeqMarked::new_normal(11, &value("v1")));
        let result2 = table.get(key("k2"), 11);
        assert_eq!(result2, SeqMarked::new_tombstone(11));
    }

    #[tokio::test]
    async fn test_set_tombstone_with_seq_increase() {
        let mut view = create_view_with_tombstone_seq(create_base_snapshot());

        // First insert
        view.set(TestSpace::Space1, key("k1"), Some(value("v1")));
        assert_eq!(view.last_seq, InternalSeq::new(11));

        // Delete (tombstone) - should increase seq
        view.set(TestSpace::Space1, key("k2"), None);
        assert_eq!(view.last_seq, InternalSeq::new(12)); // increased

        // Verify both operations by reading changes
        let table = &view.changes[&TestSpace::Space1];
        let result1 = table.get(key("k1"), 12);
        assert_eq!(result1, SeqMarked::new_normal(11, &value("v1")));
        let result2 = table.get(key("k2"), 12);
        assert_eq!(result2, SeqMarked::new_tombstone(12));
    }

    #[tokio::test]
    async fn test_mget_from_base_only() {
        let view = create_view(create_base_snapshot());

        let keys = vec![key("base_k1"), key("base_k2")];
        let result = view.get_many(TestSpace::Space1, keys).await.unwrap();

        assert_eq!(result.len(), 2);
        assert_eq!(result[0], SeqMarked::new_normal(1, value("base_v1")));
        assert_eq!(result[1], SeqMarked::new_normal(2, value("base_v2")));
    }

    #[tokio::test]
    async fn test_mget_from_changes_only() {
        let mut view = create_view(create_base_snapshot());
        view.set(TestSpace::Space1, key("new_k1"), Some(value("new_v1")));
        view.set(TestSpace::Space1, key("new_k2"), Some(value("new_v2")));

        // Verify write operations by reading changes directly
        let table = &view.changes[&TestSpace::Space1];
        assert_eq!(
            table.get(key("new_k1"), 12),
            SeqMarked::new_normal(11, &value("new_v1"))
        );
        assert_eq!(
            table.get(key("new_k2"), 12),
            SeqMarked::new_normal(12, &value("new_v2"))
        );

        let keys = vec![key("new_k1"), key("new_k2")];
        let result = view.get_many(TestSpace::Space1, keys).await.unwrap();

        assert_eq!(result.len(), 2);
        assert_eq!(result[0], SeqMarked::new_normal(11, value("new_v1")));
        assert_eq!(result[1], SeqMarked::new_normal(12, value("new_v2")));
    }

    #[tokio::test]
    async fn test_mget_merge_base_and_changes() {
        let mut view = create_view(create_base_snapshot());
        // Override base value
        view.set(TestSpace::Space1, key("base_k1"), Some(value("updated_v1")));
        // Add new value
        view.set(TestSpace::Space1, key("new_k1"), Some(value("new_v1")));

        // Verify write operations by reading changes directly
        let table = &view.changes[&TestSpace::Space1];
        assert_eq!(
            table.get(key("base_k1"), 12),
            SeqMarked::new_normal(11, &value("updated_v1"))
        );
        assert_eq!(
            table.get(key("new_k1"), 12),
            SeqMarked::new_normal(12, &value("new_v1"))
        );

        let keys = vec![key("base_k1"), key("base_k2"), key("new_k1")];
        let result = view.get_many(TestSpace::Space1, keys).await.unwrap();

        assert_eq!(result.len(), 3);
        assert_eq!(result[0], SeqMarked::new_normal(11, value("updated_v1"))); // from changes
        assert_eq!(result[1], SeqMarked::new_normal(2, value("base_v2"))); // from base
        assert_eq!(result[2], SeqMarked::new_normal(12, value("new_v1"))); // from changes
    }

    #[tokio::test]
    async fn test_mget_tombstone_override() {
        let mut view = create_view(create_base_snapshot());
        // Delete base value
        view.set(TestSpace::Space1, key("base_k1"), None);

        // Verify write operation by reading changes directly
        let table = &view.changes[&TestSpace::Space1];
        let result = table.get(key("base_k1"), 10);
        assert_eq!(result, SeqMarked::new_tombstone(10)); // tombstone uses initial last_seq (10)

        let keys = vec![key("base_k1")];
        let result = view.get_many(TestSpace::Space1, keys).await.unwrap();

        assert_eq!(result.len(), 1);
        assert_eq!(result[0], SeqMarked::new_tombstone(10)); // tombstone wins
    }

    #[tokio::test]
    async fn test_mget_nonexistent_space() {
        let view = create_view(create_base_snapshot());

        let keys = vec![key("k1")];
        let result = view.get_many(TestSpace::Space2, keys).await.unwrap();

        assert_eq!(result.len(), 1);
        assert!(result[0].is_not_found());
    }

    #[tokio::test]
    async fn test_range_from_base_only() {
        let view = create_view(create_base_snapshot());

        let mut stream = view.range(TestSpace::Space1, ..).await.unwrap();
        let mut results = Vec::new();
        while let Some(result) = stream.next().await {
            results.push(result.unwrap());
        }

        assert_eq!(results.len(), 2);
        assert_eq!(
            results[0],
            (key("base_k1"), SeqMarked::new_normal(1, value("base_v1")))
        );
        assert_eq!(
            results[1],
            (key("base_k2"), SeqMarked::new_normal(2, value("base_v2")))
        );
    }

    #[tokio::test]
    async fn test_range_from_changes_only() {
        let mut view = create_view(create_base_snapshot());
        view.set(TestSpace::Space2, key("new_k1"), Some(value("new_v1")));
        view.set(TestSpace::Space2, key("new_k2"), Some(value("new_v2")));

        // Verify write operations by reading changes directly
        let table = &view.changes[&TestSpace::Space2];
        assert_eq!(
            table.get(key("new_k1"), 12),
            SeqMarked::new_normal(11, &value("new_v1"))
        );
        assert_eq!(
            table.get(key("new_k2"), 12),
            SeqMarked::new_normal(12, &value("new_v2"))
        );

        let mut stream = view.range(TestSpace::Space2, ..).await.unwrap();
        let mut results = Vec::new();
        while let Some(result) = stream.next().await {
            results.push(result.unwrap());
        }

        assert_eq!(results.len(), 2);
        assert_eq!(
            results[0],
            (key("new_k1"), SeqMarked::new_normal(11, value("new_v1")))
        );
        assert_eq!(
            results[1],
            (key("new_k2"), SeqMarked::new_normal(12, value("new_v2")))
        );
    }

    #[tokio::test]
    async fn test_range_merge_base_and_changes() {
        let mut view = create_view(create_base_snapshot());
        view.set(TestSpace::Space1, key("base_k1"), Some(value("updated_v1")));
        view.set(TestSpace::Space1, key("new_k1"), Some(value("new_v1")));

        // Verify write operations by reading changes directly
        let table = &view.changes[&TestSpace::Space1];
        assert_eq!(
            table.get(key("base_k1"), 12),
            SeqMarked::new_normal(11, &value("updated_v1"))
        );
        assert_eq!(
            table.get(key("new_k1"), 12),
            SeqMarked::new_normal(12, &value("new_v1"))
        );

        let mut stream = view.range(TestSpace::Space1, ..).await.unwrap();
        let mut results = Vec::new();
        while let Some(result) = stream.next().await {
            results.push(result.unwrap());
        }

        // Should be merged and sorted by key
        assert_eq!(results.len(), 3);
        assert_eq!(
            results[0],
            (
                key("base_k1"),
                SeqMarked::new_normal(11, value("updated_v1"))
            )
        );
        assert_eq!(
            results[1],
            (key("base_k2"), SeqMarked::new_normal(2, value("base_v2")))
        );
        assert_eq!(
            results[2],
            (key("new_k1"), SeqMarked::new_normal(12, value("new_v1")))
        );
    }

    #[tokio::test]
    async fn test_range_nonexistent_space() {
        let view = create_view(create_base_snapshot());

        let mut stream = view.range(TestSpace::Space2, ..).await.unwrap();
        let result = stream.next().await;

        assert!(result.is_none());
    }

    #[tokio::test]
    async fn test_commit() {
        let mut view = create_view(create_base_snapshot());
        view.set(TestSpace::Space1, key("k1"), Some(value("v1")));
        view.set(TestSpace::Space2, key("k2"), Some(value("v2")));

        // Verify write operations by reading changes directly
        let table1 = &view.changes[&TestSpace::Space1];
        let table2 = &view.changes[&TestSpace::Space2];
        assert_eq!(
            table1.get(key("k1"), 12),
            SeqMarked::new_normal(11, &value("v1"))
        );
        assert_eq!(
            table2.get(key("k2"), 12),
            SeqMarked::new_normal(12, &value("v2"))
        );

        assert_eq!(view.changes.len(), 2);

        let base_data = view.commit().await.unwrap();

        // Verify the base view contains the committed changes
        let keys = vec![key("k1")];
        let result = base_data
            .get_many(TestSpace::Space1, keys, u64::MAX)
            .await
            .unwrap();
        assert_eq!(result.len(), 1);
        assert_eq!(result[0], SeqMarked::new_normal(11, value("v1")));

        let keys = vec![key("k2")];
        let result = base_data
            .get_many(TestSpace::Space2, keys, u64::MAX)
            .await
            .unwrap();
        assert_eq!(result.len(), 1);
        assert_eq!(result[0], SeqMarked::new_normal(12, value("v2")));

        // Should also still contain original base data
        let keys = vec![key("base_k1"), key("base_k2")];
        let result = base_data
            .get_many(TestSpace::Space1, keys, u64::MAX)
            .await
            .unwrap();
        assert_eq!(result.len(), 2);
        assert_eq!(result[0], SeqMarked::new_normal(1, value("base_v1")));
        assert_eq!(result[1], SeqMarked::new_normal(2, value("base_v2")));
    }

    #[tokio::test]
    async fn test_sequence_ordering() {
        let mut view = create_view(create_base_snapshot());

        view.set(TestSpace::Space1, key("k1"), Some(value("v1")));
        assert_eq!(view.last_seq, InternalSeq::new(11));

        view.set(TestSpace::Space1, key("k2"), Some(value("v2")));
        assert_eq!(view.last_seq, InternalSeq::new(12));

        view.set(TestSpace::Space1, key("k3"), Some(value("v3")));
        assert_eq!(view.last_seq, InternalSeq::new(13));

        // Verify sequences are assigned correctly by reading changes
        let table = &view.changes[&TestSpace::Space1];
        assert_eq!(
            table.get(key("k1"), 13),
            SeqMarked::new_normal(11, &value("v1"))
        );
        assert_eq!(
            table.get(key("k2"), 13),
            SeqMarked::new_normal(12, &value("v2"))
        );
        assert_eq!(
            table.get(key("k3"), 13),
            SeqMarked::new_normal(13, &value("v3"))
        );
    }

    #[tokio::test]
    async fn test_multiple_spaces() {
        let mut view = create_view(create_base_snapshot());

        view.set(TestSpace::Space1, key("k1"), Some(value("v1")));
        view.set(TestSpace::Space2, key("k2"), Some(value("v2")));

        assert_eq!(view.changes.len(), 2);
        assert!(view.changes.contains_key(&TestSpace::Space1));
        assert!(view.changes.contains_key(&TestSpace::Space2));

        // Verify data in different spaces by reading changes
        let space1_table = &view.changes[&TestSpace::Space1];
        let space2_table = &view.changes[&TestSpace::Space2];

        assert_eq!(
            space1_table.get(key("k1"), 12),
            SeqMarked::new_normal(11, &value("v1"))
        );
        assert_eq!(
            space2_table.get(key("k2"), 12),
            SeqMarked::new_normal(12, &value("v2"))
        );
    }

    #[tokio::test]
    async fn test_complex_scenario() {
        let mut view = create_view(create_base_snapshot());

        // Insert new values
        view.set(TestSpace::Space1, key("new_k1"), Some(value("new_v1")));
        // Override base value
        view.set(
            TestSpace::Space1,
            key("base_k1"),
            Some(value("updated_base_v1")),
        );
        // Delete base value
        view.set(TestSpace::Space1, key("base_k2"), None);

        // Verify write operations by reading changes directly
        let table = &view.changes[&TestSpace::Space1];
        assert_eq!(
            table.get(key("new_k1"), 12),
            SeqMarked::new_normal(11, &value("new_v1"))
        );
        assert_eq!(
            table.get(key("base_k1"), 12),
            SeqMarked::new_normal(12, &value("updated_base_v1"))
        );
        assert_eq!(table.get(key("base_k2"), 12), SeqMarked::new_tombstone(12)); // no seq increase for tombstone

        // Test mget
        let keys = vec![key("base_k1"), key("base_k2"), key("new_k1")];
        let result = view.get_many(TestSpace::Space1, keys).await.unwrap();

        assert_eq!(result.len(), 3);
        assert_eq!(
            result[0],
            SeqMarked::new_normal(12, value("updated_base_v1"))
        );
        assert_eq!(result[1], SeqMarked::new_tombstone(12)); // deleted (no seq increase)
        assert_eq!(result[2], SeqMarked::new_normal(11, value("new_v1")));
    }

    // ===== CORNER CASE TESTS =====

    #[tokio::test]
    async fn test_empty_base_view() {
        let tables = BTreeMap::new(); // Completely empty base
        let base = TablesSnapshot::new(InternalSeq::new(1), tables);
        let mut view = create_view(base);

        // Operations on completely empty view
        view.set(TestSpace::Space1, key("k1"), Some(value("v1")));
        view.set(TestSpace::Space1, key("k2"), None);

        println!("{:#?}", view);

        // Verify operations work on empty base
        let table = &view.changes[&TestSpace::Space1];
        assert_eq!(
            table.get(key("k1"), 2),
            SeqMarked::new_normal(2, &value("v1"))
        );
        assert_eq!(table.get(key("k2"), 2), SeqMarked::new_tombstone(2));

        // mget should work with empty base
        let keys = vec![key("k1"), key("k2")];
        let result = view.get_many(TestSpace::Space1, keys).await.unwrap();
        assert_eq!(result[0], SeqMarked::new_normal(2, value("v1")));
        assert_eq!(result[1], SeqMarked::new_tombstone(2));
    }

    #[tokio::test]
    async fn test_zero_initial_sequence() {
        let mut view = View::new(create_base_snapshot()).with_initial_seq(InternalSeq::new(0));

        view.set(TestSpace::Space1, key("k1"), Some(value("v1")));
        assert_eq!(view.last_seq, InternalSeq::new(1));

        // Tombstone should use seq 1 (no increment)
        view.set(TestSpace::Space1, key("k2"), None);
        assert_eq!(view.last_seq, InternalSeq::new(1));

        let table = &view.changes[&TestSpace::Space1];
        assert_eq!(
            table.get(key("k1"), 1),
            SeqMarked::new_normal(1, &value("v1"))
        );
        assert_eq!(table.get(key("k2"), 1), SeqMarked::new_tombstone(1));
    }

    #[tokio::test]
    async fn test_max_sequence_boundary() {
        let mut view =
            View::new(create_base_snapshot()).with_initial_seq(InternalSeq::new(u64::MAX - 2));

        view.set(TestSpace::Space1, key("k1"), Some(value("v1")));
        assert_eq!(view.last_seq, InternalSeq::new(u64::MAX - 1));

        view.set(TestSpace::Space1, key("k2"), Some(value("v2")));
        assert_eq!(view.last_seq, InternalSeq::new(u64::MAX));

        // Should panic on overflow
        let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            view.set(TestSpace::Space1, key("k3"), Some(value("v3")))
        }));
        assert!(result.is_err()); // Should panic on sequence overflow
    }

    #[tokio::test]
    async fn test_tombstone_resurrection() {
        let mut view = create_view(create_base_snapshot());

        // Delete base key
        view.set(TestSpace::Space1, key("base_k1"), None);
        assert_eq!(view.last_seq, InternalSeq::new(10)); // No increment

        // Resurrect the key
        view.set(
            TestSpace::Space1,
            key("base_k1"),
            Some(value("resurrected")),
        );
        assert_eq!(view.last_seq, InternalSeq::new(11)); // Increment for normal value

        // Verify both operations
        let table = &view.changes[&TestSpace::Space1];
        let result = table.get(key("base_k1"), 11);
        assert_eq!(result, SeqMarked::new_normal(11, &value("resurrected"))); // Latest wins

        // Via mget, should see resurrected value
        let keys = vec![key("base_k1")];
        let result = view.get_many(TestSpace::Space1, keys).await.unwrap();
        assert_eq!(result[0], SeqMarked::new_normal(11, value("resurrected")));
    }

    #[tokio::test]
    async fn test_multiple_updates_same_key() {
        let mut view = create_view(create_base_snapshot());

        // Multiple updates to same key
        view.set(TestSpace::Space1, key("k1"), Some(value("v1")));
        view.set(TestSpace::Space1, key("k1"), Some(value("v2")));
        view.set(TestSpace::Space1, key("k1"), Some(value("v3")));
        view.set(TestSpace::Space1, key("k1"), None); // Delete
        view.set(TestSpace::Space1, key("k1"), Some(value("v4"))); // Resurrect

        assert_eq!(view.last_seq, InternalSeq::new(14));

        // Should see latest version
        let table = &view.changes[&TestSpace::Space1];
        let result = table.get(key("k1"), 14);
        assert_eq!(result, SeqMarked::new_normal(14, &value("v4")));

        // All intermediate versions should be visible at their sequence points
        assert_eq!(
            table.get(key("k1"), 11),
            SeqMarked::new_normal(11, &value("v1"))
        );
        assert_eq!(
            table.get(key("k1"), 12),
            SeqMarked::new_normal(12, &value("v2"))
        );
        // At seq 13, tombstone overwrites v3 since tombstone uses same seq as last operation
        assert_eq!(table.get(key("k1"), 13), SeqMarked::new_tombstone(13));
        assert_eq!(
            table.get(key("k1"), 14),
            SeqMarked::new_normal(14, &value("v4"))
        ); // Latest is resurrection
    }

    #[tokio::test]
    async fn test_empty_key_lists() {
        let mut view = create_view(create_base_snapshot());
        view.set(TestSpace::Space1, key("k1"), Some(value("v1")));

        // Empty key list should return empty result
        let result = view.get_many(TestSpace::Space1, vec![]).await.unwrap();
        assert_eq!(result.len(), 0);

        // Empty key list on non-existent space
        let result = view.get_many(TestSpace::Space2, vec![]).await.unwrap();
        assert_eq!(result.len(), 0);
    }

    #[tokio::test]
    async fn test_range_bounded_operations() {
        let mut view = create_view(create_base_snapshot());
        view.set(TestSpace::Space1, key("a"), Some(value("va")));
        view.set(TestSpace::Space1, key("c"), Some(value("vc")));
        view.set(TestSpace::Space1, key("e"), Some(value("ve")));

        // Bounded range
        let mut stream = view
            .range(TestSpace::Space1, key("b")..key("d"))
            .await
            .unwrap();
        let mut results = Vec::new();
        while let Some(result) = stream.next().await {
            results.push(result.unwrap());
        }

        assert_eq!(results.len(), 3); // base_k1, base_k2 and c (all > "b" and < "d")
        assert_eq!(
            results[0],
            (key("base_k1"), SeqMarked::new_normal(1, value("base_v1")))
        );
        assert_eq!(
            results[1],
            (key("base_k2"), SeqMarked::new_normal(2, value("base_v2")))
        );
        assert_eq!(
            results[2],
            (key("c"), SeqMarked::new_normal(12, value("vc")))
        );

        // Inclusive range
        let mut stream = view
            .range(TestSpace::Space1, key("base_k1")..=key("base_k2"))
            .await
            .unwrap();
        let mut results = Vec::new();
        while let Some(result) = stream.next().await {
            results.push(result.unwrap());
        }

        assert_eq!(results.len(), 2);
        assert_eq!(
            results[0],
            (key("base_k1"), SeqMarked::new_normal(1, value("base_v1")))
        );
        assert_eq!(
            results[1],
            (key("base_k2"), SeqMarked::new_normal(2, value("base_v2")))
        );
    }

    #[tokio::test]
    async fn test_range_with_all_tombstones() {
        let mut view = create_view(create_base_snapshot());

        // Delete all base keys and add only tombstones
        view.set(TestSpace::Space1, key("base_k1"), None);
        view.set(TestSpace::Space1, key("base_k2"), None);
        view.set(TestSpace::Space1, key("k1"), None);
        view.set(TestSpace::Space1, key("k2"), None);

        let mut stream = view.range(TestSpace::Space1, ..).await.unwrap();
        let mut results = Vec::new();
        while let Some(result) = stream.next().await {
            results.push(result.unwrap());
        }

        // Should see tombstones for all keys
        assert_eq!(results.len(), 4);
        assert_eq!(results[0], (key("base_k1"), SeqMarked::new_tombstone(10)));
        assert_eq!(results[1], (key("base_k2"), SeqMarked::new_tombstone(10)));
        assert_eq!(results[2], (key("k1"), SeqMarked::new_tombstone(10)));
        assert_eq!(results[3], (key("k2"), SeqMarked::new_tombstone(10)));
    }

    #[tokio::test]
    async fn test_range_single_key() {
        let view = create_view(create_base_snapshot());

        // Single key range
        let mut stream = view
            .range(TestSpace::Space1, key("base_k1")..=key("base_k1"))
            .await
            .unwrap();
        let mut results = Vec::new();
        while let Some(result) = stream.next().await {
            results.push(result.unwrap());
        }

        assert_eq!(results.len(), 1);
        assert_eq!(
            results[0],
            (key("base_k1"), SeqMarked::new_normal(1, value("base_v1")))
        );
    }

    #[tokio::test]
    async fn test_range_empty_result() {
        let view = create_view(create_base_snapshot());

        // Range that matches no keys
        let mut stream = view
            .range(TestSpace::Space1, key("x")..key("z"))
            .await
            .unwrap();
        let result = stream.next().await;
        assert!(result.is_none());

        // Single point range for non-existent key
        let mut stream = view
            .range(TestSpace::Space1, key("x")..=key("x"))
            .await
            .unwrap();
        let result = stream.next().await;
        assert!(result.is_none());
    }

    #[tokio::test]
    async fn test_view_sequence_filtering() {
        let mut table = Table::new();
        table.insert(key("k1"), 10, value("v1")).unwrap();
        table.insert(key("k2"), 20, value("v2")).unwrap();

        let mut tables = BTreeMap::new();
        tables.insert(TestSpace::Space1, table);
        // Can only see k1, not k2
        let base = TablesSnapshot::new(InternalSeq::new(15), tables);

        let view = create_view(base);

        let keys = vec![key("k1"), key("k2")];
        let result = view.get_many(TestSpace::Space1, keys).await.unwrap();

        assert_eq!(result.len(), 2);
        assert_eq!(result[0], SeqMarked::new_normal(10, value("v1"))); // visible
        assert!(result[1].is_not_found()); // filtered out by view_seq
    }

    #[tokio::test]
    async fn test_large_key_value_operations() {
        let mut view = create_view(create_base_snapshot());

        // Large key and value
        let large_key = key(&"x".repeat(1000));
        let large_value = value(&"y".repeat(1000));

        view.set(
            TestSpace::Space1,
            large_key.clone(),
            Some(large_value.clone()),
        );

        // Verify large data handling
        let table = &view.changes[&TestSpace::Space1];
        let result = table.get(large_key.clone(), 11);
        assert_eq!(result, SeqMarked::new_normal(11, &large_value));

        // Via mget
        let keys = vec![large_key];
        let result = view.get_many(TestSpace::Space1, keys).await.unwrap();
        assert_eq!(result[0], SeqMarked::new_normal(11, large_value));
    }

    #[tokio::test]
    async fn test_many_keys_operation() {
        let mut view = create_view(create_base_snapshot());

        // Insert many keys
        for i in 0..100 {
            view.set(
                TestSpace::Space1,
                key(&format!("k{:03}", i)),
                Some(value(&format!("v{}", i))),
            );
        }

        assert_eq!(view.last_seq, InternalSeq::new(110)); // 10 + 100

        // mget many keys
        let keys: Vec<_> = (0..100).map(|i| key(&format!("k{:03}", i))).collect();
        let result = view.get_many(TestSpace::Space1, keys).await.unwrap();

        assert_eq!(result.len(), 100);
        for (i, seq_marked) in result.iter().enumerate() {
            let expected_seq = 11 + i as u64;
            assert_eq!(
                *seq_marked,
                SeqMarked::new_normal(expected_seq, value(&format!("v{}", i)))
            );
        }
    }

    #[tokio::test]
    async fn test_sequence_exact_match_boundary() {
        let mut view = create_view(create_base_snapshot());

        view.set(TestSpace::Space1, key("k1"), Some(value("v1"))); // seq 6
        view.set(TestSpace::Space1, key("k2"), Some(value("v2"))); // seq 7

        // mget with last_seq exactly matching
        let keys = vec![key("k1"), key("k2")];
        let result = view.get_many(TestSpace::Space1, keys).await.unwrap();

        assert_eq!(result.len(), 2);
        assert_eq!(result[0], SeqMarked::new_normal(11, value("v1")));
        assert_eq!(result[1], SeqMarked::new_normal(12, value("v2")));
    }

    #[tokio::test]
    async fn test_cross_space_sequence_isolation() {
        let mut view = create_view(create_base_snapshot());

        // Operations in different spaces should share sequence counter
        view.set(TestSpace::Space1, key("k1"), Some(value("v1"))); // seq 6
        view.set(TestSpace::Space2, key("k2"), Some(value("v2"))); // seq 7
        view.set(TestSpace::Space1, key("k3"), Some(value("v3"))); // seq 8

        assert_eq!(view.last_seq, InternalSeq::new(13));

        // Verify sequences are properly assigned across spaces
        let table1 = &view.changes[&TestSpace::Space1];
        let table2 = &view.changes[&TestSpace::Space2];

        assert_eq!(
            table1.get(key("k1"), 13),
            SeqMarked::new_normal(11, &value("v1"))
        );
        assert_eq!(
            table2.get(key("k2"), 13),
            SeqMarked::new_normal(12, &value("v2"))
        );
        assert_eq!(
            table1.get(key("k3"), 13),
            SeqMarked::new_normal(13, &value("v3"))
        );
    }

    #[tokio::test]
    async fn test_set_return_value() {
        let mut view = create_view(create_base_snapshot());

        // Test normal value insertion
        let order_key1 = view.set(TestSpace::Space1, key("k1"), Some(value("v1")));
        assert_eq!(order_key1, SeqMarked::new_normal(11, ()));

        // Test another normal value
        let order_key2 = view.set(TestSpace::Space1, key("k2"), Some(value("v2")));
        assert_eq!(order_key2, SeqMarked::new_normal(12, ()));

        // Test tombstone insertion (should not increment with no seq increase)
        let order_key3 = view.set(TestSpace::Space1, key("k3"), None);
        assert_eq!(order_key3, SeqMarked::new_tombstone(12));

        // Test tombstone with seq increase
        let order_key4 = view.set(TestSpace::Space1, key("k4"), None);
        assert_eq!(order_key4, SeqMarked::new_tombstone(12));
    }

    #[tokio::test]
    async fn test_set_return_value_with_increment() {
        let mut view = create_view_with_tombstone_seq(create_base_snapshot());

        // Test normal value insertion
        let order_key1 = view.set(TestSpace::Space1, key("k1"), Some(value("v1")));
        assert_eq!(order_key1, SeqMarked::new_normal(11, ()));

        // Test tombstone with seq increment
        let order_key2 = view.set(TestSpace::Space1, key("k2"), None);
        assert_eq!(order_key2, SeqMarked::new_tombstone(12));

        // Test another value
        let order_key3 = view.set(TestSpace::Space1, key("k3"), Some(value("v3")));
        assert_eq!(order_key3, SeqMarked::new_normal(13, ()));
    }

    #[tokio::test]
    async fn test_fetch_and_set_nonexistent() {
        let mut view = create_view(create_base_snapshot());

        let (old_value, new_value) = view
            .fetch_and_set(TestSpace::Space1, key("new_key"), Some(value("new_value")))
            .await
            .unwrap();

        // Should return not_found for old value
        assert_eq!(old_value, SeqMarked::new_not_found());
        // Should return the new value
        assert_eq!(new_value, SeqMarked::new_normal(11, value("new_value")));

        // Verify the value was actually set
        let current_value = view.get(TestSpace::Space1, key("new_key")).await.unwrap();
        assert_eq!(current_value, SeqMarked::new_normal(11, value("new_value")));
    }

    #[tokio::test]
    async fn test_fetch_and_set_existing_from_base() {
        let mut view = create_view(create_base_snapshot());

        let (old_value, new_value) = view
            .fetch_and_set(
                TestSpace::Space1,
                key("base_k1"),
                Some(value("updated_value")),
            )
            .await
            .unwrap();

        // Should return the old value from base
        assert_eq!(old_value, SeqMarked::new_normal(1, value("base_v1")));
        // Should return the new value
        assert_eq!(new_value, SeqMarked::new_normal(11, value("updated_value")));

        // Verify the value was updated
        let current_value = view.get(TestSpace::Space1, key("base_k1")).await.unwrap();
        assert_eq!(
            current_value,
            SeqMarked::new_normal(11, value("updated_value"))
        );
    }

    #[tokio::test]
    async fn test_fetch_and_set_existing_from_changes() {
        let mut view = create_view(create_base_snapshot());

        // First set a value to create changes
        view.set(
            TestSpace::Space1,
            key("test_key"),
            Some(value("first_value")),
        );

        let (old_value, new_value) = view
            .fetch_and_set(
                TestSpace::Space1,
                key("test_key"),
                Some(value("second_value")),
            )
            .await
            .unwrap();

        // Should return the old value from changes
        assert_eq!(old_value, SeqMarked::new_normal(11, value("first_value")));
        // Should return the new value
        assert_eq!(new_value, SeqMarked::new_normal(12, value("second_value")));

        // Verify the value was updated
        let current_value = view.get(TestSpace::Space1, key("test_key")).await.unwrap();
        assert_eq!(
            current_value,
            SeqMarked::new_normal(12, value("second_value"))
        );
    }

    #[tokio::test]
    async fn test_fetch_and_set_tombstone() {
        let mut view = create_view(create_base_snapshot());

        let (old_value, new_value) = view
            .fetch_and_set(TestSpace::Space1, key("base_k1"), None)
            .await
            .unwrap();

        // Should return the old value from base
        assert_eq!(old_value, SeqMarked::new_normal(1, value("base_v1")));
        // Should return tombstone as new value
        assert_eq!(new_value, SeqMarked::new_tombstone(10));

        // Verify the value was deleted
        let current_value = view.get(TestSpace::Space1, key("base_k1")).await.unwrap();
        assert_eq!(current_value, SeqMarked::new_tombstone(10));
    }

    #[tokio::test]
    async fn test_fetch_and_set_cross_space() {
        let mut view = create_view(create_base_snapshot());

        // Set value in Space1
        let (old1, new1) = view
            .fetch_and_set(TestSpace::Space1, key("k1"), Some(value("v1")))
            .await
            .unwrap();

        // Set value in Space2
        let (old2, new2) = view
            .fetch_and_set(TestSpace::Space2, key("k2"), Some(value("v2")))
            .await
            .unwrap();

        // Verify sequence ordering across spaces
        assert_eq!(old1, SeqMarked::new_not_found());
        assert_eq!(old2, SeqMarked::new_not_found());
        assert_eq!(new1, SeqMarked::new_normal(11, value("v1")));
        assert_eq!(new2, SeqMarked::new_normal(12, value("v2")));

        // Verify view's last_seq was updated correctly
        assert_eq!(view.last_seq, InternalSeq::new(12));
    }

    #[tokio::test]
    async fn test_fetch_and_set_tombstone_resurrection() {
        let mut view = create_view(create_base_snapshot());

        // First delete a base key
        let (old1, new1) = view
            .fetch_and_set(TestSpace::Space1, key("base_k1"), None)
            .await
            .unwrap();
        assert_eq!(old1, SeqMarked::new_normal(1, value("base_v1")));
        assert_eq!(new1, SeqMarked::new_tombstone(10));

        // Then resurrect it
        let (old2, new2) = view
            .fetch_and_set(
                TestSpace::Space1,
                key("base_k1"),
                Some(value("resurrected")),
            )
            .await
            .unwrap();
        assert_eq!(old2, SeqMarked::new_tombstone(10));
        assert_eq!(new2, SeqMarked::new_normal(11, value("resurrected")));

        // Verify final state
        let current_value = view.get(TestSpace::Space1, key("base_k1")).await.unwrap();
        assert_eq!(
            current_value,
            SeqMarked::new_normal(11, value("resurrected"))
        );
    }

    #[tokio::test]
    async fn test_fetch_and_set_delete_nonexistent() {
        let mut view = create_view(create_base_snapshot());

        // Try to delete a key that doesn't exist
        let (old_value, new_value) = view
            .fetch_and_set(TestSpace::Space1, key("nonexistent_key"), None)
            .await
            .unwrap();

        // Should return not_found for old value
        assert_eq!(old_value, SeqMarked::new_not_found());
        // Should return tombstone with seq 0 (no tombstone created)
        assert_eq!(new_value, SeqMarked::new_tombstone(0));

        // Verify no tombstone was actually created in the changes
        let key_exists_in_table = view
            .changes
            .get(&TestSpace::Space1)
            .map(|table| {
                table
                    .inner
                    .keys()
                    .any(|(k, _)| k == &key("nonexistent_key"))
            })
            .unwrap_or(false);
        assert!(!key_exists_in_table);

        // Verify the key still doesn't exist
        let current_value = view
            .get(TestSpace::Space1, key("nonexistent_key"))
            .await
            .unwrap();
        assert_eq!(current_value, SeqMarked::new_not_found());

        // Verify last_seq was not incremented
        assert_eq!(view.last_seq, InternalSeq::new(10));
    }
}
