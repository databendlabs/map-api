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

pub mod errors;
mod impl_commit;
mod impl_scoped_snapshot_get;
mod impl_scoped_snapshot_range_iter;
pub mod range_iter;
mod table_view_readonly;

use std::cmp::Reverse;
use std::collections::BTreeMap;
use std::collections::Bound;
use std::io;
use std::ops::RangeBounds;

use errors::InsertError;
use futures::Stream;
use futures_util::TryStreamExt;
use range_iter::RangeIter;
pub use table_view_readonly::TableViewReadonly;

use crate::SeqMarked;

/// A in-memory table contains multiple version of key-values
#[derive(Debug)]
pub struct Table<K, V> {
    /// Keep the most recent version of the key-value pair at the top of the map.
    ///
    /// A tombstone record does not have a value.
    pub inner: BTreeMap<(K, Reverse<SeqMarked<()>>), Option<V>>,

    /// The last inserted may be a tombstone.
    ///
    /// With the implementation that insert tombstone does not increase seq, it use (seq, tombstone) to compare.
    pub last_seq: SeqMarked<()>,
}

impl<K: Ord + Clone, V> Default for Table<K, V> {
    fn default() -> Self {
        Self::new()
    }
}

impl<K: Ord + Clone, V> Table<K, V> {
    pub fn new() -> Self {
        Self {
            inner: BTreeMap::new(),
            last_seq: SeqMarked::zero(),
        }
    }

    pub async fn from_stream(
        mut stream: impl Stream<Item = Result<(K, SeqMarked<V>), io::Error>> + Unpin,
    ) -> Result<Self, io::Error> {
        let mut inner = BTreeMap::new();
        let mut last_seq = SeqMarked::zero();

        while let Some((key, value)) = stream.try_next().await? {
            let order_key = value.order_key();
            let v = value.into_data();

            let k = (key, Reverse(order_key));

            inner.insert(k, v);
            if order_key > last_seq {
                last_seq = order_key;
            }
        }

        let s = Self { inner, last_seq };
        Ok(s)
    }

    pub fn apply(&mut self, other: Self) {
        self.apply_changes(other.last_seq, other.inner);
    }

    pub fn apply_changes(
        &mut self,
        last_seq: SeqMarked<()>,
        changes: impl IntoIterator<Item = ((K, Reverse<SeqMarked<()>>), Option<V>)>,
    ) {
        assert!(self.last_seq <= last_seq);

        self.inner.extend(changes);
        self.last_seq = last_seq;
    }

    pub fn get_many(&self, keys: Vec<K>, upto: u64) -> Vec<SeqMarked<&V>> {
        let mut result = Vec::with_capacity(keys.len());
        for key in keys {
            result.push(self.get(key, upto));
        }
        result
    }

    /// Get the value for a key at or before the specified sequence number.
    /// Returns the most recent version that is ≤ `upto`.
    ///
    /// Note: `upto` is a sequence number, not SeqMarked. This allows seeing delete operations
    /// that occur after the specified sequence. Cannot be fixed until delete operations
    /// increment the sequence number and the state machine is updated accordingly.
    pub fn get(&self, key: K, upto: u64) -> SeqMarked<&V> {
        // Find entries with the given key and sequence ≤ upto
        let range_start = (key.clone(), Reverse(SeqMarked::new_tombstone(upto)));

        // Get the first (most recent) entry in the range
        if let Some(((k, rev_seq_marked), v)) = self.inner.range(range_start..).next() {
            let seq_marked = rev_seq_marked.0;
            if k == &key {
                return if seq_marked.is_tombstone() {
                    SeqMarked::new_tombstone(*seq_marked.internal_seq())
                } else {
                    seq_marked.map(|_x| v.as_ref().unwrap())
                };
            }
        }

        SeqMarked::new_not_found()
    }

    /// Note: `upto` is a sequence number, not SeqMarked. This allows seeing delete operations
    /// that occur after the specified sequence. Cannot be fixed until delete operations
    /// increment the sequence number and the state machine is updated accordingly.
    pub fn range<R>(&self, range: R, upto: u64) -> RangeIter<'_, K, V>
    where R: RangeBounds<K> + Clone + 'static {
        // Include all the SeqMarked that is less than or equal to `upto`.
        let upto = SeqMarked::new_tombstone(upto);

        let start = range.start_bound().cloned();
        let start = match start {
            // Use the greatest seq-marked, thus Reverse(max) includes all the records starts with `k`
            Bound::Included(k) => Bound::Included((k, Reverse(SeqMarked::max_value()))),
            // Use the smallest, thus Reverse(zero) include non of the records start with `k`.
            Bound::Excluded(k) => Bound::Excluded((k, Reverse(SeqMarked::zero()))),
            Bound::Unbounded => Bound::Unbounded,
        };

        let end = range.end_bound().cloned();
        let end = match end {
            // Use the smallest seq-marked, thus Reverse(zero) includes all the records starts with `k`
            Bound::Included(k) => Bound::Included((k, Reverse(SeqMarked::zero()))),
            // Use the largest, thus Reverse(max) include non of the records start with `k`.
            Bound::Excluded(k) => Bound::Excluded((k, Reverse(SeqMarked::max_value()))),
            Bound::Unbounded => Bound::Unbounded,
        };

        RangeIter {
            inner: self.inner.range((start, end)),
            upto,
            last_seen_key: None,
        }
    }

    pub fn insert(&mut self, key: K, seq: u64, value: V) -> Result<(), InsertError> {
        let seq_marked = SeqMarked::new_normal(seq, ());
        self.internal_insert(key, seq_marked, Some(value))
    }

    pub fn insert_tombstone(&mut self, key: K, seq: u64) -> Result<(), InsertError> {
        let seq_marked = SeqMarked::new_tombstone(seq);
        self.internal_insert(key, seq_marked, None)
    }

    fn internal_insert(
        &mut self,
        key: K,
        seq_marked: SeqMarked<()>,
        value: Option<V>,
    ) -> Result<(), InsertError> {
        // If a namespace disable seq incr, same InternalSeq may be inserted

        if *seq_marked.internal_seq() >= *self.last_seq.internal_seq() {
            // ok
        } else {
            return Err(InsertError::NonIncremental {
                last: self.last_seq,
                current: seq_marked,
            });
        }

        self.last_seq = seq_marked;
        self.inner.insert((key, Reverse(seq_marked)), value);

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_get_empty_table() {
        let table: Table<String, String> = Table::new();
        let result = table.get(s("k"), 1);
        assert!(result.is_not_found());
    }

    #[test]
    fn test_apply() {
        let mut table = Table::new();
        table.insert(s("k1"), 1, s("v1")).unwrap();
        table.insert(s("k1"), 2, s("v1-2")).unwrap();
        table.insert_tombstone(s("k1"), 3).unwrap();
        table.last_seq = SeqMarked::new_normal(1, ());

        assert_eq!(table.inner.get(&(s("k1"), rs_tomb(3))), Some(&None));

        let mut other = Table::new();
        other.insert(s("k1"), 4, s("v1-4")).unwrap();
        other.last_seq = SeqMarked::new_normal(2, ());

        table.apply(other);

        assert_eq!(table.last_seq, SeqMarked::new_normal(2, ()));
        assert_eq!(table.inner.get(&(s("k1"), rs(4))), Some(&Some(s("v1-4"))));
        assert_eq!(table.inner.get(&(s("k1"), rs_tomb(3))), Some(&None));
    }

    #[test]
    fn test_get_single_value() {
        let mut table = Table::new();
        table.inner.insert(k("k", 1), Some(s("v")));

        let result = table.get(s("k"), 1);
        assert_eq!(result, SeqMarked::new_normal(1, &s("v")));
    }

    #[test]
    fn test_get_with_different_sequence_numbers() {
        let mut table = Table::new();
        table.inner.insert(k("k", 5), Some(s("v")));

        // Should find value when upto >= sequence
        let result = table.get(s("k"), 5);
        assert_eq!(result, SeqMarked::new_normal(5, &s("v")));

        // Should find value when upto > sequence
        let result = table.get(s("k"), 10);
        assert_eq!(result, SeqMarked::new_normal(5, &s("v")));

        // Should not find value when upto < sequence
        let result = table.get(s("k"), 3);
        assert!(result.is_not_found());
    }

    #[test]
    fn test_get_multiple_versions() {
        let mut table = Table::new();
        table.inner.insert(k("k", 1), Some(s("v1")));
        table.inner.insert(k("k", 3), Some(s("v3")));
        table.inner.insert(k("k", 5), Some(s("v5")));

        // Should get most recent version ≤ upto (seq 3 when asking for seq 4)
        let result = table.get(s("k"), 4);
        assert_eq!(result, SeqMarked::new_normal(3, &s("v3")));

        // Should get most recent version ≤ upto (seq 5 when asking for seq 5)
        let result = table.get(s("k"), 5);
        assert_eq!(result, SeqMarked::new_normal(5, &s("v5")));
    }

    #[test]
    fn test_get_different_keys() {
        let mut table = Table::new();

        table.inner.insert(k("k1", 1), Some(s("v1")));
        table.inner.insert(k("k2", 3), Some(s("v2-3")));
        table.inner.insert(k("k2", 2), Some(s("v2")));
        table.inner.insert(k("k3", 3), Some(s("v3")));

        let res = table.get(s("k1"), 1);
        assert_eq!(res, SeqMarked::new_normal(1, &s("v1")));

        let res = table.get(s("k2"), 3);
        assert_eq!(res, SeqMarked::new_normal(3, &s("v2-3")));

        let res = table.get(s("k2"), 2);
        assert_eq!(res, SeqMarked::new_normal(2, &s("v2")));

        let res = table.get(s("k2"), 1);
        assert!(res.is_not_found());

        let res = table.get(s("k3"), 2);
        assert!(res.is_not_found());
    }

    #[test]
    fn test_tombstone_records() {
        let mut table = Table::new();

        table.inner.insert(k("k1", 1), Some(s("v1")));
        table.inner.insert(k_tomb("k2", 4), None);
        table.inner.insert(k("k2", 3), Some(s("v2-3")));
        table.inner.insert(k("k2", 2), Some(s("v2")));
        table.inner.insert(k("k3", 3), Some(s("v3")));

        // 5

        let res = table.get(s("k2"), 5);
        assert_eq!(res, SeqMarked::new_tombstone(4));

        // 4

        let res = table.get(s("k2"), 4);
        assert_eq!(res, SeqMarked::new_tombstone(4));

        // 3

        let res = table.get(s("k2"), 3);
        assert_eq!(res, SeqMarked::new_normal(3, &s("v2-3")));

        // 2

        let res = table.get(s("k2"), 2);
        assert_eq!(res, SeqMarked::new_normal(2, &s("v2")));

        // 1

        let res = table.get(s("k2"), 1);
        assert_eq!(res, SeqMarked::new_not_found());
    }

    #[test]
    fn test_get_with_tombstone_sequence() {
        let mut table = Table::new();
        table.inner.insert(k("k", 1), Some(s("v")));

        let result = table.get(s("k"), 2);
        assert_eq!(result, SeqMarked::new_normal(1, &s("v")));
    }

    #[test]
    fn test_get_with_different_value_types() {
        // Test with String values
        let mut table_string: Table<String, String> = Table::new();
        table_string.inner.insert(k("k", 1), Some(s("v")));
        let result = table_string.get(s("k"), 1);
        assert_eq!(result, SeqMarked::new_normal(1, &s("v")));

        // Test with Vec<u8> values
        let mut table_vec: Table<String, Vec<u8>> = Table::new();
        table_vec.inner.insert(k("k", 1), Some(b"v".to_vec()));
        let result = table_vec.get(s("k"), 1);
        assert_eq!(result, SeqMarked::new_normal(1, &b"v".to_vec()));
    }

    #[test]
    fn test_mget() {
        let mut table = Table::new();
        table.inner.insert(k("k1", 1), Some(s("v1")));

        table.inner.insert(k("k2", 3), Some(s("v2-3")));
        table.inner.insert(k_tomb("k2", 2), None);
        table.inner.insert(k("k2", 2), Some(s("v2")));

        table.inner.insert(k("k3", 3), Some(s("v3-3")));
        table.inner.insert(k("k3", 2), Some(s("v3-2")));

        table.inner.insert(k_tomb("k4", 5), None);
        table.inner.insert(k("k4", 4), Some(s("v4")));

        table.inner.insert(k("k5", 4), Some(s("v5")));

        let res = table.get_many(vec![s("k2"), s("k3")], 2);
        let res = res.into_iter().map(|x| x.cloned()).collect::<Vec<_>>();
        assert_eq!(res, vec![
            //
            sm_tomb(2),
            sm(2, "v3-2"),
        ]);
    }

    #[test]
    fn test_range_full_upto() {
        let mut table = Table::new();
        table.inner.insert(k("k1", 1), Some(s("v1")));

        table.inner.insert(k("k2", 3), Some(s("v2-3")));
        table.inner.insert(k_tomb("k2", 2), None);
        table.inner.insert(k("k2", 2), Some(s("v2")));

        table.inner.insert(k("k3", 3), Some(s("v3-3")));
        table.inner.insert(k("k3", 2), Some(s("v3-2")));

        table.inner.insert(k_tomb("k4", 5), None);
        table.inner.insert(k("k4", 4), Some(s("v4")));

        table.inner.insert(k("k5", 4), Some(s("v5")));

        // 0

        let res = collect(table.range(.., 0));
        assert!(res.is_empty());

        // 1

        let res = collect(table.range(.., 1));
        assert_eq!(res, vec![(s("k1"), sm(1, "v1")),]);

        // 2

        let res = collect(table.range(.., 2));
        assert_eq!(res, vec![
            (s("k1"), sm(1, "v1")),
            (s("k2"), sm_tomb(2)),
            (s("k3"), sm(2, "v3-2")),
        ]);

        // 3

        let res = collect(table.range(.., 3));
        assert_eq!(res, vec![
            (s("k1"), sm(1, "v1")),
            (s("k2"), sm(3, "v2-3")),
            (s("k3"), sm(3, "v3-3")),
        ]);

        // 4

        let res = collect(table.range(.., 4));
        assert_eq!(res, vec![
            (s("k1"), sm(1, "v1")),
            (s("k2"), sm(3, "v2-3")),
            (s("k3"), sm(3, "v3-3")),
            (s("k4"), sm(4, "v4")),
            (s("k5"), sm(4, "v5")),
        ]);

        // 5

        let res = collect(table.range(.., 5));
        assert_eq!(res, vec![
            (s("k1"), sm(1, "v1")),
            (s("k2"), sm(3, "v2-3")),
            (s("k3"), sm(3, "v3-3")),
            (s("k4"), sm_tomb(5)),
            (s("k5"), sm(4, "v5")),
        ]);
    }

    #[test]
    fn test_range_empty_result() {
        let mut table = Table::new();
        table.inner.insert(k_tomb("k1", 1), None);
        table.inner.insert(k("k2", 3), Some(s("v2")));
        table.inner.insert(k("k3", 3), Some(s("v3")));

        // open left bound
        let res = collect(table.range(s("k2")..s("k2"), 10));
        assert_eq!(res, vec![]);
    }

    #[test]
    fn test_range_left_bound_tombstone() {
        let mut table = Table::new();
        table.inner.insert(k_tomb("k1", 1), None);
        table.inner.insert(k("k2", 3), Some(s("v2")));
        table.inner.insert(k("k3", 3), Some(s("v3")));

        // open left bound
        let res = collect(table.range(s("k1").., 10));
        assert_eq!(res, vec![
            //
            (s("k1"), sm_tomb(1)),
            (s("k2"), sm(3, "v2")),
            (s("k3"), sm(3, "v3"))
        ]);

        // close left bound
        let res = collect(table.range((Bound::Excluded(s("k1")), Bound::Unbounded), 10));
        assert_eq!(res, vec![
            //
            (s("k2"), sm(3, "v2")),
            (s("k3"), sm(3, "v3"))
        ]);
    }

    #[test]
    fn test_range_left_bound_normal() {
        let mut table = Table::new();
        table.inner.insert(k("k1", 1), Some(s("v1")));
        table.inner.insert(k("k2", 3), Some(s("v2")));
        table.inner.insert(k("k3", 3), Some(s("v3")));

        // open left bound
        let res = collect(table.range(s("k1").., 10));
        assert_eq!(res, vec![
            //
            (s("k1"), sm(1, "v1")),
            (s("k2"), sm(3, "v2")),
            (s("k3"), sm(3, "v3"))
        ]);

        // close left bound
        let res = collect(table.range((Bound::Excluded(s("k1")), Bound::Unbounded), 10));
        assert_eq!(res, vec![
            //
            (s("k2"), sm(3, "v2")),
            (s("k3"), sm(3, "v3"))
        ]);
    }

    #[test]
    fn test_range_right_bound_tombstone() {
        let mut table = Table::new();
        table.inner.insert(k("k1", 1), Some(s("v1")));
        table.inner.insert(k("k2", 3), Some(s("v2-3")));
        table.inner.insert(k_tomb("k3", 3), None);

        // open right bound
        let res = collect(table.range(..s("k3"), 10));
        assert_eq!(res, vec![
            //
            (s("k1"), sm(1, "v1")),
            (s("k2"), sm(3, "v2-3")),
        ]);

        // close right bound
        let res = collect(table.range(..=s("k3"), 10));
        assert_eq!(res, vec![
            //
            (s("k1"), sm(1, "v1")),
            (s("k2"), sm(3, "v2-3")),
            (s("k3"), sm_tomb(3)),
        ]);
    }

    #[test]
    fn test_range_right_bound_normal() {
        let mut table = Table::new();
        table.inner.insert(k("k1", 1), Some(s("v1")));
        table.inner.insert(k("k2", 3), Some(s("v2")));
        table.inner.insert(k("k3", 3), Some(s("v3")));

        // open right bound
        let res = collect(table.range(..s("k3"), 10));
        assert_eq!(res, vec![
            //
            (s("k1"), sm(1, "v1")),
            (s("k2"), sm(3, "v2")),
        ]);

        // close right bound
        let res = collect(table.range(..=s("k3"), 10));
        assert_eq!(res, vec![
            //
            (s("k1"), sm(1, "v1")),
            (s("k2"), sm(3, "v2")),
            (s("k3"), sm(3, "v3")),
        ]);
    }

    #[test]
    fn test_ranges() {
        let mut table = Table::new();
        table.inner.insert(k("k1", 1), Some(s("v1")));

        table.inner.insert(k("k2", 3), Some(s("v2-3")));
        table.inner.insert(k_tomb("k2", 2), None);
        table.inner.insert(k("k2", 2), Some(s("v2")));

        table.inner.insert(k("k3", 3), Some(s("v3-3")));
        table.inner.insert(k("k3", 2), Some(s("v3-2")));

        table.inner.insert(k_tomb("k4", 5), None);
        table.inner.insert(k("k4", 4), Some(s("v4")));

        table.inner.insert(k("k5", 4), Some(s("v5")));

        let res = collect(table.range(s("k2")..s("k4"), 2));
        assert_eq!(res, vec![
            //
            (s("k2"), sm_tomb(2)),
            (s("k3"), sm(2, "v3-2")),
        ]);

        let res = collect(table.range(s("k2")..=s("k4"), 4));
        assert_eq!(res, vec![
            //
            (s("k2"), sm(3, "v2-3")),
            (s("k3"), sm(3, "v3-3")),
            (s("k4"), sm(4, "v4")),
        ]);

        let res = collect(table.range(s("k2")..=s("k4"), 5));
        assert_eq!(res, vec![
            //
            (s("k2"), sm(3, "v2-3")),
            (s("k3"), sm(3, "v3-3")),
            (s("k4"), sm_tomb(5)),
        ]);
    }

    #[test]
    fn test_insert() {
        let mut table = Table::new();
        assert_eq!(table.last_seq, SeqMarked::zero());

        table.insert(s("k1"), 1, s("v1")).unwrap();
        assert_eq!(table.last_seq, ordkey(1));
        let result = table.get(s("k1"), 1);
        assert_eq!(result.cloned(), sm(1, "v1"));

        table.insert_tombstone(s("k1"), 1).unwrap();
        assert_eq!(table.last_seq, ordkey_tomb(1));
        let result = table.get(s("k1"), 1);
        assert_eq!(result.cloned(), sm_tomb(1));

        let res = table.insert(s("k2"), 0, s("v2"));
        assert_eq!(
            res,
            Err(InsertError::NonIncremental {
                last: ordkey_tomb(1),
                current: ordkey(0),
            })
        );

        let res = table.insert(s("k2"), 2, s("v2"));
        assert_eq!(res, Ok(()));
    }

    #[test]
    fn test_insert_tombstone() {
        let mut table = Table::new();
        assert_eq!(table.last_seq, SeqMarked::zero());

        table.insert_tombstone(s("k1"), 2).unwrap();
        assert_eq!(table.last_seq, ordkey_tomb(2));

        let result = table.get(s("k1"), 2);
        assert_eq!(result.cloned(), sm_tomb(2));

        let result = table.get(s("k1"), 1);
        assert!(result.is_not_found());
    }

    #[test]
    fn test_insert_and_tombstone_mixed() {
        let mut table = Table::new();

        table.insert(s("k"), 1, s("v1")).unwrap();
        table.insert_tombstone(s("k"), 3).unwrap();
        table.insert(s("k"), 5, s("v5")).unwrap();

        // At seq 2: should get v1
        let result = table.get(s("k"), 2);
        assert_eq!(result.cloned(), sm(1, "v1"));

        // At seq 4: should get tombstone at seq 3
        let result = table.get(s("k"), 4);
        assert_eq!(result.cloned(), sm_tomb(3));

        // At seq 5: should get v5
        let result = table.get(s("k"), 5);
        assert_eq!(result.cloned(), sm(5, "v5"));
    }

    #[test]
    fn test_apply_changes_basic() {
        let mut table: Table<String, String> = Table::new();
        table.last_seq = SeqMarked::new_normal(5, ());

        let changes = vec![
            ((s("k1"), rs(7)), Some(s("v1"))),
            ((s("k2"), rs(8)), Some(s("v2"))),
        ];

        table.apply_changes(SeqMarked::new_normal(10, ()), changes);

        assert_eq!(table.last_seq, SeqMarked::new_normal(10, ()));
        assert_eq!(table.inner.get(&(s("k1"), rs(7))), Some(&Some(s("v1"))));
        assert_eq!(table.inner.get(&(s("k2"), rs(8))), Some(&Some(s("v2"))));
    }

    #[test]
    fn test_apply_changes_with_tombstones() {
        let mut table: Table<String, String> = Table::new();
        table.last_seq = SeqMarked::new_normal(3, ());

        let changes = vec![
            ((s("k1"), rs(5)), Some(s("v1"))),
            ((s("k2"), rs_tomb(6)), None), // Tombstone
        ];

        table.apply_changes(SeqMarked::new_normal(7, ()), changes);

        assert_eq!(table.last_seq, SeqMarked::new_normal(7, ()));
        assert_eq!(table.inner.get(&(s("k1"), rs(5))), Some(&Some(s("v1"))));
        assert_eq!(table.inner.get(&(s("k2"), rs_tomb(6))), Some(&None));
    }

    #[test]
    fn test_apply_changes_empty() {
        let mut table: Table<String, String> = Table::new();
        table.last_seq = SeqMarked::new_normal(2, ());

        table.apply_changes(SeqMarked::new_normal(5, ()), vec![]);

        assert_eq!(table.last_seq, SeqMarked::new_normal(5, ()));
        assert!(table.inner.is_empty());
    }

    #[test]
    fn test_apply_changes_same_seq() {
        let mut table: Table<String, String> = Table::new();
        table.last_seq = SeqMarked::new_normal(4, ());

        let changes = vec![((s("k1"), rs(4)), Some(s("v1")))];

        table.apply_changes(SeqMarked::new_normal(4, ()), changes);

        assert_eq!(table.last_seq, SeqMarked::new_normal(4, ()));
        assert_eq!(table.inner.get(&(s("k1"), rs(4))), Some(&Some(s("v1"))));
    }

    #[test]
    #[should_panic(expected = "assertion failed")]
    fn test_apply_changes_invalid_seq() {
        let mut table: Table<String, String> = Table::new();
        table.last_seq = SeqMarked::new_normal(10, ());

        let changes = vec![((s("k1"), rs(5)), Some(s("v1")))];

        // This should panic because 5 < 10
        table.apply_changes(SeqMarked::new_normal(5, ()), changes);
    }

    #[test]
    fn test_apply_changes_extends_existing() {
        let mut table: Table<String, String> = Table::new();
        table.insert(s("existing"), 1, s("old_value")).unwrap();
        table.last_seq = SeqMarked::new_normal(1, ());

        let changes = vec![
            ((s("new_key"), rs(2)), Some(s("new_value"))),
            ((s("another"), rs_tomb(3)), None), // Tombstone
        ];

        table.apply_changes(SeqMarked::new_normal(3, ()), changes);

        assert_eq!(table.last_seq, SeqMarked::new_normal(3, ()));
        // Existing key should still be there
        assert_eq!(
            table.inner.get(&(s("existing"), rs(1))),
            Some(&Some(s("old_value")))
        );
        // New keys should be added
        assert_eq!(
            table.inner.get(&(s("new_key"), rs(2))),
            Some(&Some(s("new_value")))
        );
        assert_eq!(table.inner.get(&(s("another"), rs_tomb(3))), Some(&None));
    }

    #[tokio::test]
    async fn test_from_stream_basic() {
        let items = vec![
            Ok((s("k1"), SeqMarked::new_normal(1, s("v1")))),
            Ok((s("k2"), SeqMarked::new_normal(3, s("v2")))),
            Ok((s("k3"), SeqMarked::new_normal(2, s("v3")))),
        ];
        let stream = futures::stream::iter(items);

        let table = Table::<String, String>::from_stream(stream).await.unwrap();

        assert_eq!(table.last_seq, SeqMarked::new_normal(3, ()));
        assert_eq!(table.inner.get(&(s("k1"), rs(1))), Some(&Some(s("v1"))));
        assert_eq!(table.inner.get(&(s("k2"), rs(3))), Some(&Some(s("v2"))));
        assert_eq!(table.inner.get(&(s("k3"), rs(2))), Some(&Some(s("v3"))));
    }

    #[tokio::test]
    async fn test_from_stream_with_tombstones() {
        let items = vec![
            Ok((s("k1"), SeqMarked::new_normal(1, s("v1")))),
            Ok((s("k2"), SeqMarked::new_tombstone(2))),
            Ok((s("k3"), SeqMarked::new_normal(3, s("v3")))),
        ];
        let stream = futures::stream::iter(items);

        let table = Table::<String, String>::from_stream(stream).await.unwrap();

        assert_eq!(table.last_seq, SeqMarked::new_normal(3, ()));
        assert_eq!(table.inner.get(&(s("k1"), rs(1))), Some(&Some(s("v1"))));
        assert_eq!(table.inner.get(&(s("k2"), rs_tomb(2))), Some(&None));
        assert_eq!(table.inner.get(&(s("k3"), rs(3))), Some(&Some(s("v3"))));
    }

    #[tokio::test]
    async fn test_from_stream_empty() {
        let items: Vec<Result<(String, SeqMarked<String>), std::io::Error>> = vec![];
        let stream = futures::stream::iter(items);

        let table = Table::<String, String>::from_stream(stream).await.unwrap();

        assert_eq!(table.last_seq, SeqMarked::zero());
        assert!(table.inner.is_empty());
    }

    #[tokio::test]
    async fn test_from_stream_error() {
        let items = vec![
            Ok((s("k1"), SeqMarked::new_normal(1, s("v1")))),
            Err(std::io::Error::new(std::io::ErrorKind::Other, "test error")),
        ];
        let stream = futures::stream::iter(items);

        let result = Table::<String, String>::from_stream(stream).await;
        assert!(result.is_err());
    }

    #[tokio::test]
    async fn test_from_stream_sequence_ordering() {
        let items = vec![
            Ok((s("k1"), SeqMarked::new_normal(5, s("v1")))),
            Ok((s("k2"), SeqMarked::new_normal(2, s("v2")))),
            Ok((s("k3"), SeqMarked::new_normal(8, s("v3")))),
            Ok((s("k4"), SeqMarked::new_normal(3, s("v4")))),
        ];
        let stream = futures::stream::iter(items);

        let table = Table::<String, String>::from_stream(stream).await.unwrap();

        // Last seq should be the highest sequence seen (8)
        assert_eq!(table.last_seq, SeqMarked::new_normal(8, ()));
        assert_eq!(table.inner.len(), 4);
        assert_eq!(table.inner.get(&(s("k3"), rs(8))), Some(&Some(s("v3"))));
    }

    #[tokio::test]
    async fn test_from_stream_duplicate_keys() {
        let items = vec![
            Ok((s("k1"), SeqMarked::new_normal(1, s("v1_old")))),
            Ok((s("k1"), SeqMarked::new_normal(3, s("v1_new")))),
            Ok((s("k1"), SeqMarked::new_tombstone(2))),
        ];
        let stream = futures::stream::iter(items);

        let table = Table::<String, String>::from_stream(stream).await.unwrap();

        assert_eq!(table.last_seq, SeqMarked::new_normal(3, ()));
        // All versions should be stored with different sequence keys
        assert_eq!(table.inner.get(&(s("k1"), rs(1))), Some(&Some(s("v1_old"))));
        assert_eq!(table.inner.get(&(s("k1"), rs(3))), Some(&Some(s("v1_new"))));
        assert_eq!(table.inner.get(&(s("k1"), rs_tomb(2))), Some(&None));
        assert_eq!(table.inner.len(), 3);
    }

    fn collect<'a>(
        it: impl Iterator<Item = (&'a String, SeqMarked<&'a String>)>,
    ) -> Vec<(String, SeqMarked<String>)> {
        it.map(|(k, v)| (k.clone(), v.cloned())).collect::<Vec<_>>()
    }

    fn k(s: impl ToString, seq: u64) -> (String, Reverse<SeqMarked<()>>) {
        (s.to_string(), rs(seq))
    }

    fn k_tomb(s: impl ToString, seq: u64) -> (String, Reverse<SeqMarked<()>>) {
        (s.to_string(), rs_tomb(seq))
    }

    fn sm(seq: u64, v: impl ToString) -> SeqMarked<String> {
        SeqMarked::new_normal(seq, v.to_string())
    }

    fn sm_tomb(seq: u64) -> SeqMarked<String> {
        SeqMarked::new_tombstone(seq)
    }

    fn ordkey(seq: u64) -> SeqMarked<()> {
        SeqMarked::new_normal(seq, ())
    }

    fn ordkey_tomb(seq: u64) -> SeqMarked<()> {
        SeqMarked::new_tombstone(seq)
    }

    fn s(x: impl ToString) -> String {
        x.to_string()
    }

    fn rs(seq: u64) -> Reverse<SeqMarked<()>> {
        Reverse(SeqMarked::new_normal(seq, ()))
    }

    fn rs_tomb(seq: u64) -> Reverse<SeqMarked<()>> {
        Reverse(SeqMarked::new_tombstone(seq))
    }
}
