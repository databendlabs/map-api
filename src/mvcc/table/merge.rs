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

use itertools::kmerge_by;
use itertools::Itertools;
use seq_marked::InternalSeq;

use crate::mvcc::coalesce::Coalesce;
use crate::mvcc::coalesce::Ent;
use crate::mvcc::Table;

impl<K, V> Table<K, V>
where
    K: Ord + fmt::Debug,
    K: Clone,
    V: Clone,
{
    /// Merge tables with MVCC coalescing based on minimum snapshot sequence.
    ///
    /// Records with same key are coalesced when older versions are invisible to all snapshots.
    /// A record at sequence `seq` is removed if `min_seq >= seq` and a newer version exists.
    ///
    /// `compact()` differs from `apply()`: removes older versions when invisible to snapshots.
    /// `apply()` is for fast writes, `compact()` is for compaction.
    pub fn compact(&self, other: &Self, min_snapshot_seq: InternalSeq) -> Self {
        // Track the highest sequence from both input tables
        let last_seq = if self.last_seq > other.last_seq {
            self.last_seq
        } else {
            other.last_seq
        };

        let it1 = self.inner.clone().into_iter();
        let it2 = other.inner.clone().into_iter();

        // merge two iterators
        let it = kmerge_by([it1, it2], |a: &Ent<K, V>, b: &Ent<K, V>| a.0 < b.0);

        // coalesce records with the same key, and the latter one can not be seen by any mvcc snapshot.
        let coalesce = Coalesce { min_snapshot_seq };
        let it = it.coalesce(|a, b| coalesce.coalesce(a, b));

        let bt = BTreeMap::from_iter(it);

        Self {
            inner: bt,
            last_seq,
        }
    }
}

#[cfg(test)]
mod tests {
    use std::cmp::Reverse;

    use seq_marked::SeqMarked;

    use super::*;

    type K = String;
    type V = String;

    fn k(s: impl ToString, seq: u64) -> (K, Reverse<SeqMarked<()>>) {
        (s.to_string(), Reverse(SeqMarked::new_normal(seq, ())))
    }

    fn k_tomb(s: impl ToString, seq: u64) -> (K, Reverse<SeqMarked<()>>) {
        (s.to_string(), Reverse(SeqMarked::new_tombstone(seq)))
    }

    fn s(x: impl ToString) -> V {
        x.to_string()
    }

    #[allow(clippy::type_complexity)]
    fn collect_inner(table: &Table<K, V>) -> Vec<((K, Reverse<SeqMarked<()>>), Option<V>)> {
        table
            .inner
            .iter()
            .map(|(k, v)| (k.clone(), v.clone()))
            .collect()
    }

    #[allow(clippy::type_complexity)]
    fn table_from_entries(entries: Vec<((K, Reverse<SeqMarked<()>>), Option<V>)>) -> Table<K, V> {
        let mut table = Table::new();
        let mut max_seq = SeqMarked::zero();

        for (key, value) in entries {
            let seq_marked = key.1 .0;
            if seq_marked > max_seq {
                max_seq = seq_marked;
            }
            table.inner.insert(key, value);
        }

        table.last_seq = max_seq;
        table
    }

    #[test]
    fn test_compact_comprehensive() {
        // Test case 1: Different keys - both should be kept
        let table1 = table_from_entries(vec![
            (k("a", 10), Some(s("v1"))),
            (k("c", 15), Some(s("v3"))),
        ]);
        let table2 = table_from_entries(vec![
            (k("b", 12), Some(s("v2"))),
            (k("d", 18), Some(s("v4"))),
        ]);

        let merged = table1.compact(&table2, InternalSeq::new(5));
        let actual = collect_inner(&merged);
        let expected = vec![
            (k("a", 10), Some(s("v1"))),
            (k("b", 12), Some(s("v2"))),
            (k("c", 15), Some(s("v3"))),
            (k("d", 18), Some(s("v4"))),
        ];
        assert_eq!(actual, expected);
        // last_seq should be max(15, 18) = 18
        assert_eq!(merged.last_seq, SeqMarked::new_normal(18, ()));

        // Test case 2: Same key, cannot coalesce (gr > min_seq)
        // min_seq=5, sequences: gr=15, sm=12
        // Since gr(15) > min_seq(5), cannot coalesce - keep both
        let table1 = table_from_entries(vec![
            (k("k", 15), Some(s("v_new"))), // greater seq
        ]);
        let table2 = table_from_entries(vec![
            (k("k", 12), Some(s("v_old"))), // smaller seq
        ]);

        let merged = table1.compact(&table2, InternalSeq::new(5));
        let actual = collect_inner(&merged);
        let expected = vec![
            (k("k", 15), Some(s("v_new"))),
            (k("k", 12), Some(s("v_old"))),
        ];
        assert_eq!(actual, expected);
        // last_seq should be max(15, 12) = 15
        assert_eq!(merged.last_seq, SeqMarked::new_normal(15, ()));

        // Test case 3: Same key, can coalesce (gr <= min_seq)
        // min_seq=20, sequences: gr=18, sm=10
        // Since gr(18) <= min_seq(20), can coalesce - keep greater seq only
        let table1 = table_from_entries(vec![
            (k("k", 18), Some(s("v_new"))), // greater seq
        ]);
        let table2 = table_from_entries(vec![
            (k("k", 10), Some(s("v_old"))), // smaller seq
        ]);

        let merged = table1.compact(&table2, InternalSeq::new(20));
        let actual = collect_inner(&merged);
        let expected = vec![(k("k", 18), Some(s("v_new")))];
        assert_eq!(actual, expected);
        // last_seq should be max(18, 10) = 18
        assert_eq!(merged.last_seq, SeqMarked::new_normal(18, ()));

        // Test case 4: Mixed record types (normal, tombstone) with coalescing
        let table1 = table_from_entries(vec![
            (k("k1", 15), Some(s("v1"))), // normal record, can coalesce
            (k_tomb("k2", 12), None),     // tombstone record, greater seq
        ]);
        let table2 = table_from_entries(vec![
            (k_tomb("k1", 12), None), // tombstone record, will be coalesced
            (k_tomb("k2", 10), None), // tombstone record, smaller seq
        ]);

        let merged = table1.compact(&table2, InternalSeq::new(20));
        let actual = collect_inner(&merged);
        let expected = vec![(k("k1", 15), Some(s("v1"))), (k_tomb("k2", 12), None)];
        assert_eq!(actual, expected);
        // last_seq should be max(15, 12) = 15
        assert_eq!(merged.last_seq, SeqMarked::new_normal(15, ()));

        // Test case 5: Complex scenario with multiple keys and mixed record types
        let table1 = table_from_entries(vec![
            (k("a", 20), Some(s("a_new"))),   // can coalesce with a_old
            (k("b", 15), Some(s("b_new"))),   // can coalesce with b_old
            (k_tomb("c", 25), None),          // tombstone
            (k("d", 30), Some(s("d_only1"))), // only in table1
            (k("f", 30), Some(s("f_new"))),   // cannot coalesce with f_old (gr > min_seq)
        ]);
        let table2 = table_from_entries(vec![
            (k("a", 10), Some(s("a_old"))),   // will be coalesced away
            (k("b", 12), Some(s("b_old"))),   // will be coalesced away
            (k("c", 8), Some(s("c_old"))),    // will be coalesced away
            (k("e", 35), Some(s("e_only2"))), // only in table2
            (k("f", 28), Some(s("f_old"))),   // cannot coalesce with f_new (gr > min_seq)
        ]);

        // min_seq=25
        // Coalesce condition: gr <= min_seq
        // a: gr=20, sm=10 -> gr(20) <= min_seq(25)? Yes. -> Coalesce, keep gr only
        // b: gr=15, sm=12 -> gr(15) <= min_seq(25)? Yes. -> Coalesce, keep gr only
        // c: gr=25, sm=8 -> gr(25) <= min_seq(25)? Yes. -> Coalesce, keep gr only
        // d: only in table1 -> keep 30
        // e: only in table2 -> keep 35
        // f: gr=30, sm=28 -> gr(30) <= min_seq(25)? No. -> Cannot coalesce, keep both
        let merged = table1.compact(&table2, InternalSeq::new(25));

        // Should have: a(20), b(15), c(25), d(30), e(35), f(30), f(28) = 7 entries
        let actual = collect_inner(&merged);
        let expected = vec![
            (k("a", 20), Some(s("a_new"))),
            (k("b", 15), Some(s("b_new"))),
            (k_tomb("c", 25), None),
            (k("d", 30), Some(s("d_only1"))),
            (k("e", 35), Some(s("e_only2"))),
            (k("f", 30), Some(s("f_new"))),
            (k("f", 28), Some(s("f_old"))),
        ];
        assert_eq!(actual, expected);
        // last_seq should be max(30, 35) = 35
        assert_eq!(merged.last_seq, SeqMarked::new_normal(35, ()));

        // Test case 6: Empty tables
        let empty1 = Table::new();
        let table_with_data = table_from_entries(vec![(k("x", 5), Some(s("value")))]);

        let merged = empty1.compact(&table_with_data, InternalSeq::new(1));
        let actual = collect_inner(&merged);
        let expected = vec![(k("x", 5), Some(s("value")))];
        assert_eq!(actual, expected);
        // last_seq should be max(0, 5) = 5
        assert_eq!(merged.last_seq, SeqMarked::new_normal(5, ()));

        // Test case 7: Tombstone sequences in last_seq tracking
        let table1 = table_from_entries(vec![
            (k_tomb("x", 50), None), // tombstone with highest seq
        ]);
        let table2 = table_from_entries(vec![
            (k("y", 40), Some(s("value"))), // normal record
        ]);

        let merged = table1.compact(&table2, InternalSeq::new(1));
        let actual = collect_inner(&merged);
        let expected = vec![(k_tomb("x", 50), None), (k("y", 40), Some(s("value")))];
        assert_eq!(actual, expected);
        // last_seq should be max(50, 40) = 50 (tombstone can be highest)
        assert_eq!(merged.last_seq, SeqMarked::new_tombstone(50));
    }
}
