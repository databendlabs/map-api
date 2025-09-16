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

//! MVCC record coalescing for compaction.
//!
//! This module provides functionality to coalesce (merge) duplicate records
//! with the same key during MVCC compaction, based on snapshot visibility.

use std::cmp::Reverse;
use std::fmt;

use seq_marked::InternalSeq;
use seq_marked::SeqMarked;

/// MVCC record coalescing engine.
///
/// Determines whether duplicate records with the same key can be safely merged
/// during compaction, based on the minimum active snapshot sequence number.
///
/// # Coalescing Logic
///
/// For two entries with the same key and sequences `gr` (greater) and `sm` (smaller),
/// where `gr > sm`, the smaller entry can be removed if no active snapshot can see it.
/// This happens when `gr <= min_seq`, meaning all active snapshots have sequence
/// numbers greater than `gr`, so neither entry is visible to any snapshot.
pub struct Coalesce {
    /// Sequence number of the minimum (oldest) active MVCC snapshot.
    /// Records with sequences <= this value cannot be seen by any active snapshot.
    pub(crate) min_snapshot_seq: InternalSeq,
}

/// Table entry type: `((key, reverse_seq), value)`.
///
/// - `K`: Key type
/// - `Reverse<SeqMarked<()>>`: Sequence in reverse order (newest first)
/// - `Option<V>`: Value (`None` for tombstone records)
pub(crate) type Ent<K, V> = ((K, Reverse<SeqMarked<()>>), Option<V>);

impl Coalesce {
    /// Attempts to coalesce two table entries based on MVCC snapshot visibility.
    ///
    /// # Arguments
    ///
    /// * `entry1` - First entry (should have greater sequence due to reverse ordering)
    /// * `entry2` - Second entry (should have smaller sequence due to reverse ordering)
    ///
    /// # Returns
    ///
    /// * `Ok(entry)` - Entries can be coalesced, returns the entry to keep
    /// * `Err((entry1, entry2))` - Entries cannot be coalesced, both must be kept
    ///
    /// # Coalescing Rules
    ///
    /// 1. **Different keys**: Always keep both entries
    /// 2. **Same key, `gr > min_seq`**: Keep both (snapshots may see the smaller entry)  
    /// 3. **Same key, `gr <= min_seq`**: Keep only greater entry (no snapshot can see either)
    ///
    /// # Panics
    ///
    /// Panics if `entry1` does not come before `entry2` in sort order.
    #[allow(clippy::type_complexity)]
    pub fn coalesce<K, V>(
        &self,
        ent1: Ent<K, V>,
        ent2: Ent<K, V>,
    ) -> Result<Ent<K, V>, (Ent<K, V>, Ent<K, V>)>
    where
        K: PartialEq + PartialOrd,
        K: fmt::Debug,
    {
        let k1 = &ent1.0;
        let k2 = &ent2.0;

        // Entries must be in sorted order for merge algorithms to work correctly
        assert!(
            k1 < k2,
            "entry1 must come before entry2 in sort order, but got: {:?} and {:?}",
            k1,
            k2
        );

        if k1.0 == k2.0 {
            // Same key: check if we can coalesce based on sequence visibility

            // Extract the greater sequence (entry1 comes first due to reverse ordering)
            let rseq_1 = &k1.1;
            let gr = rseq_1.0.internal_seq();

            if gr <= self.min_snapshot_seq {
                // No active snapshot can see either entry, safe to coalesce
                Ok(ent1)
            } else {
                // Active snapshots may see the smaller entry, must keep both
                //
                // Example timeline: [min_seq=10] ... [sm=12] [gr=15] ... [active snapshots]
                // Since gr(15) > min_seq(10), snapshots might exist that can see sm(12)
                Err((ent1, ent2))
            }
        } else {
            // Different keys: always keep both entries
            Err((ent1, ent2))
        }
    }
}

#[cfg(test)]
mod tests {
    use std::cmp::Reverse;

    use seq_marked::SeqMarked;

    use super::*;

    type K = &'static str;
    type V = &'static str;

    /// Creates a test entry with the specified key, sequence, and optional value.
    ///
    /// # Arguments
    ///
    /// * `k` - Key for the entry
    /// * `seq` - Sequence number
    /// * `v` - Value (`Some(value)` for normal record, `None` for tombstone)
    fn ent(k: K, seq: u64, v: Option<V>) -> Ent<K, V> {
        let seq_marked = match v {
            Some(_) => SeqMarked::new_normal(seq, ()),
            None => SeqMarked::new_tombstone(seq),
        };
        ((k, Reverse(seq_marked)), v)
    }

    /// Tests comprehensive coalescing scenarios.
    ///
    /// Validates the coalescing logic across different key scenarios,
    /// sequence relationships, and record types (normal/tombstone).
    #[test]
    fn test_coalesce_comprehensive() {
        // Case 1: Different keys → always keep both entries
        let coalesce = Coalesce {
            min_snapshot_seq: InternalSeq::new(5),
        };
        let e1 = ent("a", 8, Some("v1"));
        let e2 = ent("b", 6, Some("v2"));
        assert_eq!(coalesce.coalesce(e1, e2), Err((e1, e2)));

        // Case 2: Same key, gr > min_seq → cannot coalesce (keep both)
        // min_seq=5, gr=15: snapshots may exist that can see sm=12
        let coalesce = Coalesce {
            min_snapshot_seq: InternalSeq::new(5),
        };
        let gr = ent("k", 15, Some("v_new"));
        let sm = ent("k", 12, Some("v_old"));
        assert_eq!(coalesce.coalesce(gr, sm), Err((gr, sm)));

        // Case 3: Same key, gr <= min_seq → can coalesce (keep greater only)
        // min_seq=20, gr=18: no active snapshot can see either entry
        let coalesce = Coalesce {
            min_snapshot_seq: InternalSeq::new(20),
        };
        let gr = ent("k", 18, Some("v_new"));
        let sm = ent("k", 10, Some("v_old"));
        assert_eq!(coalesce.coalesce(gr, sm), Ok(gr));

        // Case 4: Tombstone record scenarios
        let coalesce = Coalesce {
            min_snapshot_seq: InternalSeq::new(20),
        };

        // Normal record + tombstone record
        let gr_normal = ent("k1", 15, Some("v1"));
        let sm_tomb = ent("k1", 12, None);
        assert_eq!(coalesce.coalesce(gr_normal, sm_tomb), Ok(gr_normal));

        // Both tombstone records
        let gr_tomb = ent("k2", 12, None);
        let sm_tomb = ent("k2", 10, None);
        assert_eq!(coalesce.coalesce(gr_tomb, sm_tomb), Ok(gr_tomb));

        // Case 5: Boundary condition (gr == min_seq) → can coalesce
        let coalesce = Coalesce {
            min_snapshot_seq: InternalSeq::new(10),
        };
        let gr = ent("k", 10, Some("v1"));
        let sm = ent("k", 5, Some("v2"));
        assert_eq!(coalesce.coalesce(gr, sm), Ok(gr));
    }
}
