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

//! Defines the `Marked` enum for representing values with metadata and tombstones.
//!
//! This module provides the core value representation used throughout the library.
//! The `Marked` enum can represent both normal values with metadata and tombstones
//! (deleted values), which is essential for replication and synchronization in
//! distributed systems.

#[cfg(test)]
mod marked_test;

mod marked_impl;
mod seq_tombstone;

pub(crate) use seq_tombstone::SeqTombstone;

use crate::seq_value::SeqV;
use crate::seq_value::SeqValue;

/// A versioned value wrapper that can represent both normal values and tombstones (deleted values).
///
/// The `Marked` enum is a core type in this library that wraps values with:
/// - A sequence number for versioning
/// - Optional metadata
/// - Support for representing deleted values (tombstones)
///
/// This type is essential for implementing distributed key-value stores where
/// tracking deletions is as important as tracking additions and updates.
///
/// # Sequence Numbers
///
/// The `internal_seq` field is used internally and is different from the `seq` in `SeqV`,
/// which is used by the application:
/// - A normal entry (non-deleted) has a positive `seq` that is the same as the corresponding `internal_seq`
/// - A deleted tombstone also has an `internal_seq`, while for an application, a deleted entry has `seq=0`
///
/// # Type Parameters
///
/// - `M`: The metadata type associated with values
/// - `T`: The value type, defaults to `Vec<u8>`
///
/// # Examples
///
/// ```
/// use map_api::SeqMarked;
///
/// // Create a normal value
/// let normal = SeqMarked::<(), Vec<u8>>::new_normal(1, vec![1, 2, 3]);
///
/// // Create a tombstone (deleted value)
/// let tombstone = SeqMarked::<(), Vec<u8>>::new_tombstone(2);
///
/// // Check if a value is a tombstone
/// assert!(!normal.is_tombstone());
/// assert!(tombstone.is_tombstone());
/// ```
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum SeqMarked<M, T = Vec<u8>> {
    /// Represents a deleted value (tombstone).
    ///
    /// A tombstone is used to mark a key as deleted while preserving its
    /// sequence number for replication and synchronization purposes.
    TombStone {
        /// The internal sequence number of the tombstone.
        internal_seq: u64,
    },

    /// Represents a normal (non-deleted) value.
    ///
    /// A normal value contains the actual value data, its sequence number,
    /// and optional metadata.
    Normal {
        /// The internal sequence number of the value.
        internal_seq: u64,
        /// The actual value data.
        value: T,
        /// Optional metadata associated with the value.
        meta: Option<M>,
    },
}

impl<M, T> From<(u64, T, Option<M>)> for SeqMarked<M, T> {
    fn from((seq, value, meta): (u64, T, Option<M>)) -> Self {
        assert_ne!(seq, 0);

        SeqMarked::Normal {
            internal_seq: seq,
            value,
            meta,
        }
    }
}

impl<M, T> From<SeqV<M, T>> for SeqMarked<M, T> {
    fn from(value: SeqV<M, T>) -> Self {
        SeqMarked::new_with_meta(value.seq, value.data, value.meta)
    }
}

impl<M, T> SeqValue<M, T> for SeqMarked<M, T> {
    fn seq(&self) -> u64 {
        match self {
            SeqMarked::TombStone { internal_seq: _ } => 0,
            SeqMarked::Normal {
                internal_seq: seq, ..
            } => *seq,
        }
    }

    fn value(&self) -> Option<&T> {
        match self {
            SeqMarked::TombStone { internal_seq: _ } => None,
            SeqMarked::Normal {
                internal_seq: _,
                value,
                meta: _,
            } => Some(value),
        }
    }

    fn into_value(self) -> Option<T> {
        match self {
            SeqMarked::TombStone { internal_seq: _ } => None,
            SeqMarked::Normal {
                internal_seq: _,
                value,
                meta: _,
            } => Some(value),
        }
    }

    fn meta(&self) -> Option<&M> {
        match self {
            SeqMarked::TombStone { .. } => None,
            SeqMarked::Normal { meta, .. } => meta.as_ref(),
        }
    }
}

impl<M, T> SeqMarked<M, T> {
    pub const fn empty() -> Self {
        SeqMarked::TombStone { internal_seq: 0 }
    }

    /// Return a key to determine which one of the values of the same key are the last inserted.
    pub fn order_key(&self) -> SeqTombstone {
        match self {
            SeqMarked::TombStone { internal_seq: seq } => SeqTombstone::tombstone(*seq),
            SeqMarked::Normal {
                internal_seq: seq, ..
            } => SeqTombstone::normal(*seq),
        }
    }

    pub fn unpack(self) -> Option<(T, Option<M>)> {
        match self {
            SeqMarked::TombStone { internal_seq: _ } => None,
            SeqMarked::Normal {
                internal_seq: _,
                value,
                meta,
            } => Some((value, meta)),
        }
    }

    pub fn unpack_ref(&self) -> Option<(&T, Option<&M>)> {
        match self {
            SeqMarked::TombStone { internal_seq: _ } => None,
            SeqMarked::Normal {
                internal_seq: _,
                value,
                meta,
            } => Some((value, meta.as_ref())),
        }
    }

    /// Return the one with the larger sequence number.
    pub fn max(a: Self, b: Self) -> Self {
        if a.order_key() > b.order_key() {
            a
        } else {
            b
        }
    }

    /// Return the one with the larger sequence number.
    // Not used, may be useful.
    #[allow(dead_code)]
    pub fn max_ref<'l>(a: &'l Self, b: &'l Self) -> &'l Self {
        if a.order_key() > b.order_key() {
            a
        } else {
            b
        }
    }

    pub fn new_tombstone(internal_seq: u64) -> Self {
        SeqMarked::TombStone { internal_seq }
    }

    #[allow(dead_code)]
    pub fn new_normal(seq: u64, value: T) -> Self {
        SeqMarked::Normal {
            internal_seq: seq,
            value,
            meta: None,
        }
    }

    pub fn new_with_meta(seq: u64, value: T, meta: Option<M>) -> Self {
        SeqMarked::Normal {
            internal_seq: seq,
            value,
            meta,
        }
    }

    #[allow(dead_code)]
    pub fn with_meta(self, meta: Option<M>) -> Self {
        match self {
            SeqMarked::TombStone { .. } => {
                unreachable!("Tombstone has no meta")
            }
            SeqMarked::Normal {
                internal_seq,
                value,
                ..
            } => SeqMarked::Normal {
                internal_seq,
                value,
                meta,
            },
        }
    }

    /// Return if the entry is neither a normal entry nor a tombstone.
    pub fn is_not_found(&self) -> bool {
        matches!(self, SeqMarked::TombStone { internal_seq: 0 })
    }

    pub fn is_tombstone(&self) -> bool {
        matches!(self, SeqMarked::TombStone { .. })
    }

    #[allow(dead_code)]
    pub(crate) fn is_normal(&self) -> bool {
        matches!(self, SeqMarked::Normal { .. })
    }
}

impl<M, T> From<SeqMarked<M, T>> for Option<SeqV<M, T>> {
    fn from(value: SeqMarked<M, T>) -> Self {
        match value {
            SeqMarked::TombStone { internal_seq: _ } => None,
            SeqMarked::Normal {
                internal_seq: seq,
                value,
                meta,
            } => Some(SeqV::with_meta(seq, meta, value)),
        }
    }
}

#[cfg(test)]
mod tests {

    use super::SeqMarked;

    #[test]
    fn test_marked_new() {
        let m = SeqMarked::new_normal(1, "a");
        assert_eq!(
            SeqMarked::Normal {
                internal_seq: 1,
                value: "a",
                meta: None
            },
            m
        );

        let m = m.with_meta(Some(20u64));

        assert_eq!(
            SeqMarked::Normal {
                internal_seq: 1,
                value: "a",
                meta: Some(20u64)
            },
            m
        );

        let m = SeqMarked::new_with_meta(2, "b", Some(30u64));

        assert_eq!(
            SeqMarked::Normal {
                internal_seq: 2,
                value: "b",
                meta: Some(30u64)
            },
            m
        );

        let m: SeqMarked<u32> = SeqMarked::new_tombstone(3);
        assert_eq!(SeqMarked::TombStone { internal_seq: 3 }, m);
    }
}
