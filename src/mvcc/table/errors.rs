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

use seq_marked::SeqMarked;

/// Errors that can occur during table insert operations.
#[derive(Clone, PartialEq, Eq, thiserror::Error, Debug)]
pub enum InsertError {
    /// Sequence number is not monotonically increasing.
    ///
    /// All normal inserts must have strictly increasing sequences.
    /// Tombstones may reuse the current sequence.
    #[error("NonIncremental: current={current:?} >= last={last:?}(equal only allowed for tombstone) does not hold")]
    NonIncremental {
        last: SeqMarked<()>,
        current: SeqMarked<()>,
    },
}

#[cfg(test)]
mod tests {
    use seq_marked::SeqMarked;

    use super::*;

    #[test]
    fn test_insert_error_display() {
        let err = InsertError::NonIncremental {
            last: SeqMarked::new_normal(5, ()),
            current: SeqMarked::new_normal(3, ()),
        };

        let expected = "NonIncremental: current=SeqMarked { seq: 3, marked: Normal(()) } >= last=SeqMarked { seq: 5, marked: Normal(()) }(equal only allowed for tombstone) does not hold";
        assert_eq!(err.to_string(), expected);
    }

    #[test]
    fn test_insert_error_display_with_tombstone() {
        let err = InsertError::NonIncremental {
            last: SeqMarked::new_normal(5, ()),
            current: SeqMarked::new_tombstone(5),
        };

        let expected = "NonIncremental: current=SeqMarked { seq: 5, marked: TombStone } >= last=SeqMarked { seq: 5, marked: Normal(()) }(equal only allowed for tombstone) does not hold";
        assert_eq!(err.to_string(), expected);
    }

    #[test]
    fn test_insert_error_clone() {
        let err1 = InsertError::NonIncremental {
            last: SeqMarked::new_normal(10, ()),
            current: SeqMarked::new_normal(8, ()),
        };

        let err2 = err1.clone();
        assert_eq!(err1, err2);

        // Verify both have same content
        assert_eq!(format!("{:?}", err1), format!("{:?}", err2));
    }

    #[test]
    fn test_insert_error_eq() {
        let err1 = InsertError::NonIncremental {
            last: SeqMarked::new_tombstone(7),
            current: SeqMarked::new_normal(5, ()),
        };

        let err2 = InsertError::NonIncremental {
            last: SeqMarked::new_tombstone(7),
            current: SeqMarked::new_normal(5, ()),
        };

        let err3 = InsertError::NonIncremental {
            last: SeqMarked::new_tombstone(8),
            current: SeqMarked::new_normal(5, ()),
        };

        assert_eq!(err1, err2);
        assert_ne!(err1, err3);
    }

    #[test]
    fn test_insert_error_debug() {
        let err = InsertError::NonIncremental {
            last: SeqMarked::new_normal(15, ()),
            current: SeqMarked::new_tombstone(12),
        };

        let debug = format!("{:?}", err);
        assert!(debug.contains("NonIncremental"));
        assert!(debug.contains("last:"));
        assert!(debug.contains("current:"));
    }

    #[test]
    fn test_insert_error_different_marked_types() {
        // Test with different combinations of SeqMarked types
        let cases = vec![
            (SeqMarked::new_normal(10, ()), SeqMarked::new_normal(8, ())),
            (SeqMarked::new_normal(10, ()), SeqMarked::new_tombstone(8)),
            (SeqMarked::new_tombstone(10), SeqMarked::new_normal(8, ())),
            (SeqMarked::new_tombstone(10), SeqMarked::new_tombstone(9)),
            (SeqMarked::new_normal(10, ()), SeqMarked::new_tombstone(10)), /* Equal seq, tombstone allowed */
        ];

        for (last, current) in cases {
            let err = InsertError::NonIncremental { last, current };

            // Verify error can be created and displayed
            let _ = err.to_string();
            let _ = format!("{:?}", err);

            // Verify clone works
            let cloned = err.clone();
            assert_eq!(err, cloned);
        }
    }
}
