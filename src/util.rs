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

//! Utility functions and types for the map API.
use std::fmt;
use std::io;

use seq_marked::SeqMarked;

/// Result type of key-value pair and io Error used in a map.
type KVResult<K, V> = Result<(K, SeqMarked<V>), io::Error>;

/// Comparator function for sorting key-value results by key and internal sequence number.
///
/// This function is used to establish a total ordering of key-value pairs where:
/// 1. Entries are first ordered by their keys
/// 2. For the same key, entries are ordered by their internal sequence numbers
///
/// Returns `true` if `r1` should be placed before `r2` in the sorted order.
pub fn by_key_seq<K, V>(r1: &KVResult<K, V>, r2: &KVResult<K, V>) -> bool
where K: Ord + fmt::Debug {
    match (r1, r2) {
        (Ok((k1, v1)), Ok((k2, v2))) => {
            let iseq1 = v1.order_key();
            let iseq2 = v2.order_key();

            // Same (key, seq) is only allowed if they are both tombstone:
            // `MapApi::set(None)` when there is already a tombstone produces
            // another tombstone with the same internal_seq.
            assert!(
                (k1, iseq1) != (k2, iseq2) || (iseq1.is_tombstone() && iseq2.is_tombstone()),
                "by_key_seq: same (key, internal_seq) and not all tombstone: k1:{:?} v1.internal_seq:{:?} k2:{:?} v2.internal_seq:{:?}",
                k1,
                iseq1,
                k2,
                iseq2,
            );

            // Put entries with the same key together, smaller internal-seq first
            // Tombstone is always greater.
            (k1, v1.order_key()) <= (k2, v2.order_key())
        }
        // If there is an error, just yield them in order.
        // It's the caller's responsibility to handle the error.
        _ => true,
    }
}

/// Attempts to merge two consecutive key-value results with the same key.
///
/// If the keys are equal, returns `Ok(combined)` where the values are merged by taking the greater one.
/// Otherwise, returns `Err((r1, r2))` to indicate that the results should not be merged.
#[allow(clippy::type_complexity)]
pub fn merge_kv_results<K, V>(
    r1: KVResult<K, V>,
    r2: KVResult<K, V>,
) -> Result<KVResult<K, V>, (KVResult<K, V>, KVResult<K, V>)>
where
    K: Ord,
{
    match (r1, r2) {
        (Ok((k1, v1)), Ok((k2, v2))) if k1 == k2 => Ok(Ok((k1, crate::SeqMarked::max(v1, v2)))),
        // If there is an error,
        // or k1 != k2
        // just yield them without change.
        (r1, r2) => Err((r1, r2)),
    }
}

#[cfg(test)]
mod tests {
    use std::io::Error;
    use std::io::ErrorKind;

    use seq_marked::SeqMarked;

    use super::*;

    #[test]
    fn test_by_key_seq_different_keys() {
        let r1: KVResult<String, String> =
            Ok(("a".to_string(), SeqMarked::new_normal(1, "v1".to_string())));
        let r2: KVResult<String, String> =
            Ok(("b".to_string(), SeqMarked::new_normal(2, "v2".to_string())));

        assert!(by_key_seq(&r1, &r2)); // "a" < "b"
        assert!(!by_key_seq(&r2, &r1)); // "b" > "a"
    }

    #[test]
    fn test_by_key_seq_same_key_different_seq() {
        let r1: KVResult<String, String> = Ok((
            "key".to_string(),
            SeqMarked::new_normal(1, "v1".to_string()),
        ));
        let r2: KVResult<String, String> = Ok((
            "key".to_string(),
            SeqMarked::new_normal(2, "v2".to_string()),
        ));

        assert!(by_key_seq(&r1, &r2)); // seq 1 < seq 2
        assert!(!by_key_seq(&r2, &r1)); // seq 2 > seq 1
    }

    #[test]
    fn test_by_key_seq_same_key_normal_vs_tombstone() {
        let normal: KVResult<String, String> = Ok((
            "key".to_string(),
            SeqMarked::new_normal(1, "v1".to_string()),
        ));
        let tombstone: KVResult<String, String> =
            Ok(("key".to_string(), SeqMarked::new_tombstone(1)));

        assert!(by_key_seq(&normal, &tombstone)); // normal < tombstone at same seq
        assert!(!by_key_seq(&tombstone, &normal)); // tombstone > normal at same seq
    }

    #[test]
    fn test_by_key_seq_same_key_both_tombstones() {
        let tomb1: KVResult<String, String> = Ok(("key".to_string(), SeqMarked::new_tombstone(1)));
        let tomb2: KVResult<String, String> = Ok(("key".to_string(), SeqMarked::new_tombstone(1)));

        // Both tombstones with same seq are allowed and should be equal
        assert!(by_key_seq(&tomb1, &tomb2));
        assert!(by_key_seq(&tomb2, &tomb1));
    }

    #[test]
    #[should_panic(expected = "by_key_seq: same (key, internal_seq) and not all tombstone")]
    fn test_by_key_seq_same_key_seq_panic() {
        let r1: KVResult<String, String> = Ok((
            "key".to_string(),
            SeqMarked::new_normal(1, "v1".to_string()),
        ));
        let r2: KVResult<String, String> = Ok((
            "key".to_string(),
            SeqMarked::new_normal(1, "v2".to_string()),
        ));

        // This should panic because same (key, seq) with normal values is not allowed
        by_key_seq(&r1, &r2);
    }

    #[test]
    fn test_by_key_seq_with_errors() {
        let ok_result: KVResult<String, String> = Ok((
            "key".to_string(),
            SeqMarked::new_normal(1, "v1".to_string()),
        ));
        let err_result: KVResult<String, String> = Err(Error::new(ErrorKind::Other, "test error"));

        // Error results should always yield true (maintain order)
        assert!(by_key_seq(&err_result, &ok_result));
        assert!(by_key_seq(&ok_result, &err_result));
        assert!(by_key_seq(&err_result, &err_result));
    }

    #[test]
    fn test_by_key_seq_equal_keys_different_seqs_ordering() {
        let r1: KVResult<String, String> = Ok((
            "key".to_string(),
            SeqMarked::new_normal(1, "v1".to_string()),
        ));
        let r2: KVResult<String, String> = Ok((
            "key".to_string(),
            SeqMarked::new_normal(2, "v2".to_string()),
        ));

        // Lower sequence should come first (<=)
        assert!(by_key_seq(&r1, &r2));
        assert!(!by_key_seq(&r2, &r1));
    }

    #[test]
    fn test_merge_kv_results_same_key() {
        let r1: KVResult<String, String> = Ok((
            "key".to_string(),
            SeqMarked::new_normal(1, "v1".to_string()),
        ));
        let r2: KVResult<String, String> = Ok((
            "key".to_string(),
            SeqMarked::new_normal(2, "v2".to_string()),
        ));

        let result = merge_kv_results(r1, r2).unwrap();

        match result {
            Ok((key, value)) => {
                assert_eq!(key, "key");
                assert_eq!(value, SeqMarked::new_normal(2, "v2".to_string())); // Higher seq wins
            }
            _ => panic!("Expected Ok result"),
        }
    }

    #[test]
    fn test_merge_kv_results_different_keys() {
        let r1: KVResult<String, String> = Ok((
            "key1".to_string(),
            SeqMarked::new_normal(1, "v1".to_string()),
        ));
        let r2: KVResult<String, String> = Ok((
            "key2".to_string(),
            SeqMarked::new_normal(2, "v2".to_string()),
        ));

        let result = merge_kv_results(r1, r2);

        assert!(result.is_err());
        let (returned_r1, returned_r2) = result.unwrap_err();
        assert_eq!(returned_r1.unwrap().0, "key1");
        assert_eq!(returned_r2.unwrap().0, "key2");
    }

    #[test]
    fn test_merge_kv_results_with_tombstone() {
        let normal: KVResult<String, String> = Ok((
            "key".to_string(),
            SeqMarked::new_normal(1, "v1".to_string()),
        ));
        let tombstone: KVResult<String, String> =
            Ok(("key".to_string(), SeqMarked::new_tombstone(2)));

        let result = merge_kv_results(normal, tombstone).unwrap();

        match result {
            Ok((key, value)) => {
                assert_eq!(key, "key");
                assert!(value.is_tombstone());
                assert_eq!(value, SeqMarked::new_tombstone(2));
            }
            _ => panic!("Expected Ok result"),
        }
    }

    #[test]
    fn test_merge_kv_results_with_errors() {
        let ok_result: KVResult<String, String> = Ok((
            "key".to_string(),
            SeqMarked::new_normal(1, "v1".to_string()),
        ));
        let err_result: KVResult<String, String> = Err(Error::new(ErrorKind::Other, "test error"));

        // Error results should not be merged - create fresh instances since Error doesn't clone
        let ok_result2: KVResult<String, String> = Ok((
            "key".to_string(),
            SeqMarked::new_normal(1, "v1".to_string()),
        ));
        let err_result2: KVResult<String, String> = Err(Error::new(ErrorKind::Other, "test error"));
        let _ok_result3: KVResult<String, String> = Ok((
            "key".to_string(),
            SeqMarked::new_normal(1, "v1".to_string()),
        ));
        let err_result3: KVResult<String, String> = Err(Error::new(ErrorKind::Other, "test error"));
        let err_result4: KVResult<String, String> = Err(Error::new(ErrorKind::Other, "test error"));

        let result1 = merge_kv_results(ok_result, err_result);
        assert!(result1.is_err());

        let result2 = merge_kv_results(err_result2, ok_result2);
        assert!(result2.is_err());

        let result3 = merge_kv_results(err_result3, err_result4);
        assert!(result3.is_err());
    }

    #[test]
    fn test_merge_kv_results_normal_vs_tombstone_same_seq() {
        let normal: KVResult<String, String> = Ok((
            "key".to_string(),
            SeqMarked::new_normal(1, "v1".to_string()),
        ));
        let tombstone: KVResult<String, String> =
            Ok(("key".to_string(), SeqMarked::new_tombstone(1)));

        let result = merge_kv_results(normal, tombstone).unwrap();

        match result {
            Ok((key, value)) => {
                assert_eq!(key, "key");
                // Tombstone should win even at same seq due to order_key behavior
                assert!(value.is_tombstone());
            }
            _ => panic!("Expected Ok result"),
        }
    }

    #[test]
    fn test_merge_kv_results_preserves_higher_seq() {
        let lower_seq: KVResult<String, String> = Ok((
            "key".to_string(),
            SeqMarked::new_normal(5, "v5".to_string()),
        ));
        let higher_seq: KVResult<String, String> = Ok((
            "key".to_string(),
            SeqMarked::new_normal(10, "v10".to_string()),
        ));

        let result = merge_kv_results(lower_seq, higher_seq).unwrap();

        match result {
            Ok((key, value)) => {
                assert_eq!(key, "key");
                assert_eq!(value, SeqMarked::new_normal(10, "v10".to_string()));
            }
            _ => panic!("Expected Ok result"),
        }
    }
}
