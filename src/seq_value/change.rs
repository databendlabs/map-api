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

use crate::SeqV;

/// An update event for a key.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Change<M, V = Vec<u8>> {
    pub key: String,
    pub before: Option<SeqV<M, V>>,
    pub after: Option<SeqV<M, V>>,
}

impl<M, V> Change<M, V> {
    pub fn new(key: String, before: Option<SeqV<M, V>>, after: Option<SeqV<M, V>>) -> Self {
        Self { key, before, after }
    }

    pub fn is_delete(&self) -> bool {
        self.after.is_none()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::SeqV;

    #[test]
    fn test_change_new() {
        let before = Some(SeqV::new(1, "old_value".as_bytes().to_vec()));
        let after = Some(SeqV::new(2, "new_value".as_bytes().to_vec()));

        let change =
            Change::<(), Vec<u8>>::new("test_key".to_string(), before.clone(), after.clone());

        assert_eq!(change.key, "test_key");
        assert_eq!(change.before, before);
        assert_eq!(change.after, after);
    }

    #[test]
    fn test_change_is_delete_true() {
        let before = Some(SeqV::new(1, "value".as_bytes().to_vec()));
        let change = Change::<(), Vec<u8>>::new("key".to_string(), before, None);

        assert!(change.is_delete());
    }

    #[test]
    fn test_change_is_delete_false() {
        let before = Some(SeqV::new(1, "old".as_bytes().to_vec()));
        let after = Some(SeqV::new(2, "new".as_bytes().to_vec()));
        let change = Change::<(), Vec<u8>>::new("key".to_string(), before, after);

        assert!(!change.is_delete());
    }

    #[test]
    fn test_change_insert() {
        let after = Some(SeqV::new(1, "new_value".as_bytes().to_vec()));
        let change = Change::<(), Vec<u8>>::new("new_key".to_string(), None, after.clone());

        assert_eq!(change.key, "new_key");
        assert_eq!(change.before, None);
        assert_eq!(change.after, after);
        assert!(!change.is_delete());
    }

    #[test]
    fn test_change_update() {
        let before = Some(SeqV::new(1, "old".as_bytes().to_vec()));
        let after = Some(SeqV::new(2, "new".as_bytes().to_vec()));
        let change = Change::<(), Vec<u8>>::new("key".to_string(), before.clone(), after.clone());

        assert_eq!(change.key, "key");
        assert_eq!(change.before, before);
        assert_eq!(change.after, after);
        assert!(!change.is_delete());
    }

    #[test]
    fn test_change_clone_and_eq() {
        let before = Some(SeqV::new(1, vec![1, 2, 3]));
        let after = Some(SeqV::new(2, vec![4, 5, 6]));
        let change1 = Change::<(), Vec<u8>>::new("key".to_string(), before, after);
        let change2 = change1.clone();

        assert_eq!(change1, change2);
    }
}
