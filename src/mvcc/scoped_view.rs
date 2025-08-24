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

use std::io;

use seq_marked::SeqMarked;

use crate::mvcc::ScopedViewReadonly;
use crate::mvcc::ViewKey;
use crate::mvcc::ViewValue;

/// A view bound to a specific namespace.
///
/// This trait provides read-write access to data within a namespace that is embedded
/// in the view implementation, eliminating the need to specify namespace
/// parameters for each operation.
///
/// Extends [`ScopedViewReadonly`] with write operations:
/// - [`set`](Self::set) - Set or delete values within the scoped namespace  
/// - [`fetch_and_set`](Self::fetch_and_set) - Atomically get old value and set new value, returning both
#[async_trait::async_trait]
pub trait ScopedView<K, V>
where
    K: ViewKey,
    V: ViewValue,
    Self: ScopedViewReadonly<K, V>,
{
    /// Fetch the current value of a key and set it to a new value atomically.
    ///
    /// Returns a tuple of (old_value, new_value) where:
    /// - `old_value` is the previous value (or `SeqMarked::new_not_found()` if key didn't exist)
    /// - `new_value` is the newly set value (or `SeqMarked::new_tombstone()` if deleted)
    ///
    /// This is useful for atomic get-then-set operations within the scoped namespace.
    async fn fetch_and_set(
        &mut self,
        key: K,
        value: Option<V>,
    ) -> Result<(SeqMarked<V>, SeqMarked<V>), io::Error> {
        let old_value = self.get(key.clone()).await?;
        let order_key = self.set(key.clone(), value.clone());
        let new_value = match value {
            Some(v) => order_key.map(|_| v),
            None => SeqMarked::new_tombstone(*order_key.internal_seq()),
        };
        Ok((old_value, new_value))
    }

    /// Set or delete a value within the scoped namespace.
    ///
    /// Returns the sequence marker of the newly set value.
    ///
    /// For atomic get-then-set operations, see [`fetch_and_set`](Self::fetch_and_set).
    fn set(&mut self, key: K, value: Option<V>) -> SeqMarked<()>;
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;
    use std::io;
    use std::ops::RangeBounds;

    use futures::StreamExt;
    use seq_marked::InternalSeq;
    use seq_marked::SeqMarked;

    use super::*;
    use crate::IOResultStream;

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

    // Mock implementation for testing
    struct MockScopedView {
        base_seq: InternalSeq,
        data: BTreeMap<TestKey, SeqMarked<TestValue>>,
    }

    impl MockScopedView {
        fn new() -> Self {
            let mut data = BTreeMap::new();
            data.insert(
                key("initial_key"),
                SeqMarked::new_normal(1, value("initial_value")),
            );

            Self {
                base_seq: InternalSeq::new(5),
                data,
            }
        }
    }

    #[async_trait::async_trait]
    impl ScopedViewReadonly<TestKey, TestValue> for MockScopedView {
        fn base_seq(&self) -> InternalSeq {
            self.base_seq
        }

        async fn get(&self, key: TestKey) -> Result<SeqMarked<TestValue>, io::Error> {
            match self.data.get(&key) {
                Some(value) => Ok(value.clone()),
                None => Ok(SeqMarked::new_not_found()),
            }
        }

        async fn range<R>(
            &self,
            range: R,
        ) -> Result<IOResultStream<(TestKey, SeqMarked<TestValue>)>, io::Error>
        where
            R: RangeBounds<TestKey> + Send + Sync + Clone + 'static,
        {
            let items: Vec<_> = self
                .data
                .range(range)
                .map(|(k, v)| Ok((k.clone(), v.clone())))
                .collect();

            let stream = futures::stream::iter(items);
            Ok(Box::pin(stream))
        }
    }

    #[async_trait::async_trait]
    impl ScopedView<TestKey, TestValue> for MockScopedView {
        fn set(&mut self, key: TestKey, value: Option<TestValue>) -> SeqMarked<()> {
            match value {
                Some(v) => {
                    self.data.insert(key, SeqMarked::new_normal(2, v));
                    SeqMarked::new_normal(2, ())
                }
                None => {
                    self.data.insert(key, SeqMarked::new_tombstone(2));
                    SeqMarked::new_tombstone(2)
                }
            }
        }
    }

    #[tokio::test]
    async fn test_scoped_view_trait_set_value() {
        let mut view = MockScopedView::new();
        let order_key = view.set(key("new_key"), Some(value("new_value")));

        // Should return the order key
        assert_eq!(order_key, SeqMarked::new_normal(2, ()));

        // Verify through the read-only interface
        let result = view.get(key("new_key")).await.unwrap();
        assert_eq!(result, SeqMarked::new_normal(2, value("new_value")));
    }

    #[tokio::test]
    async fn test_scoped_view_trait_set_tombstone() {
        let mut view = MockScopedView::new();
        let order_key = view.set(key("tombstone_key"), None);

        // Should return the order key for tombstone
        assert_eq!(order_key, SeqMarked::new_tombstone(2));

        // Verify through the read-only interface
        let result = view.get(key("tombstone_key")).await.unwrap();
        assert_eq!(result, SeqMarked::new_tombstone(2));
    }

    #[tokio::test]
    async fn test_scoped_view_trait_overwrite_existing() {
        let mut view = MockScopedView::new();

        // Verify initial value
        let initial = view.get(key("initial_key")).await.unwrap();
        assert_eq!(initial, SeqMarked::new_normal(1, value("initial_value")));

        // Overwrite it
        let order_key = view.set(key("initial_key"), Some(value("updated_value")));

        // Should return the order key
        assert_eq!(order_key, SeqMarked::new_normal(2, ()));

        // Should see the new value
        let updated = view.get(key("initial_key")).await.unwrap();
        assert_eq!(updated, SeqMarked::new_normal(2, value("updated_value")));
    }

    #[tokio::test]
    async fn test_scoped_view_trait_inherits_readonly() {
        let mut view = MockScopedView::new();

        // Test that ScopedView implements ScopedViewReadonly
        let order_key = view.set(key("test_key"), Some(value("test_value")));

        // Should return the order key
        assert_eq!(order_key, SeqMarked::new_normal(2, ()));

        assert_eq!(view.base_seq(), InternalSeq::new(5));
        let result = view.get(key("test_key")).await.unwrap();
        assert_eq!(result, SeqMarked::new_normal(2, value("test_value")));
    }

    #[tokio::test]
    async fn test_scoped_view_trait_range_after_set() {
        let mut view = MockScopedView::new();
        let order_key = view.set(key("added_key"), Some(value("added_value")));

        // Should return the order key
        assert_eq!(order_key, SeqMarked::new_normal(2, ()));

        let mut stream = view.range(..).await.unwrap();
        let mut results = Vec::new();
        while let Some(result) = stream.next().await {
            results.push(result.unwrap());
        }

        assert_eq!(results.len(), 2);
        assert_eq!(
            results[0],
            (
                key("added_key"),
                SeqMarked::new_normal(2, value("added_value"))
            )
        );
        assert_eq!(
            results[1],
            (
                key("initial_key"),
                SeqMarked::new_normal(1, value("initial_value"))
            )
        );
    }

    #[tokio::test]
    async fn test_scoped_view_trait_fetch_and_set_nonexistent() {
        let mut view = MockScopedView::new();

        let (old_value, new_value) = view
            .fetch_and_set(key("new_key"), Some(value("new_value")))
            .await
            .unwrap();

        // Should return not_found for old value
        assert_eq!(old_value, SeqMarked::new_not_found());
        // Should return the new value
        assert_eq!(new_value, SeqMarked::new_normal(2, value("new_value")));

        // Verify the value was actually set
        let current_value = view.get(key("new_key")).await.unwrap();
        assert_eq!(current_value, SeqMarked::new_normal(2, value("new_value")));
    }

    #[tokio::test]
    async fn test_scoped_view_trait_fetch_and_set_existing() {
        let mut view = MockScopedView::new();

        let (old_value, new_value) = view
            .fetch_and_set(key("initial_key"), Some(value("updated_value")))
            .await
            .unwrap();

        // Should return the old value
        assert_eq!(old_value, SeqMarked::new_normal(1, value("initial_value")));
        // Should return the new value
        assert_eq!(new_value, SeqMarked::new_normal(2, value("updated_value")));

        // Verify the value was updated
        let current_value = view.get(key("initial_key")).await.unwrap();
        assert_eq!(
            current_value,
            SeqMarked::new_normal(2, value("updated_value"))
        );
    }

    #[tokio::test]
    async fn test_scoped_view_trait_fetch_and_set_tombstone() {
        let mut view = MockScopedView::new();

        let (old_value, new_value) = view.fetch_and_set(key("initial_key"), None).await.unwrap();

        // Should return the old value
        assert_eq!(old_value, SeqMarked::new_normal(1, value("initial_value")));
        // Should return tombstone as new value
        assert_eq!(new_value, SeqMarked::new_tombstone(2));

        // Verify the value was deleted
        let current_value = view.get(key("initial_key")).await.unwrap();
        assert_eq!(current_value, SeqMarked::new_tombstone(2));
    }

    #[tokio::test]
    async fn test_scoped_view_trait_fetch_and_set_tombstone_to_value() {
        let mut view = MockScopedView::new();

        // First delete the key
        view.set(key("initial_key"), None);

        let (old_value, new_value) = view
            .fetch_and_set(key("initial_key"), Some(value("resurrected")))
            .await
            .unwrap();

        // Should return the tombstone as old value
        assert_eq!(old_value, SeqMarked::new_tombstone(2));
        // Should return the new value
        assert_eq!(new_value, SeqMarked::new_normal(2, value("resurrected")));

        // Verify the value was resurrected
        let current_value = view.get(key("initial_key")).await.unwrap();
        assert_eq!(
            current_value,
            SeqMarked::new_normal(2, value("resurrected"))
        );
    }

    #[tokio::test]
    async fn test_scoped_view_trait_fetch_and_set_multiple_operations() {
        let mut view = MockScopedView::new();

        // First operation on existing key
        let (old1, new1) = view
            .fetch_and_set(key("initial_key"), Some(value("version1")))
            .await
            .unwrap();
        assert_eq!(old1, SeqMarked::new_normal(1, value("initial_value")));
        assert_eq!(new1, SeqMarked::new_normal(2, value("version1")));

        // Second operation on the same key
        let (old2, new2) = view
            .fetch_and_set(key("initial_key"), Some(value("version2")))
            .await
            .unwrap();
        assert_eq!(old2, SeqMarked::new_normal(2, value("version1")));
        assert_eq!(new2, SeqMarked::new_normal(2, value("version2")));

        // Verify final state
        let current_value = view.get(key("initial_key")).await.unwrap();
        assert_eq!(current_value, SeqMarked::new_normal(2, value("version2")));
    }
}
