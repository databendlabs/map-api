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
use std::ops::RangeBounds;

use seq_marked::InternalSeq;
use seq_marked::SeqMarked;

use crate::mvcc::Commit;
use crate::mvcc::ScopedView;
use crate::mvcc::ScopedViewReadonly;
use crate::mvcc::View;
use crate::mvcc::ViewKey;
use crate::mvcc::ViewNamespace;
use crate::mvcc::ViewReadonly;
use crate::mvcc::ViewValue;
use crate::IOResultStream;

/// A view scoped to a specific namespace for convenient operations.
///
/// Provides namespace-scoped access to an underlying `View`, eliminating
/// the need to specify the namespace parameter for each operation.
pub struct NamespaceView<'a, S, K, V, BaseView>
where
    S: ViewNamespace,
    K: ViewKey,
    V: ViewValue,
    BaseView: ViewReadonly<S, K, V> + Commit<S, K, V>,
{
    pub space: S,
    pub view: &'a mut View<S, K, V, BaseView>,
}

impl<S, K, V, BaseView> NamespaceView<'_, S, K, V, BaseView>
where
    S: ViewNamespace,
    K: ViewKey,
    V: ViewValue,
    BaseView: ViewReadonly<S, K, V> + Commit<S, K, V>,
{
    pub fn set(&mut self, key: K, value: Option<V>) -> SeqMarked<()> {
        self.view.set(self.space, key, value)
    }

    pub async fn get(&self, key: K) -> Result<SeqMarked<V>, io::Error> {
        self.view.get(self.space, key).await
    }

    pub async fn range<R>(&self, range: R) -> Result<IOResultStream<(K, SeqMarked<V>)>, io::Error>
    where R: RangeBounds<K> + Send + Sync + Clone + 'static {
        self.view.range(self.space, range).await
    }
}

#[async_trait::async_trait]
impl<S, K, V, BaseView> ScopedViewReadonly<K, V> for NamespaceView<'_, S, K, V, BaseView>
where
    S: ViewNamespace,
    K: ViewKey,
    V: ViewValue,
    BaseView: ViewReadonly<S, K, V> + Commit<S, K, V>,
{
    fn base_seq(&self) -> InternalSeq {
        self.view.base_seq()
    }

    async fn get(&self, key: K) -> Result<SeqMarked<V>, io::Error> {
        self.view.get(self.space, key).await
    }

    async fn range<R>(&self, range: R) -> Result<IOResultStream<(K, SeqMarked<V>)>, io::Error>
    where R: RangeBounds<K> + Send + Sync + Clone + 'static {
        self.view.range(self.space, range).await
    }
}

#[async_trait::async_trait]
impl<S, K, V, BaseView> ScopedView<K, V> for NamespaceView<'_, S, K, V, BaseView>
where
    S: ViewNamespace,
    K: ViewKey,
    V: ViewValue,
    BaseView: ViewReadonly<S, K, V> + Commit<S, K, V>,
{
    fn set(&mut self, key: K, value: Option<V>) -> SeqMarked<()> {
        self.view.set(self.space, key, value)
    }
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;

    use futures::StreamExt;
    use seq_marked::InternalSeq;
    use seq_marked::SeqMarked;

    use super::*;
    use crate::mvcc::table::TableViewReadonly;
    use crate::mvcc::Table;

    #[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
    enum TestSpace {
        Space1,
    }

    impl ViewNamespace for TestSpace {
        fn if_increase_seq(&self) -> bool {
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

    fn create_base_view() -> TableViewReadonly<TestSpace, TestKey, TestValue> {
        let mut table = Table::new();
        table.insert(key("base_k1"), 1, value("base_v1")).unwrap();
        table.insert(key("base_k2"), 2, value("base_v2")).unwrap();

        let mut tables = BTreeMap::new();
        tables.insert(TestSpace::Space1, table);
        let mut view = TableViewReadonly::new(tables);
        view.base_seq = InternalSeq::new(10); // Set high enough to see all base data
        view
    }

    #[tokio::test]
    async fn test_namespace_delegation() {
        let mut view = View::new(create_base_view());

        // Test delegation through namespace view
        let mut ns = view.namespace(TestSpace::Space1);
        let order_key = ns.set(key("k1"), Some(value("v1")));

        // Should return the order key
        assert_eq!(order_key, SeqMarked::new_normal(11, ()));

        // Should be equivalent to direct call
        assert_eq!(view.last_seq, InternalSeq::new(11));
        let table = &view.changes[&TestSpace::Space1];
        assert_eq!(
            table.get(key("k1"), 11),
            SeqMarked::new_normal(11, &value("v1"))
        );
    }

    #[tokio::test]
    async fn test_namespace_get_delegation() {
        let mut view = View::new(create_base_view());
        let _ = view.set(TestSpace::Space1, key("k1"), Some(value("v1")));

        let ns = view.namespace(TestSpace::Space1);
        let result = ns.get(key("base_k1")).await.unwrap();

        // Should match direct get call
        assert_eq!(result, SeqMarked::new_normal(1, value("base_v1")));
    }

    #[tokio::test]
    async fn test_namespace_get_multiple_delegation() {
        let mut view = View::new(create_base_view());
        let _ = view.set(TestSpace::Space1, key("k1"), Some(value("v1")));

        let ns = view.namespace(TestSpace::Space1);
        let result1 = ns.get(key("base_k1")).await.unwrap();
        let result2 = ns.get(key("k1")).await.unwrap();

        // Should match direct get calls
        assert_eq!(result1, SeqMarked::new_normal(1, value("base_v1")));
        assert_eq!(result2, SeqMarked::new_normal(11, value("v1")));
    }

    #[tokio::test]
    async fn test_namespace_range_delegation() {
        let mut view = View::new(create_base_view());
        let _ = view.set(TestSpace::Space1, key("c"), Some(value("vc")));

        let ns = view.namespace(TestSpace::Space1);
        let mut stream = ns.range(..).await.unwrap();
        let mut results = Vec::new();
        while let Some(result) = stream.next().await {
            results.push(result.unwrap());
        }

        // Should see both base and new data
        assert_eq!(results.len(), 3);
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
            (key("c"), SeqMarked::new_normal(11, value("vc")))
        );
    }

    #[tokio::test]
    async fn test_scoped_view_readonly_base_seq() {
        let mut view = View::new(create_base_view());
        let ns = view.namespace(TestSpace::Space1);

        // Should delegate to underlying view's base_seq
        assert_eq!(ns.base_seq(), InternalSeq::new(10));
    }

    #[tokio::test]
    async fn test_scoped_view_readonly_get() {
        let mut view = View::new(create_base_view());
        let _ = view.set(
            TestSpace::Space1,
            key("test_key"),
            Some(value("test_value")),
        );

        let ns = view.namespace(TestSpace::Space1);

        // Should get from base view
        let result1 = ns.get(key("base_k1")).await.unwrap();
        assert_eq!(result1, SeqMarked::new_normal(1, value("base_v1")));

        // Should get from changes
        let result2 = ns.get(key("test_key")).await.unwrap();
        assert_eq!(result2, SeqMarked::new_normal(11, value("test_value")));
    }

    #[tokio::test]
    async fn test_scoped_view_readonly_range() {
        let mut view = View::new(create_base_view());
        let _ = view.set(TestSpace::Space1, key("new_key"), Some(value("new_value")));

        let ns = view.namespace(TestSpace::Space1);

        let mut stream = ns.range(..).await.unwrap();
        let mut results = Vec::new();
        while let Some(result) = stream.next().await {
            results.push(result.unwrap());
        }

        assert_eq!(results.len(), 3);
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
            (
                key("new_key"),
                SeqMarked::new_normal(11, value("new_value"))
            )
        );
    }

    #[tokio::test]
    async fn test_scoped_view_set() {
        let mut view = View::new(create_base_view());
        let mut ns = view.namespace(TestSpace::Space1);

        let order_key = ns.set(key("scoped_key"), Some(value("scoped_value")));

        // Should return the order key
        assert_eq!(order_key, SeqMarked::new_normal(11, ()));

        // Should be reflected in the underlying view
        let result = view
            .get(TestSpace::Space1, key("scoped_key"))
            .await
            .unwrap();
        assert_eq!(result, SeqMarked::new_normal(11, value("scoped_value")));
    }

    #[tokio::test]
    async fn test_scoped_view_set_tombstone() {
        let mut view = View::new(create_base_view());
        let mut ns = view.namespace(TestSpace::Space1);

        let order_key = ns.set(key("tombstone_key"), None);

        // Should return the order key for tombstone
        assert_eq!(order_key, SeqMarked::new_tombstone(10));

        // Should create a tombstone in the underlying view
        let result = view
            .get(TestSpace::Space1, key("tombstone_key"))
            .await
            .unwrap();
        assert_eq!(result, SeqMarked::new_tombstone(10));
    }

    #[tokio::test]
    async fn test_scoped_view_traits_consistency() {
        let mut view = View::new(create_base_view());
        let mut ns = view.namespace(TestSpace::Space1);

        // Test that scoped view operations are equivalent to direct view operations
        let order_key = ns.set(key("consistency_key"), Some(value("consistency_value")));

        // Should return the order key
        assert_eq!(order_key, SeqMarked::new_normal(11, ()));

        let scoped_result = ns.get(key("consistency_key")).await.unwrap();
        let direct_result = view
            .get(TestSpace::Space1, key("consistency_key"))
            .await
            .unwrap();

        assert_eq!(scoped_result, direct_result);
        assert_eq!(
            scoped_result,
            SeqMarked::new_normal(11, value("consistency_value"))
        );
    }

    #[tokio::test]
    async fn test_scoped_view_fetch_and_set_nonexistent() {
        let mut view = View::new(create_base_view());
        let mut ns = view.namespace(TestSpace::Space1);

        let (old_value, new_value) = ns
            .fetch_and_set(key("new_key"), Some(value("new_value")))
            .await
            .unwrap();

        // Should return not_found for old value
        assert_eq!(old_value, SeqMarked::new_not_found());
        // Should return the new value
        assert_eq!(new_value, SeqMarked::new_normal(11, value("new_value")));

        // Verify the value was actually set in the underlying view
        let result = view.get(TestSpace::Space1, key("new_key")).await.unwrap();
        assert_eq!(result, SeqMarked::new_normal(11, value("new_value")));
    }

    #[tokio::test]
    async fn test_scoped_view_fetch_and_set_existing() {
        let mut view = View::new(create_base_view());
        let mut ns = view.namespace(TestSpace::Space1);

        let (old_value, new_value) = ns
            .fetch_and_set(key("base_k1"), Some(value("updated_value")))
            .await
            .unwrap();

        // Should return the old value from base
        assert_eq!(old_value, SeqMarked::new_normal(1, value("base_v1")));
        // Should return the new value
        assert_eq!(new_value, SeqMarked::new_normal(11, value("updated_value")));

        // Verify the value was updated in the underlying view
        let result = view.get(TestSpace::Space1, key("base_k1")).await.unwrap();
        assert_eq!(result, SeqMarked::new_normal(11, value("updated_value")));
    }

    #[tokio::test]
    async fn test_scoped_view_fetch_and_set_tombstone() {
        let mut view = View::new(create_base_view());
        let mut ns = view.namespace(TestSpace::Space1);

        let (old_value, new_value) = ns.fetch_and_set(key("base_k1"), None).await.unwrap();

        // Should return the old value from base
        assert_eq!(old_value, SeqMarked::new_normal(1, value("base_v1")));
        // Should return tombstone as new value
        assert_eq!(new_value, SeqMarked::new_tombstone(10));

        // Verify the value was deleted in the underlying view
        let result = view.get(TestSpace::Space1, key("base_k1")).await.unwrap();
        assert_eq!(result, SeqMarked::new_tombstone(10));
    }

    #[tokio::test]
    async fn test_scoped_view_fetch_and_set_with_changes() {
        let mut view = View::new(create_base_view());
        let mut ns = view.namespace(TestSpace::Space1);

        // First set a value to create changes
        ns.set(key("test_key"), Some(value("first_value")));

        let (old_value, new_value) = ns
            .fetch_and_set(key("test_key"), Some(value("second_value")))
            .await
            .unwrap();

        // Should return the old value from changes
        assert_eq!(old_value, SeqMarked::new_normal(11, value("first_value")));
        // Should return the new value
        assert_eq!(new_value, SeqMarked::new_normal(12, value("second_value")));

        // Verify the value was updated
        let result = view.get(TestSpace::Space1, key("test_key")).await.unwrap();
        assert_eq!(result, SeqMarked::new_normal(12, value("second_value")));
    }

    #[tokio::test]
    async fn test_scoped_view_fetch_and_set_sequence_consistency() {
        let mut view = View::new(create_base_view());
        let mut ns = view.namespace(TestSpace::Space1);

        // Multiple fetch_and_set operations
        let (old1, new1) = ns
            .fetch_and_set(key("k1"), Some(value("v1")))
            .await
            .unwrap();
        let (old2, new2) = ns
            .fetch_and_set(key("k2"), Some(value("v2")))
            .await
            .unwrap();

        // Verify sequence ordering
        assert_eq!(old1, SeqMarked::new_not_found());
        assert_eq!(old2, SeqMarked::new_not_found());
        assert_eq!(new1, SeqMarked::new_normal(11, value("v1")));
        assert_eq!(new2, SeqMarked::new_normal(12, value("v2")));

        // Verify the view's last_seq was updated correctly
        assert_eq!(view.last_seq, InternalSeq::new(12));
    }
}
