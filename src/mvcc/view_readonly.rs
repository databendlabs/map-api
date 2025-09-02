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

use crate::mvcc::ViewKey;
use crate::mvcc::ViewNamespace;
use crate::mvcc::ViewValue;
use crate::IOResultStream;

/// Read-only view providing snapshot isolation at a specific sequence point.
///
/// All operations see a consistent snapshot as of `base_seq()`. Safe for concurrent access.
///
/// ⚠️ **Tombstone Anomaly**: Deletion operations may reuse sequence numbers,
/// causing inconsistent visibility at sequence boundaries.
#[async_trait::async_trait]
pub trait ViewReadonly<S, K, V>
where
    Self: Send + Sync,
    S: ViewNamespace,
    K: ViewKey,
    V: ViewValue,
{
    /// Maximum sequence number visible in this view.
    fn view_seq(&self) -> InternalSeq;

    /// Get value for key in the specified namespace.
    async fn get(&self, space: S, key: K) -> Result<SeqMarked<V>, io::Error>;

    /// Get multiple keys atomically. Defaults to sequential `get()` calls.
    async fn get_many(&self, space: S, keys: Vec<K>) -> Result<Vec<SeqMarked<V>>, io::Error> {
        let mut results = Vec::with_capacity(keys.len());
        for key in keys {
            let result = self.get(space, key).await?;
            results.push(result);
        }
        Ok(results)
    }

    /// Stream key-value pairs within the specified range in sorted order.
    async fn range<R>(
        &self,
        space: S,
        range: R,
    ) -> Result<IOResultStream<(K, SeqMarked<V>)>, io::Error>
    where
        R: RangeBounds<K> + Send + Sync + Clone + 'static;
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;

    use futures_util::stream;
    use seq_marked::SeqMarked;

    use super::*;

    #[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
    struct TestNamespace(u8);

    #[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
    struct TestKey(String);

    #[derive(Debug, Clone, PartialEq, Eq)]
    struct TestValue(String);

    impl ViewNamespace for TestNamespace {
        fn increments_seq(&self) -> bool {
            true
        }
    }

    fn ns(id: u8) -> TestNamespace {
        TestNamespace(id)
    }

    fn key(s: &str) -> TestKey {
        TestKey(s.to_string())
    }

    fn value(s: &str) -> TestValue {
        TestValue(s.to_string())
    }

    struct MockViewReadonly {
        data: BTreeMap<(TestNamespace, TestKey), SeqMarked<TestValue>>,
        seq: InternalSeq,
    }

    impl MockViewReadonly {
        fn new() -> Self {
            let mut data = BTreeMap::new();

            // Namespace 1
            data.insert((ns(1), key("k1")), SeqMarked::new_normal(1, value("v1")));
            data.insert((ns(1), key("k2")), SeqMarked::new_normal(2, value("v2")));
            data.insert((ns(1), key("k3")), SeqMarked::new_tombstone(3));
            data.insert((ns(1), key("k4")), SeqMarked::new_normal(5, value("v4")));

            // Namespace 2
            data.insert((ns(2), key("k1")), SeqMarked::new_normal(4, value("n2v1")));

            Self {
                data,
                seq: InternalSeq::new(10),
            }
        }
    }

    #[async_trait::async_trait]
    impl ViewReadonly<TestNamespace, TestKey, TestValue> for MockViewReadonly {
        fn view_seq(&self) -> InternalSeq {
            self.seq
        }

        async fn get(
            &self,
            space: TestNamespace,
            key: TestKey,
        ) -> Result<SeqMarked<TestValue>, io::Error> {
            Ok(self
                .data
                .get(&(space, key))
                .cloned()
                .unwrap_or_else(SeqMarked::new_not_found))
        }

        async fn range<R>(
            &self,
            _space: TestNamespace,
            _range: R,
        ) -> Result<IOResultStream<(TestKey, SeqMarked<TestValue>)>, io::Error>
        where
            R: RangeBounds<TestKey> + Send + Sync + Clone + 'static,
        {
            Ok(Box::pin(stream::empty()))
        }
    }

    #[tokio::test]
    async fn test_get_many_mixed() {
        let view = MockViewReadonly::new();
        let keys = vec![key("k1"), key("nx"), key("k3"), key("k4")];
        let res = view.get_many(ns(1), keys).await.unwrap();

        let expected = vec![
            SeqMarked::new_normal(1, value("v1")),
            SeqMarked::new_not_found(),
            SeqMarked::new_tombstone(3),
            SeqMarked::new_normal(5, value("v4")),
        ];
        assert_eq!(res, expected);
    }

    #[tokio::test]
    async fn test_get_many_empty() {
        let view = MockViewReadonly::new();
        let res = view.get_many(ns(1), vec![]).await.unwrap();
        assert_eq!(res, vec![]);
    }

    #[tokio::test]
    async fn test_get_many_single() {
        let view = MockViewReadonly::new();
        let keys = vec![key("k2")];
        let res = view.get_many(ns(1), keys).await.unwrap();
        let expected = vec![SeqMarked::new_normal(2, value("v2"))];
        assert_eq!(res, expected);
    }

    #[tokio::test]
    async fn test_get_many_duplicates() {
        let view = MockViewReadonly::new();
        let keys = vec![key("k1"), key("k1"), key("k2")];
        let res = view.get_many(ns(1), keys).await.unwrap();
        let expected = vec![
            SeqMarked::new_normal(1, value("v1")),
            SeqMarked::new_normal(1, value("v1")),
            SeqMarked::new_normal(2, value("v2")),
        ];
        assert_eq!(res, expected);
    }

    #[tokio::test]
    async fn test_get_many_maintains_order() {
        let view = MockViewReadonly::new();
        let keys = vec![key("k4"), key("k1"), key("k2")];
        let res = view.get_many(ns(1), keys).await.unwrap();
        let expected = vec![
            SeqMarked::new_normal(5, value("v4")),
            SeqMarked::new_normal(1, value("v1")),
            SeqMarked::new_normal(2, value("v2")),
        ];
        assert_eq!(res, expected);
    }

    #[tokio::test]
    async fn test_get_many_different_namespace() {
        let view = MockViewReadonly::new();
        let keys = vec![key("k1")];

        // Namespace 1
        let res = view.get_many(ns(1), keys.clone()).await.unwrap();
        assert_eq!(res, vec![SeqMarked::new_normal(1, value("v1"))]);

        // Namespace 2
        let res = view.get_many(ns(2), keys).await.unwrap();
        assert_eq!(res, vec![SeqMarked::new_normal(4, value("n2v1"))]);
    }

    #[tokio::test]
    async fn test_get_many_nonexistent_namespace() {
        let view = MockViewReadonly::new();
        let keys = vec![key("k1"), key("k2")];
        let res = view.get_many(ns(99), keys).await.unwrap();
        let expected = vec![SeqMarked::new_not_found(), SeqMarked::new_not_found()];
        assert_eq!(res, expected);
    }

    // Test that errors from get() are propagated
    struct ErrorView;

    #[async_trait::async_trait]
    impl ViewReadonly<TestNamespace, TestKey, TestValue> for ErrorView {
        fn view_seq(&self) -> InternalSeq {
            InternalSeq::new(0)
        }

        async fn get(
            &self,
            _space: TestNamespace,
            _key: TestKey,
        ) -> Result<SeqMarked<TestValue>, io::Error> {
            Err(io::Error::new(io::ErrorKind::Other, "test error"))
        }

        async fn range<R>(
            &self,
            _space: TestNamespace,
            _range: R,
        ) -> Result<IOResultStream<(TestKey, SeqMarked<TestValue>)>, io::Error>
        where
            R: RangeBounds<TestKey> + Send + Sync + Clone + 'static,
        {
            Ok(Box::pin(stream::empty()))
        }
    }

    #[tokio::test]
    async fn test_get_many_error_propagation() {
        let view = ErrorView;
        let keys = vec![key("k1")];
        let result = view.get_many(ns(1), keys).await;
        assert!(result.is_err());
        assert_eq!(result.unwrap_err().to_string(), "test error");
    }
}
