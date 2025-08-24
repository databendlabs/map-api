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
use crate::mvcc::ViewValue;
use crate::IOResultStream;

/// A read-only view bound to a specific namespace. It is used to read data
/// from a namespace without modifying it.
///
/// This trait provides access to data within a namespace that is embedded
/// in the view implementation, eliminating the need to specify namespace
/// parameters for each operation.
#[async_trait::async_trait]
pub trait ScopedViewReadonly<K, V>
where
    K: ViewKey,
    V: ViewValue,
{
    /// Return the last seq(inclusive) this view can see.
    fn base_seq(&self) -> InternalSeq;

    async fn get(&self, key: K) -> Result<SeqMarked<V>, io::Error>;

    async fn range<R>(&self, range: R) -> Result<IOResultStream<(K, SeqMarked<V>)>, io::Error>
    where R: RangeBounds<K> + Send + Sync + Clone + 'static;
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
            data.insert(key("key1"), SeqMarked::new_normal(1, value("value1")));
            data.insert(key("key2"), SeqMarked::new_normal(2, value("value2")));
            data.insert(key("key3"), SeqMarked::new_tombstone(3));

            Self {
                base_seq: InternalSeq::new(10),
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

    #[tokio::test]
    async fn test_scoped_view_readonly_trait_base_seq() {
        let view = MockScopedView::new();
        assert_eq!(view.base_seq(), InternalSeq::new(10));
    }

    #[tokio::test]
    async fn test_scoped_view_readonly_trait_get_existing() {
        let view = MockScopedView::new();
        let result = view.get(key("key1")).await.unwrap();
        assert_eq!(result, SeqMarked::new_normal(1, value("value1")));
    }

    #[tokio::test]
    async fn test_scoped_view_readonly_trait_get_tombstone() {
        let view = MockScopedView::new();
        let result = view.get(key("key3")).await.unwrap();
        assert_eq!(result, SeqMarked::new_tombstone(3));
    }

    #[tokio::test]
    async fn test_scoped_view_readonly_trait_get_not_found() {
        let view = MockScopedView::new();
        let result = view.get(key("nonexistent")).await.unwrap();
        assert_eq!(result, SeqMarked::new_not_found());
    }

    #[tokio::test]
    async fn test_scoped_view_readonly_trait_range() {
        let view = MockScopedView::new();
        let mut stream = view.range(..).await.unwrap();
        let mut results = Vec::new();
        while let Some(result) = stream.next().await {
            results.push(result.unwrap());
        }

        assert_eq!(results.len(), 3);
        assert_eq!(
            results[0],
            (key("key1"), SeqMarked::new_normal(1, value("value1")))
        );
        assert_eq!(
            results[1],
            (key("key2"), SeqMarked::new_normal(2, value("value2")))
        );
        assert_eq!(results[2], (key("key3"), SeqMarked::new_tombstone(3)));
    }

    #[tokio::test]
    async fn test_scoped_view_readonly_trait_range_bounded() {
        let view = MockScopedView::new();
        let mut stream = view.range(key("key1")..=key("key2")).await.unwrap();
        let mut results = Vec::new();
        while let Some(result) = stream.next().await {
            results.push(result.unwrap());
        }

        assert_eq!(results.len(), 2);
        assert_eq!(
            results[0],
            (key("key1"), SeqMarked::new_normal(1, value("value1")))
        );
        assert_eq!(
            results[1],
            (key("key2"), SeqMarked::new_normal(2, value("value2")))
        );
    }
}
