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
use std::ops::Deref;
use std::ops::RangeBounds;
use std::sync::Arc;

use crate::impls::level::Level;
use crate::KVResultStream;
use crate::MapApiRO;
use crate::SeqMarkedOf;
use crate::ValueOf;

/// A single **immutable** level data.
///
/// Only used for testing.
#[allow(dead_code)]
#[derive(Debug, Clone)]
pub struct Immutable<V = Vec<u8>> {
    /// An in-process unique to identify this immutable level.
    ///
    /// It is used to assert an immutable level is not replaced after compaction.
    level: Arc<Level<V>>,
}

impl<M> Immutable<M> {
    #[allow(dead_code)]
    fn new(level: Arc<Level<M>>) -> Self {
        Self { level }
    }

    #[allow(dead_code)]
    pub(crate) fn new_from_level(level: Level<M>) -> Self {
        Self::new(Arc::new(level))
    }
}

impl<M> AsRef<Level<M>> for Immutable<M> {
    fn as_ref(&self) -> &Level<M> {
        self.level.as_ref()
    }
}

impl<M> Deref for Immutable<M> {
    type Target = Level<M>;

    fn deref(&self) -> &Self::Target {
        self.level.as_ref()
    }
}

#[async_trait::async_trait]
impl MapApiRO<String> for Immutable<ValueOf<String>> {
    async fn get(&self, key: &String) -> Result<SeqMarkedOf<String>, io::Error> {
        self.level.get(key).await
    }

    async fn range<R>(&self, range: R) -> Result<KVResultStream<String>, io::Error>
    where R: RangeBounds<String> + Clone + Send + Sync + 'static {
        let strm = self.level.range(range).await?;
        Ok(strm)
    }
}

#[cfg(test)]
mod tests {
    use futures_util::TryStreamExt;

    use super::*;
    use crate::impls::level::Level;
    use crate::MapApi;
    use crate::SeqMarked;

    #[test]
    fn test_immutable_new() {
        let level = Level::<Vec<u8>>::default();
        let immutable = Immutable::new(Arc::new(level));

        // Test that we can access the inner level
        assert_eq!(Arc::strong_count(&immutable.level), 1);
    }

    #[test]
    fn test_immutable_new_from_level() {
        let level = Level::<Vec<u8>>::default();
        let immutable = Immutable::new_from_level(level);

        // Test that the immutable was created successfully
        assert_eq!(Arc::strong_count(&immutable.level), 1);
    }

    #[test]
    fn test_immutable_as_ref() {
        let level = Level::<Vec<u8>>::default();
        let immutable = Immutable::new_from_level(level);

        // Test AsRef trait
        let level_ref: &Level<Vec<u8>> = immutable.as_ref();
        let _: &Level<Vec<u8>> = level_ref; // Type assertion
    }

    #[test]
    fn test_immutable_deref() {
        let level = Level::<Vec<u8>>::default();
        let immutable = Immutable::new_from_level(level);

        // Test Deref trait - we should be able to access Level methods directly
        let _level_direct: &Level<Vec<u8>> = &immutable;
    }

    #[tokio::test]
    async fn test_immutable_map_api_ro_get() {
        let mut level = Level::default();
        level
            .set(
                "test_key".to_string(),
                Some("test_value".as_bytes().to_vec()),
            )
            .await
            .unwrap();
        let immutable = Immutable::new_from_level(level);

        // Test MapApiRO implementation
        let result = immutable.get(&"test_key".to_string()).await.unwrap();
        assert!(result.is_normal());
        assert_eq!(
            result,
            SeqMarked::new_normal(1, "test_value".as_bytes().to_vec())
        );
    }

    #[tokio::test]
    async fn test_immutable_map_api_ro_get_nonexistent() {
        let level = Level::default();
        let immutable = Immutable::new_from_level(level);

        let result = immutable.get(&"nonexistent".to_string()).await.unwrap();
        assert!(result.is_not_found());
    }

    #[tokio::test]
    async fn test_immutable_map_api_ro_range() {
        let mut level = Level::default();
        level
            .set("key1".to_string(), Some("value1".as_bytes().to_vec()))
            .await
            .unwrap();
        level
            .set("key2".to_string(), Some("value2".as_bytes().to_vec()))
            .await
            .unwrap();
        let immutable = Immutable::new_from_level(level);

        // Test range operation
        let range_result = immutable
            .range("key1".to_string().."key3".to_string())
            .await
            .unwrap();
        let items: Vec<_> = range_result.try_collect().await.unwrap();

        assert_eq!(items.len(), 2);
        assert_eq!(items[0].0, "key1");
        assert_eq!(items[1].0, "key2");
    }

    #[tokio::test]
    async fn test_immutable_map_api_ro_empty_range() {
        let level = Level::default();
        let immutable = Immutable::new_from_level(level);

        let range_result = immutable
            .range("a".to_string().."z".to_string())
            .await
            .unwrap();
        let items: Vec<_> = range_result.try_collect().await.unwrap();

        assert!(items.is_empty());
    }

    #[test]
    fn test_immutable_clone() {
        let level = Level::<Vec<u8>>::default();
        let immutable1 = Immutable::new_from_level(level);
        let immutable2 = immutable1.clone();

        // Test that both point to the same Arc
        assert!(Arc::ptr_eq(&immutable1.level, &immutable2.level));
        assert_eq!(Arc::strong_count(&immutable1.level), 2);
    }
}
