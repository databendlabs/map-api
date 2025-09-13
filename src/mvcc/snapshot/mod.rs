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

//! MVCC snapshot providing point-in-time view of data with commit functionality.

use std::collections::BTreeMap;
use std::io;
use std::marker::PhantomData;
use std::ops::RangeBounds;

use seq_marked::InternalSeq;

use crate::mvcc::seq_bounded_get::SeqBoundedGet;
use crate::mvcc::seq_bounded_range::SeqBoundedRange;
use crate::mvcc::Commit;
use crate::mvcc::Table;
use crate::mvcc::ViewKey;
use crate::mvcc::ViewNamespace;
use crate::mvcc::ViewValue;
use crate::IOResultStream;
use crate::SeqMarked;

/// MVCC snapshot with fixed sequence number for consistent point-in-time reads and commits.
#[derive(Clone, Debug, Default)]
pub struct Snapshot<S, K, V, D>
where
    S: ViewNamespace,
    K: ViewKey,
    V: ViewValue,
    D: SeqBoundedGet<S, K, V>,
    D: SeqBoundedRange<S, K, V>,
    D: Commit<S, K, V>,
    D: Send + Sync,
{
    /// The snapshot sequence number. Values with sequence numbers greater than this will be invisible.
    snapshot_seq: InternalSeq,
    data: D,
    _phantom: PhantomData<(S, K, V)>,
}

impl<S, K, V, D> Snapshot<S, K, V, D>
where
    S: ViewNamespace,
    K: ViewKey,
    V: ViewValue,
    D: SeqBoundedGet<S, K, V>,
    D: SeqBoundedRange<S, K, V>,
    D: Commit<S, K, V>,
    D: Send + Sync,
{
    pub fn new(snapshot_seq: InternalSeq, data: D) -> Self {
        Self {
            snapshot_seq,
            data,
            _phantom: PhantomData,
        }
    }

    pub fn snapshot_seq(&self) -> InternalSeq {
        self.snapshot_seq
    }

    /// Gets the value for a key upto the internal snapshot sequence(inclusive).
    pub async fn get(&self, space: S, key: K) -> Result<SeqMarked<V>, io::Error> {
        self.data.get(space, key, *self.snapshot_seq).await
    }

    /// Gets the value for a key upto the internal snapshot sequence(inclusive).
    pub async fn get_many(&self, space: S, keys: Vec<K>) -> Result<Vec<SeqMarked<V>>, io::Error> {
        self.data.get_many(space, keys, *self.snapshot_seq).await
    }

    /// Gets the value for a key upto the internal snapshot sequence(inclusive).
    pub async fn range<R>(
        &self,
        space: S,
        range: R,
    ) -> Result<IOResultStream<(K, SeqMarked<V>)>, io::Error>
    where
        R: RangeBounds<K> + Send + Sync + Clone + 'static,
    {
        self.data.range(space, range, *self.snapshot_seq).await
    }

    /// Commits changes to the underlying data store and returns the updated data handle.
    pub async fn commit(
        mut self,
        new_seq: InternalSeq,
        changes: BTreeMap<S, Table<K, V>>,
    ) -> Result<D, io::Error> {
        self.data.commit(new_seq, changes).await?;
        Ok(self.data)
    }

    pub fn data(&self) -> &D {
        &self.data
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;
    use std::sync::Mutex;

    use futures_util::StreamExt;

    use super::*;

    #[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
    struct TestSpace(u8);

    impl ViewNamespace for TestSpace {
        fn increments_seq(&self) -> bool {
            true
        }
    }

    #[derive(Debug, Clone)]
    struct MockData {
        calls: Arc<Mutex<Vec<String>>>,
    }

    impl MockData {
        fn new() -> Self {
            Self {
                calls: Arc::new(Mutex::new(Vec::new())),
            }
        }

        fn record_call(&self, call: &str) {
            self.calls.lock().unwrap().push(call.to_string());
        }

        fn get_calls(&self) -> Vec<String> {
            self.calls.lock().unwrap().clone()
        }
    }

    #[async_trait::async_trait]
    impl SeqBoundedGet<TestSpace, String, String> for MockData {
        async fn get(
            &self,
            _space: TestSpace,
            _k: String,
            seq: u64,
        ) -> Result<SeqMarked<String>, io::Error> {
            self.record_call(&format!("get(seq:{})", seq));
            Ok(SeqMarked::new_not_found())
        }

        async fn get_many(
            &self,
            _space: TestSpace,
            _keys: Vec<String>,
            seq: u64,
        ) -> Result<Vec<SeqMarked<String>>, io::Error> {
            self.record_call(&format!("get_many(seq:{})", seq));
            Ok(vec![])
        }
    }

    #[async_trait::async_trait]
    impl SeqBoundedRange<TestSpace, String, String> for MockData {
        async fn range<R>(
            &self,
            _space: TestSpace,
            _range: R,
            seq: u64,
        ) -> Result<IOResultStream<(String, SeqMarked<String>)>, io::Error>
        where
            R: RangeBounds<String> + Send + Sync + Clone + 'static,
        {
            self.record_call(&format!("range(seq:{})", seq));
            Ok(futures::stream::empty().boxed())
        }
    }

    #[async_trait::async_trait]
    impl Commit<TestSpace, String, String> for MockData {
        async fn commit(
            &mut self,
            seq: InternalSeq,
            _changes: BTreeMap<TestSpace, Table<String, String>>,
        ) -> Result<(), io::Error> {
            self.record_call(&format!("commit(seq:{})", *seq));
            Ok(())
        }
    }

    #[tokio::test]
    async fn test_snapshot_delegates_to_data() {
        let mock = MockData::new();
        let snapshot = Snapshot {
            snapshot_seq: InternalSeq::new(42),
            data: mock.clone(),
            _phantom: PhantomData,
        };

        snapshot.get(TestSpace(1), "k".to_string()).await.unwrap();
        snapshot
            .get_many(TestSpace(1), vec!["k".to_string()])
            .await
            .unwrap();
        let _stream = snapshot.range(TestSpace(1), ..).await.unwrap();

        let calls = mock.get_calls();
        assert_eq!(calls, vec![
            "get(seq:42)",
            "get_many(seq:42)",
            "range(seq:42)"
        ]);

        let mock2 = MockData::new();
        let snapshot2 = Snapshot {
            snapshot_seq: InternalSeq::new(99),
            data: mock2.clone(),
            _phantom: PhantomData,
        };

        snapshot2
            .commit(InternalSeq::new(100), BTreeMap::new())
            .await
            .unwrap();
        assert_eq!(mock2.get_calls(), vec!["commit(seq:100)"]);
    }
}
