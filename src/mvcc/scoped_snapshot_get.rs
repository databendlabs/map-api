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

//! Namespace-scoped read operations with snapshot isolation.

use std::io;

use seq_marked::SeqMarked;

use crate::mvcc::ViewKey;
use crate::mvcc::ViewValue;
/// Read-only view bound to a namespace with snapshot isolation.
///
/// Operations are bounded by `snapshot_seq`, ensuring only data with sequences ≤ `snapshot_seq`
/// is visible. Pre-scoped to eliminate namespace parameters.
///
/// ⚠️ **Tombstone Anomaly**: May observe different deletion states for keys with identical sequences.
#[async_trait::async_trait]
pub trait ScopedSnapshotGet<K, V>
where
    Self: Send + Sync,
    K: ViewKey,
    V: ViewValue,
{
    /// Retrieves the value for a specific key at the given snapshot sequence.
    ///
    /// Returns the most recent version with sequence ≤ `snapshot_seq`,
    /// or `SeqMarked::new_not_found()` if no such version exists.
    async fn get(&self, key: K, snapshot_seq: u64) -> Result<SeqMarked<V>, io::Error>;

    /// Retrieves multiple keys atomically at the given snapshot sequence.
    ///
    /// Results maintain the same order as input keys. Default implementation calls `get()` sequentially.
    async fn get_many(
        &self,
        keys: Vec<K>,
        snapshot_seq: u64,
    ) -> Result<Vec<SeqMarked<V>>, io::Error> {
        let mut values = Vec::with_capacity(keys.len());
        for key in keys {
            let value = self.get(key, snapshot_seq).await?;
            values.push(value);
        }
        Ok(values)
    }
}
