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

use std::collections::BTreeMap;
use std::io;

use seq_marked::InternalSeq;

use crate::mvcc::key::ViewKey;
use crate::mvcc::table::Table;
use crate::mvcc::value::ViewValue;
use crate::mvcc::view_namespace::ViewNamespace;

/// Commits staged changes to persistent storage.
#[async_trait::async_trait]
pub trait Commit<S, K, V>
where
    S: ViewNamespace,
    K: ViewKey,
    V: ViewValue,
{
    /// Apply staged changes to underlying storage.
    ///
    /// # Parameters
    /// - `last_seq`: Highest sequence number in the changes
    /// - `changes`: Pending modifications organized by namespace
    async fn commit(
        &mut self,
        last_seq: InternalSeq,
        changes: BTreeMap<S, Table<K, V>>,
    ) -> Result<(), io::Error>;
}
