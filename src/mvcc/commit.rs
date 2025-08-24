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
use crate::mvcc::key_space::ViewNameSpace;
use crate::mvcc::table::Table;
use crate::mvcc::value::ViewValue;

/// A trait for committing a view with staged changes.
#[async_trait::async_trait]
pub trait Commit<S, K, V>
where
    S: ViewNameSpace,
    K: ViewKey,
    V: ViewValue,
{
    /// Commit the staged changes in this view.
    ///
    /// Flushes pending changes to the underlying storage.
    ///
    /// Note: Whether changes are persisted depends on the implementation.
    async fn commit(
        &mut self,
        last_seq: InternalSeq,
        changes: BTreeMap<S, Table<K, V>>,
    ) -> Result<(), io::Error>;
}
