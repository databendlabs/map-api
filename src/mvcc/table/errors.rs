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

use seq_marked::SeqMarked;

/// Errors that can occur during table insert operations.
#[derive(Clone, PartialEq, Eq, thiserror::Error, Debug)]
pub enum InsertError {
    /// Sequence number is not monotonically increasing.
    ///
    /// All normal inserts must have strictly increasing sequences.
    /// Tombstones may reuse the current sequence.
    #[error("NonIncremental: current={current:?} >= last={last:?}(equal only allowed for tombstone) does not hold")]
    NonIncremental {
        last: SeqMarked<()>,
        current: SeqMarked<()>,
    },
}
