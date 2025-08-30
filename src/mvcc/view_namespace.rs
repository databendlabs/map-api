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

use std::fmt;

pub trait ViewNamespace
where Self: Clone + Copy + Ord + fmt::Debug + Send + Sync + Unpin + 'static
{
    /// Return whether inserting this namespace record requires an increase the sequence number by one.
    ///
    /// Associated record, such as a secondary index of the primary index may share the same seq.
    /// For example, insert `(k1 seq=3) = "x"`, and its expiration index key is `((timestamp, seq=3), seq=3) = key`
    fn if_increase_seq(&self) -> bool;
}
