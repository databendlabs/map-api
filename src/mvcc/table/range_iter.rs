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

use std::cmp::Reverse;
use std::collections::btree_map::Range;

use seq_marked::SeqMarked;

pub struct RangeIter<'a, K, V>
where K: PartialEq
{
    pub(crate) inner: Range<'a, (K, Reverse<SeqMarked<()>>), Option<V>>,
    pub(crate) upto: SeqMarked<()>,
    pub(crate) last_seen_key: Option<&'a K>,
}

impl<'a, K, V> Iterator for RangeIter<'a, K, V>
where K: PartialEq
{
    type Item = (&'a K, SeqMarked<&'a V>);

    fn next(&mut self) -> Option<Self::Item> {
        loop {
            let ((k, Reverse(seq_marked)), v) = self.inner.next()?;

            // Skip older record that already yielded.
            if Some(k) == self.last_seen_key {
                continue;
            }

            // Too new, it should not see this record, skip
            if seq_marked > &self.upto {
                continue;
            }

            self.last_seen_key = Some(k);

            return Some((k, seq_marked.map(|_x| v.as_ref().unwrap())));
        }
    }
}
