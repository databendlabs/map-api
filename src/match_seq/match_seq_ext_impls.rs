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

use crate::match_seq::errors::ConflictSeq;
use crate::match_seq::MatchSeq;
use crate::match_seq::MatchSeqExt;
use crate::seq_value::SeqV;

impl MatchSeqExt<u64> for MatchSeq {
    fn match_seq(&self, seq: &u64) -> Result<(), ConflictSeq> {
        match self {
            MatchSeq::Any => Ok(()),
            MatchSeq::Exact(s) if seq == s => Ok(()),
            MatchSeq::GE(s) if seq >= s => Ok(()),
            _ => Err(ConflictSeq::NotMatch {
                want: *self,
                got: *seq,
            }),
        }
    }
}

impl<M, T> MatchSeqExt<SeqV<M, T>> for MatchSeq {
    fn match_seq(&self, sv: &SeqV<M, T>) -> Result<(), ConflictSeq> {
        self.match_seq(&sv.seq)
    }
}

impl<M, T> MatchSeqExt<Option<&SeqV<M, T>>> for MatchSeq {
    fn match_seq(&self, sv: &Option<&SeqV<M, T>>) -> Result<(), ConflictSeq> {
        let seq = sv.map_or(0, |sv| sv.seq);
        self.match_seq(&seq)
    }
}

impl<M, T> MatchSeqExt<Option<SeqV<M, T>>> for MatchSeq {
    fn match_seq(&self, sv: &Option<SeqV<M, T>>) -> Result<(), ConflictSeq> {
        self.match_seq(&sv.as_ref())
    }
}
