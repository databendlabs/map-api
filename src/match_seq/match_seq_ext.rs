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

/// Check if the sequence number satisfies the condition.
pub trait MatchSeqExt<T> {
    /// Match against a some value containing seq by checking if the seq satisfies the condition.
    fn match_seq(&self, sv: &T) -> Result<(), ConflictSeq>;
}

#[cfg(test)]
mod tests {
    use crate::match_seq::errors::ConflictSeq;
    use crate::match_seq::MatchSeq;
    use crate::match_seq::MatchSeqExt;

    type SeqV = crate::seq_value::SeqV<u64, u64>;

    #[test]
    fn test_match_seq_match_seq_value() -> anyhow::Result<()> {
        assert_eq!(MatchSeq::GE(0).match_seq(&Some(SeqV::new(0, 1))), Ok(()));
        assert_eq!(MatchSeq::GE(0).match_seq(&Some(SeqV::new(1, 1))), Ok(()));

        //

        assert_eq!(
            MatchSeq::Exact(3).match_seq(&None::<SeqV>),
            Err(ConflictSeq::NotMatch {
                want: MatchSeq::Exact(3),
                got: 0
            })
        );
        assert_eq!(
            MatchSeq::Exact(3).match_seq(&Some(SeqV::new(0, 1))),
            Err(ConflictSeq::NotMatch {
                want: MatchSeq::Exact(3),
                got: 0
            })
        );
        assert_eq!(
            MatchSeq::Exact(3).match_seq(&Some(SeqV::new(2, 1))),
            Err(ConflictSeq::NotMatch {
                want: MatchSeq::Exact(3),
                got: 2
            })
        );
        assert_eq!(MatchSeq::Exact(3).match_seq(&Some(SeqV::new(3, 1))), Ok(()));
        assert_eq!(
            MatchSeq::Exact(3).match_seq(&Some(SeqV::new(4, 1))),
            Err(ConflictSeq::NotMatch {
                want: MatchSeq::Exact(3),
                got: 4
            })
        );

        //

        assert_eq!(
            MatchSeq::GE(3).match_seq(&None::<SeqV>),
            Err(ConflictSeq::NotMatch {
                want: MatchSeq::GE(3),
                got: 0
            })
        );
        assert_eq!(
            MatchSeq::GE(3).match_seq(&Some(SeqV::new(0, 1))),
            Err(ConflictSeq::NotMatch {
                want: MatchSeq::GE(3),
                got: 0
            })
        );
        assert_eq!(
            MatchSeq::GE(3).match_seq(&Some(SeqV::new(2, 1))),
            Err(ConflictSeq::NotMatch {
                want: MatchSeq::GE(3),
                got: 2
            })
        );
        assert_eq!(MatchSeq::GE(3).match_seq(&Some(SeqV::new(3, 1))), Ok(()));
        assert_eq!(MatchSeq::GE(3).match_seq(&Some(SeqV::new(4, 1))), Ok(()));

        Ok(())
    }
}
