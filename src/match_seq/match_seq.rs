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

use serde::Deserialize;
use serde::Serialize;

/// Specifies the sequence number condition that an operation must satisfy to take effect.
///
/// In distributed systems, each value stored in the system has an associated sequence number
/// (`seq`) that represents its version. `MatchSeq` provides a way to express conditional
/// operations based on these sequence numbers:
///
/// - Match any sequence number (unconditional operation)
/// - Match an exact sequence number (compare-and-swap operations)
/// - Match sequence numbers greater than or equal to a value (update existing entries)
///
/// This is essential for implementing optimistic concurrency control and ensuring
/// consistency in distributed environments.
#[derive(Serialize, Deserialize, Debug, Copy, Clone, PartialEq, Eq, deepsize::DeepSizeOf)]
pub enum MatchSeq {
    // TODO(xp): remove Any, it is equivalent to GE(0)
    /// Any value is acceptable, i.e. does not check seq at all.
    Any,

    /// To match an exact value of seq.
    ///
    /// E.g., CAS updates the exact version of some value,
    /// and put-if-absent adds a value only when seq is 0.
    Exact(u64),

    /// To match a seq that is greater-or-equal some value.
    ///
    /// E.g., GE(1) perform an update on any existent value.
    GE(u64),
}

impl fmt::Display for MatchSeq {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self {
            MatchSeq::Any => {
                write!(f, "is any value")
            }
            MatchSeq::Exact(s) => {
                write!(f, "== {}", s)
            }
            MatchSeq::GE(s) => {
                write!(f, ">= {}", s)
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use crate::match_seq::MatchSeq;

    #[derive(serde::Serialize)]
    struct Foo {
        f: MatchSeq,
    }

    #[test]
    fn test_match_seq_serde() -> anyhow::Result<()> {
        //

        let t = Foo { f: MatchSeq::Any };
        let s = serde_json::to_string(&t)?;
        println!("{s}");

        Ok(())
    }

    #[test]
    fn test_match_seq_display() -> anyhow::Result<()> {
        assert_eq!("== 3", MatchSeq::Exact(3).to_string());
        assert_eq!(">= 3", MatchSeq::GE(3).to_string());

        Ok(())
    }
}
