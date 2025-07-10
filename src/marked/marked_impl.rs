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

//! Implement the conversion between `Marked` and other types

use std::io;

use crate::marked::SeqMarked;

impl<M> TryFrom<SeqMarked<M>> for SeqMarked<M, String> {
    type Error = io::Error;

    /// Convert `Marked<Vec<u8>>` to `Marked<String>`
    fn try_from(marked: SeqMarked<M>) -> Result<Self, Self::Error> {
        // convert Vec<u8> to String
        match marked {
            SeqMarked::TombStone { internal_seq } => Ok(SeqMarked::TombStone { internal_seq }),
            SeqMarked::Normal {
                internal_seq,
                value,
                meta,
            } => {
                let s = String::from_utf8(value).map_err(|e| {
                    io::Error::new(
                        io::ErrorKind::InvalidData,
                        format!("fail to convert Vec<u8> to String: {}", e),
                    )
                })?;
                Ok(SeqMarked::Normal {
                    internal_seq,
                    value: s,
                    meta,
                })
            }
        }
    }
}

impl<M> From<SeqMarked<M, String>> for SeqMarked<M> {
    /// Convert `Marked<String>` to `Marked<Vec<u8>>`
    fn from(value: SeqMarked<M, String>) -> Self {
        match value {
            SeqMarked::TombStone { internal_seq } => SeqMarked::TombStone { internal_seq },
            SeqMarked::Normal {
                internal_seq,
                value,
                meta,
            } => {
                let v = value.into_bytes();
                SeqMarked::Normal {
                    internal_seq,
                    value: v,
                    meta,
                }
            }
        }
    }
}
