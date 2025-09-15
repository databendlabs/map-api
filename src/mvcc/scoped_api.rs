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

use crate::mvcc::ScopedGet;
use crate::mvcc::ScopedRange;
use crate::mvcc::ScopedSet;
use crate::mvcc::ViewKey;
use crate::mvcc::ViewValue;

/// Combined MVCC API for namespace-scoped operations.
///
/// This trait combines read, write, and range operations within a single namespace,
/// providing a unified interface for common MVCC patterns. Implementors automatically
/// gain access to all scoped operations without namespace parameters.
///
/// # Auto-Implementation
///
/// This trait is automatically implemented for any type that provides the constituent
/// scoped operations: [`ScopedGet`], [`ScopedSet`], and [`ScopedRange`].
///
/// # Type Parameters
/// - `K`: Key type satisfying [`ViewKey`] constraints
/// - `V`: Value type satisfying [`ViewValue`] constraints
#[async_trait::async_trait]
pub trait ScopedApi<K, V>
where
    K: ViewKey,
    V: ViewValue,
    Self: ScopedGet<K, V>,
    Self: ScopedSet<K, V>,
    Self: ScopedRange<K, V>,
{
}

impl<K, V, T> ScopedApi<K, V> for T
where
    K: ViewKey,
    V: ViewValue,
    T: ScopedGet<K, V>,
    T: ScopedSet<K, V>,
    T: ScopedRange<K, V>,
{
}
