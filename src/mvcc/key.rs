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

/// Trait for types that can be used as keys in MVCC operations.
///
/// # Requirements
///
/// Keys must satisfy multiple constraints to work within the MVCC system:
/// - **Ordering**: `Ord` enables range queries and consistent iteration
/// - **Cloning**: `Clone` supports versioning and snapshot operations
/// - **Threading**: `Send + Sync` allows concurrent access across threads
/// - **Debugging**: `Debug` provides troubleshooting capabilities
/// - **Async**: `Unpin` enables use in async contexts
///
/// # Automatic Implementation
///
/// This trait is automatically implemented for any type that meets the trait bounds.
/// Common key types include `String`, `u64`, `Vec<u8>`, and custom structs that derive
/// the required traits.
pub trait ViewKey
where Self: Clone + Ord + fmt::Debug + Send + Sync + Unpin + 'static
{
}

impl<K> ViewKey for K where K: Clone + Ord + fmt::Debug + Send + Sync + Unpin + 'static {}
