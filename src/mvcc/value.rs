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

/// Trait for types that can be stored as values in MVCC operations.
///
/// # Requirements
///
/// Values must satisfy multiple constraints for versioned storage:
/// - **Cloning**: `Clone` enables efficient versioning and multi-reader access
/// - **Threading**: `Send + Sync` allows concurrent access across threads
/// - **Debugging**: `Debug` provides troubleshooting and logging capabilities
/// - **Async**: `Unpin` enables use in async stream operations
///
/// # Automatic Implementation
///
/// This trait is automatically implemented for any type that meets the trait bounds.
/// Common value types include `String`, primitive types, `Vec<T>`, serializable structs,
/// and enum variants that derive the required traits.
pub trait ViewValue: fmt::Debug + Clone + Send + Sync + Unpin + 'static {}

impl<V> ViewValue for V where V: fmt::Debug + Clone + Send + Sync + Unpin + 'static {}
