// Copyright 2015-2021 Swim Inc.
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

use crate::{Semigroup, Zero};

/// Trait for types with an associative binary operator and a zero element. Implementors are
/// responsible for ensuring that:
/// * The operation is associatve.
/// * Applying the operator to some value `v` of the type and the zero element (in both directions)
/// results in a value that is identical to `v`.
pub trait Monoid: Zero + Semigroup {}

impl<T: Zero + Semigroup> Monoid for T {}
