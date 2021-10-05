// Copyright 2015-2021 SWIM.AI inc.
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

use swim_utilities::never::Never;

mod private {
    pub trait Sealed {}

    impl Sealed for super::CNil {}

    impl<H, T: super::Coproduct> Sealed for super::CCons<H, T> {}
}

/// Trait for the co-product of a number of types. Coproducts are used by the derivation macro
/// for [`crate::form::structural::read::StructuralReadable`] to avoid needing to generate
/// enumeration types.
pub trait Coproduct: private::Sealed {
    /// The number of types in the co-product.
    const NUM_OPTIONS: usize;
}

/// The empty coproduct (this type has no members).
pub struct CNil(Never);

impl CNil {
    /// Witnesses that this type has no members.
    pub fn explode(&self) -> ! {
        self.0.explode()
    }
}

impl Coproduct for CNil {
    const NUM_OPTIONS: usize = 0;
}

/// A non-empty coproduct. In general, we should have `T: Coproduct`, however, this makes it
/// impossible to implement traits for general co-products due the the potentially unbounded
/// recursion.
pub enum CCons<H, T> {
    Head(H),
    Tail(T),
}

impl<H, T: Coproduct> Coproduct for CCons<H, T> {
    const NUM_OPTIONS: usize = T::NUM_OPTIONS + 1;
}

/// At trait that is implemented for all coproducts where the types are identical allowing them to
/// be unified to a simple value of that type.
pub trait Unify {
    type Out;
    fn unify(self) -> Self::Out;
}

impl<T> Unify for CCons<T, CNil> {
    type Out = T;

    fn unify(self) -> Self::Out {
        match self {
            CCons::Head(h) => h,
            CCons::Tail(t) => t.explode(),
        }
    }
}

impl<T: Unify> Unify for CCons<T::Out, T> {
    type Out = T::Out;

    fn unify(self) -> Self::Out {
        match self {
            CCons::Head(h) => h,
            CCons::Tail(t) => t.unify(),
        }
    }
}
