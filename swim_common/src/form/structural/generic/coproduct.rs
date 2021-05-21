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

use crate::form::structural::read::Never;

mod private {
    pub trait Sealed {}

    impl Sealed for super::CNil {}

    impl<H, T: super::Coproduct> Sealed for super::CCons<H, T> {}
}

pub trait Coproduct: private::Sealed {
    const NUM_OPTIONS: usize;
}

pub struct CNil(Never);

impl CNil {
    pub fn explode(&self) -> ! {
        self.0.explode()
    }
}

impl Coproduct for CNil {
    const NUM_OPTIONS: usize = 0;
}

pub enum CCons<H, T> {
    Head(H),
    Tail(T),
}

impl<H, T: Coproduct> Coproduct for CCons<H, T> {
    const NUM_OPTIONS: usize = T::NUM_OPTIONS + 1;
}
