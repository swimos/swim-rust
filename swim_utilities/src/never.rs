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

/// Empty type for cases that can never ocurr (reading a primitive as a record for example).
/// This is essentially the same as [`core::convert::Infallible`] but is intended to be useable
/// for cases that are not only error types. It should be possible to replace it with `!` when it
/// is stabilized.
#[derive(Clone, Copy)]
pub enum Never {}

impl Never {
    /// Witnesses that an instance of [Never] cannot exist.
    pub fn explode(&self) -> ! {
        match *self {}
    }
}
