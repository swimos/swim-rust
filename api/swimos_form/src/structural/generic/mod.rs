// Copyright 2015-2023 Swim Inc.
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

pub mod coproduct;

/// A representation of the header of a Recon record as a heterogeneous list. This is used in
/// the derive macro for [`crate::structural::write::StructuralWritable`] and should not
/// generally be used in hand written code.
#[doc(hidden)]
pub mod header;
