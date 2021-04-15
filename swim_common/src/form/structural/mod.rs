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

use crate::form::structural::read::StructuralReadable;
use crate::form::structural::write::StructuralWritable;

mod bridge;
pub mod read;
pub mod write;

/// A more flexible alternative to [`Form`] where readers and writers have full
/// visbility of the strucutures of the values that the work on. This will eventually
/// replace the [`Form`] trait.
pub trait StructuralForm: StructuralReadable + StructuralWritable {}

impl<T: StructuralReadable + StructuralWritable> StructuralForm for T {}
