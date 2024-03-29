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

mod combinators;
pub mod retryable;
mod union;

pub use combinators::{
    immediate_or_join, immediate_or_start, race, race3, try_last, Either3, ImmediateOrJoin,
    ImmediateOrStart, NotifyOnBlocked, Race2, Race3, SecondaryResult, StopAfterError,
};
pub use union::{UnionFuture3, UnionFuture4};
