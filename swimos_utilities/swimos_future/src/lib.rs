// Copyright 2015-2024 Swim Inc.
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

//! Futures Combinators
//!
//! Additional combinators for [`std::future::Future`]s that express transformations that are not
//! available in the [`futures`] crate.

mod combinators;
mod retry_strategy;
mod union;

pub use combinators::{
    immediate_or_join, immediate_or_start, race, try_last, ImmediateOrJoin, ImmediateOrStart,
    NotifyOnBlocked, Race2, SecondaryResult, StopAfterError,
};
pub use retry_strategy::{ExponentialStrategy, IntervalStrategy, Quantity, RetryStrategy};
pub use union::{UnionFuture3, UnionFuture4};
