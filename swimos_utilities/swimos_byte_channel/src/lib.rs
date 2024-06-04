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

//! # SPSC Byte Channel
//!
//! A single producer, single consumer  asynchronous byte channel. The [`ByteReader`] and [`ByteWriter`]
//! endpoints of the channel implement the Tokio [`tokio::io::AsyncRead`] and [`tokio::io::AsyncWrite`]
//! traits respectively.
//!
//! ## Running Cooperatively
//!
//! Currently, types outside of of the Tokio crate cannot efficiently take part in the Tokio co-operative
//! yielding mechanism. To work around this, the byte channels have an optional cooperation mechanism of
//! their own that is enabled by the `coop` feature flag.
//!
//! To use the cooperation mechanism, wrap any top level task futures with [`RunWithBudget`]. The extension
//! trait [`BudgetedFutureExt`] adds methods to all futures to allocated a budget for the task. The budget is
//! shared between all byte channels within that future.
//!
//! Whenever any of those channels makes progress it will consume a unit of budget. A channel that exhausts the
//! budget will reset the budget and immediately yield to the runtime (after rescheduling itself).

mod channel;
#[cfg(feature = "coop")]
mod coop;

pub use channel::{are_connected, byte_channel, ByteReader, ByteWriter};

#[cfg(feature = "coop")]
pub use coop::{BudgetedFutureExt, RunWithBudget};
