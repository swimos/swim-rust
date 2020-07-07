// Copyright 2015-2020 SWIM.AI inc.
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

use std::fmt::Debug;
use std::future::Future;
use std::time::Duration;
use swim_runtime::time::delay;

/// Trait for tracking the passage of time in asynchronous code. Implementations should ensure that
/// time is monotonically non-decreasing.
pub trait Clock: Debug + Clone + Send + Sync + 'static {
    /// The type of futures tracking a delay.
    type DelayFuture: Future<Output = ()> + Send + 'static;

    /// Create a future that will complete after a fixed delay.
    fn delay(&self, duration: Duration) -> Self::DelayFuture;
}

#[derive(Debug, Clone)]
struct RuntimeClock;

impl Clock for RuntimeClock {
    type DelayFuture = delay::Delay;

    fn delay(&self, duration: Duration) -> Self::DelayFuture {
        delay::delay_for(duration)
    }
}

/// A clock that uses delays as provided by the runtime.
pub fn runtime_clock() -> impl Clock {
    RuntimeClock
}
