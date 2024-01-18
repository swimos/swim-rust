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

#[cfg(feature = "futures")]
pub use retryable::*;

#[cfg(feature = "futures")]
mod retryable {
    pub const IMMEDIATE_TAG: &str = "immediate";
    pub const INTERVAL_TAG: &str = "interval";
    pub const EXPONENTIAL_TAG: &str = "exponential";
    pub const RETRY_IMMEDIATE_TAG: &str = "immediate";
    pub const RETRY_INTERVAL_TAG: &str = "interval";
    pub const RETRY_EXPONENTIAL_TAG: &str = "exponential";
    pub const RETRY_NONE_TAG: &str = "none";
    pub const DELAY_TAG: &str = "delay";
    pub const RETRIES_TAG: &str = "retries";
    pub const MAX_INTERVAL_TAG: &str = "max_interval";
    pub const MAX_BACKOFF_TAG: &str = "max_backoff";
    pub const INFINITE_TAG: &str = "infinite";
}

pub const DURATION_TAG: &str = "duration";
pub const SECS_TAG: &str = "secs";
pub const NANOS_TAG: &str = "nanos";
