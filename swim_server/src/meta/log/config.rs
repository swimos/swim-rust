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

use std::num::NonZeroUsize;
use std::time::Duration;
use swim_utilities::algebra::non_zero_usize;

/// A flushing strategy for the log buffer.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum FlushStrategy {
    /// Immediately flush any new entries that are pushed into the buffer.
    Immediate,
    /// Buffer N entries into the buffer before sending them to their log lanes.
    Buffer(NonZeroUsize),
}

/// Configuration for node-level logging.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct LogConfig {
    /// How frequently to flush any pending entries in the buffer.
    pub flush_interval: Duration,
    /// The channel capacity between the node logger and the send task.
    pub channel_buffer_size: NonZeroUsize,
    /// The channel capacity for each log lane.
    pub lane_buffer: NonZeroUsize,
    /// A flushing strategy for the log buffer. Entries will either be sent immediately or buffered
    /// until the buffer is full.
    pub flush_strategy: FlushStrategy,
    /// The maximum number of pending messages that may be buffered between the send task and a
    /// log lane.
    pub max_pending_messages: NonZeroUsize,
}

impl Default for LogConfig {
    fn default() -> Self {
        LogConfig {
            flush_interval: Duration::from_secs(30),
            channel_buffer_size: non_zero_usize!(64),
            lane_buffer: non_zero_usize!(16),
            flush_strategy: FlushStrategy::Immediate,
            max_pending_messages: non_zero_usize!(16),
        }
    }
}
