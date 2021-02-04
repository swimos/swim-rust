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

use std::num::NonZeroUsize;

#[cfg(test)]
mod tests;

//Strategies for watching events from a lane.

/// Publish only the most recent event.
#[derive(Debug, PartialEq, Eq, Clone)]
pub struct Dropping;

/// Push lane events into a bounded queue.
#[derive(Debug, PartialEq, Eq, Clone)]
pub struct Queue(pub NonZeroUsize);

/// Publish the latest lane events to a bounded buffer.
#[derive(Debug, PartialEq, Eq, Clone)]
pub struct Buffered(pub NonZeroUsize);

/// The default buffer size for the [`Queue`] and [`Buffered`] strategies.
const DEFAULT_BUFFER: usize = 10;

fn default_buffer() -> NonZeroUsize {
    NonZeroUsize::new(DEFAULT_BUFFER).unwrap()
}

impl Default for Queue {
    fn default() -> Self {
        Queue(default_buffer())
    }
}

impl Default for Buffered {
    fn default() -> Self {
        Buffered(default_buffer())
    }
}
