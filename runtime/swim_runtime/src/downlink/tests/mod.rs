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

use std::{num::NonZeroUsize, time::Duration};

use swim_utilities::algebra::non_zero_usize;

use crate::routing::RoutingAddr;

mod map;
mod value;

const BUFFER_SIZE: NonZeroUsize = non_zero_usize!(1024);
const CHANNEL_SIZE: usize = 16;
const TEST_TIMEOUT: Duration = Duration::from_secs(10);
const REMOTE_ADDR: RoutingAddr = RoutingAddr::remote(1);
const REMOTE_NODE: &str = "/remote";
const REMOTE_LANE: &str = "remote_lane";
const EMPTY_TIMEOUT: Duration = Duration::from_secs(2);
const ATT_QUEUE_SIZE: NonZeroUsize = non_zero_usize!(8);

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum State {
    Unlinked,
    Linked,
    Synced,
}
