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

#![allow(clippy::match_wild_err_arm)]

pub mod never;
pub mod ptr;

#[cfg(feature = "errors")]
pub use swim_errors as errors;

#[cfg(feature = "future")]
pub use swim_future as future;

#[cfg(feature = "iteratee")]
pub use swim_iteratee as iteratee;

#[cfg(any(feature = "lrucache", feature = "rtree"))]
pub mod collections {
    #[cfg(feature = "lrucache")]
    pub use swim_lrucache as lrucache;
    #[cfg(feature = "rtree")]
    pub use swim_rtree as rtree;
}

#[cfg(feature = "time")]
pub use swim_time as time;

#[cfg(feature = "text")]
pub use swim_route as routing;

#[cfg(feature = "text")]
pub use swim_format as format;

#[cfg(feature = "trigger")]
pub use swim_trigger as trigger;

#[cfg(any(feature = "bilock", feature = "sync"))]
pub mod sync {
    #[cfg(feature = "bilock")]
    pub use swim_bilock as bilock;

    #[cfg(feature = "sync")]
    pub use swim_sync::circular_buffer;
    #[cfg(feature = "sync")]
    pub use swim_sync::rwlock;
    #[cfg(feature = "sync")]
    pub use swim_sync::topic;
}

#[cfg(feature = "test-util")]
pub use swim_test_util as test_util;