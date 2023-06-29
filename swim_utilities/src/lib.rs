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

#![allow(clippy::match_wild_err_arm)]

pub mod never;

#[cfg(feature = "algebra")]
pub mod algebra {
    pub use swim_algebra::*;
}

#[cfg(feature = "errors")]
pub use swim_errors as errors;

#[cfg(feature = "future")]
pub use swim_future as future;

#[cfg(feature = "rtree")]
pub mod collections {
    pub use swim_rtree as rtree;
}

#[cfg(feature = "time")]
pub use swim_time as time;

#[cfg(feature = "multi_reader")]
pub use swim_multi_reader as multi_reader;

#[cfg(any(feature = "io", feature = "buf_channel"))]
pub mod io {

    #[cfg(feature = "io")]
    pub use swim_io::*;

    #[cfg(feature = "buf_channel")]
    pub use byte_channel;
}

#[cfg(feature = "text")]
pub use swim_route as routing;

#[cfg(feature = "text")]
pub use swim_format as format;

#[cfg(feature = "trigger")]
pub use swim_trigger as trigger;

#[cfg(feature = "sync")]
pub mod sync {
    pub use swim_sync::circular_buffer;
}

#[cfg(feature = "test-util")]
pub use swim_test_util as test_util;

#[cfg(feature = "uri_forest")]
pub use swim_uri_forest as uri_forest;

pub use swim_num::non_zero_usize;
