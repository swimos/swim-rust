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

pub mod handlers;
pub mod never;

#[cfg(feature = "algebra")]
pub mod algebra {
    pub use swimos_algebra::*;
}

#[cfg(feature = "errors")]
pub use swimos_errors as errors;

#[cfg(feature = "future")]
pub use swimos_future as future;

#[cfg(feature = "rtree")]
pub mod collections {
    pub use swimos_rtree as rtree;
}

#[cfg(feature = "time")]
pub use swimos_time as time;

#[cfg(feature = "multi_reader")]
pub use swimos_multi_reader as multi_reader;

#[cfg(any(feature = "io", feature = "buf_channel"))]
pub mod io {

    #[cfg(feature = "io")]
    pub use swimos_io::*;

    #[cfg(feature = "buf_channel")]
    pub use byte_channel;
}

#[cfg(feature = "text")]
pub use swimos_route as routing;

#[cfg(feature = "text")]
pub use swimos_format as format;

#[cfg(feature = "trigger")]
pub use swimos_trigger as trigger;

#[cfg(feature = "sync")]
pub mod sync {
    pub use swimos_sync::circular_buffer;
}

#[cfg(feature = "test-util")]
pub use swimos_test_util as test_util;

#[cfg(feature = "uri_forest")]
pub use swimos_uri_forest as uri_forest;

pub use swimos_num::non_zero_usize;

#[cfg(feature = "encoding")]
pub use swimos_encoding as encoding;
