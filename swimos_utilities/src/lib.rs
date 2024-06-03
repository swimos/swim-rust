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

//! # SwimOS Utilities
//!
//! Collects together a number of utility crates that can be individually enabled by features.

#![allow(clippy::match_wild_err_arm)]

/// Wrappers around closure types to server as event handlers.
pub mod handlers;

/// Provides a canonical uninhabited type [`never::Never`].
pub mod never;

#[cfg(feature = "algebra")]
#[doc(inline)]
pub use swimos_algebra as algebra;

#[cfg(feature = "errors")]
#[doc(inline)]
pub use swimos_errors as errors;

#[cfg(feature = "future")]
#[doc(inline)]
pub use swimos_future as future;

#[cfg(feature = "rtree")]
pub mod collections {
    #[doc(inline)]
    pub use swimos_rtree as rtree;
}

#[cfg(feature = "time")]
#[doc(inline)]
pub use swimos_time as time;

#[cfg(feature = "multi_reader")]
#[doc(inline)]
pub use swimos_multi_reader as multi_reader;

#[cfg(any(feature = "io", feature = "buf_channel"))]
pub mod io {

    #[cfg(feature = "io")]
    #[doc(inline)]
    pub use swimos_fs as fs;

    #[cfg(feature = "buf_channel")]
    #[doc(inline)]
    pub use swimos_byte_channel as byte_channel;
}

#[cfg(feature = "text")]
#[doc(inline)]
pub use swimos_route as routing;

#[cfg(feature = "text")]
#[doc(inline)]
pub use swimos_format as format;

#[cfg(feature = "trigger")]
#[doc(inline)]
pub use swimos_trigger as trigger;

#[cfg(feature = "sync")]
pub mod sync {
    #[doc(inline)]
    pub use swimos_sync as circular_buffer;
}

#[cfg(feature = "test-util")]
#[doc(inline)]
pub use swimos_test_util as test_util;

#[cfg(feature = "uri_forest")]
#[doc(inline)]
pub use swimos_uri_forest as uri_forest;

#[doc(inline)]
pub use swimos_num::non_zero_usize;

#[cfg(feature = "encoding")]
#[doc(inline)]
pub use swimos_encoding as encoding;
