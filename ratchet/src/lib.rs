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

//! A fully asynchronous implementation of [RFC6455](https://datatracker.ietf.org/doc/html/rfc6455)
//! (The WebSocket protocol). Complete with an optional implementation of
//! [RFC7692](https://datatracker.ietf.org/doc/html/rfc7692) (Compression Extensions For WebSocket).
//!
//! # Features
//! - Implement your own own extensions using [ratchet_ext](../ratchet_ext).
//! - Per-message deflate with [ratchet_deflate](../ratchet_deflate) or enable with the `deflate`
//! feature.
//! - Split WebSocket with the `split` feature.

#![deny(
    missing_docs,
    missing_copy_implementations,
    missing_debug_implementations,
    trivial_numeric_casts,
    unstable_features,
    unused_must_use,
    unused_mut,
    unused_imports,
    unused_import_braces
)]

pub use ratchet_core::{self, *};
pub use ratchet_ext::{self, *};

/// Per-message deflate.
#[cfg(feature = "deflate")]
pub mod deflate {
    pub use ratchet_deflate::{self, *};
}

#[allow(missing_docs)]
#[cfg(feature = "fixture")]
pub mod fixture {
    pub use ratchet_core::fixture::{self, *};
}
