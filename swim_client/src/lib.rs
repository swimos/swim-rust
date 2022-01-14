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

#![allow(clippy::match_wild_err_arm)]

//! ## Feature flags
//!
//! - `debug`: Enables all debug features listed below.
//! - `log_verbose`: Sets the global tracing level to the highest.
//! - `websocket`: Provides a WebSocket connector that works on non-WASM platforms.

pub mod configuration;
pub mod downlink;
pub mod interface;
pub mod router;
pub use swim_async_runtime as runtime;
