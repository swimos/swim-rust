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

/// A runtime for writing Swim applications that may be used on WebAssembly (WASM) and non-WASM platforms.
/// `swim_async_runtime` provides two components:
///
/// - `task`: a runtime for executing futures on. WASM futures are spawned on the current thread
/// and are expected to be of type `Future<Output = ()>`, `swim_async_runtime` wraps any future to the expected type
/// for WASM and still allows for a future of type `Future<Output = R>` to be executed. Non-WASM futures
/// are executed by Tokio.
///
/// - `time`: provides a wrapping of `tokio::time` on non-WASM platforms, and `wasm-timer` for WASM
/// platforms.
///
/// This functionality is important so that Swim applications can target multiple platforms and
/// the nuances of platforms are abstracted away. This allows for features such as repeating futures
/// to be supported on all platforms.
pub mod task;
pub mod time;
