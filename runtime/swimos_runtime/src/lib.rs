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

//! SwimOS Agent & Downlink Runtime
//!
//! Tokio tasks describing the core IO loops for agents and downlinks. These tasks implement
//! the Warp protocol only and are entirely decoupled from the state and user defined behaviour
//! of the the agents/downlinks.

use swimos_utilities::byte_channel::{ByteReader, ByteWriter};

/// The agent runtime task.
pub mod agent;
mod backpressure;
/// The downlink runtime task.
pub mod downlink;
mod timeout_coord;

/// Ends of two independent channels (for example the input and output channels of an agent).
type Io = (ByteWriter, ByteReader);
