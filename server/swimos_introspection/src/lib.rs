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

//! # Introspection Support
//!
//! Adds support for introspection to a Swim server.
//!
//! - The [`register_introspection`] will add special meta-agents to export information about running agents.
//! - The [`IntrospectionResolver`] type is used by the server to register normal agents for introspection.

mod config;
mod forest;
mod meta_agent;
mod meta_mesh;
mod model;
mod route;
mod task;

pub use config::IntrospectionConfig;
pub use route::{lane_pattern, mesh_pattern, node_pattern};
pub use task::{register_introspection, AgentRegistration, IntrospectionResolver};
