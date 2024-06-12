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

//! # SwimOS API
//!
//! This crate provides the API for implementing components that can be executed by the SwimOS runtime.
//!
//! - The [`agent`] module contains the [`agent::Agent`] trait that can be implemented to add new kinds of agent
//! to the runtime. The canonical Rust implementation of this trait can be found in the `swimos_agent` crate.
//! - The [`persistence`] module contains the [`persistence::PlanePersistence`] trait that can be implemented
//! to add new storage implementations to allow a Swim server to maintain an external persistent state that can
//! outlive a single execution of the server process.

pub mod address;
pub mod agent;
pub mod error;
pub mod http;
pub mod persistence;
