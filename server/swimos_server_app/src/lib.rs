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

//! # SwimOS Server Application
//!
//! This module contains the main runtime loop for the SwimOS server. A SwimOS server contains a web server
//! that can accept incoming websocket connections, using the Warp protocol, to communicate with agents
//! that run within the server. It will additionally accept plain HTTP connections that are targetted at
//! HTTP lanes exposed by the agents.
//!
//! A server instance is created by using the [`ServerBuilder`] to configure how the server should behave. By
//! default, a server will not host any agent routes and these must be registered by calling
//! [`ServerBuilder::add_route`]. This crate is not sufficient, in itself, to produce a complete SwimOS application
//!  as it does not supply any implementation of the [agent interface](swimos_api::agent::Agent).
//!
//! A Rust implementation of agents is provided by the `swimos_agent` crate.
//!
//! # Examples
//!
//! ```no_run
//! use std::error::Error;
//! use swimos_server_app::{Server, ServerBuilder};
//!
//! # async fn example(ctrl_c: impl std::future::Future<Output = ()>) -> Result<(), Box<dyn Error>> {
//! let server = ServerBuilder::with_plane_name("Example Server")
//!     .set_bind_addr("127.0.0.1:8080".parse()?)
//!     .enable_introspection()
//!     .with_in_memory_store()
//!     // Register agent routes.
//!     .build()
//!     .await?;
//!
//! let (task, mut handle) = server.run();
//! let stop = async move {
//!     ctrl_c.await; // Provide a signal to cause the server to stop. E.g. Ctrl-C.
//!     handle.stop();
//! };
//! tokio::join!(stop, task).1?;
//! # Ok(())
//! # }
//! ```
//!

mod config;
mod error;
mod in_memory_store;
mod plane;
mod server;
mod util;

pub use self::{
    config::{RemoteConnectionsConfig, SwimServerConfig},
    server::{BoxServer, Server, ServerBuilder, ServerHandle},
    util::AgentExt,
};

pub use error::{AmbiguousRoutes, ServerBuilderError, ServerError};
pub use ratchet::deflate::DeflateConfig;
pub use swimos_introspection::IntrospectionConfig;
use swimos_utilities::byte_channel::{ByteReader, ByteWriter};

type Io = (ByteWriter, ByteReader);
