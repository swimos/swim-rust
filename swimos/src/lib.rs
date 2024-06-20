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

#[doc(inline)]
pub use swimos_model as model;

/// Defines the low level API for implementing Swim applications.
pub mod api {
    pub use swimos_api::agent::{Agent, DownlinkKind, UplinkKind};
    pub use swimos_utilities::handlers::NoHandler;

    /// Address types to refer to the location of agent instances.
    pub mod address {
        pub use swimos_api::address::{Address, RelativeAddress};
    }

    /// Error types produced by implementations of the implementations of the Swim API.
    pub mod error {
        pub use swimos_api::error::{
            AgentInitError, AgentRuntimeError, AgentTaskError, DownlinkFailureReason,
            DownlinkRuntimeError, DownlinkTaskError, NodeIntrospectionError, OpenStoreError,
            StoreError,
        };
    }

    /// Defines the interface for defining Swim agent implementations.
    pub mod agent {
        pub use swimos_api::agent::{
            AgentConfig, AgentContext, AgentInitResult, AgentTask, BoxAgent, DownlinkKind,
            LaneConfig, StoreKind, WarpLaneKind,
        };

        #[cfg(feature = "server")]
        pub use swimos_server_app::AgentExt;
    }
}

/// Retry strategies for processes that could fail.
pub mod retry {
    pub use swimos_utilities::future::{Quantity, RetryStrategy};
}

/// Descriptions of agent routes and patterns for matching against them.
pub mod route {
    pub use swimos_utilities::routing::RouteUri;
    pub use swimos_utilities::routing::{ApplyError, ParseError, RoutePattern, UnapplyError};
}

/// Channels and error types for communication between the SwimOS runtime and Swim agents.
pub mod io {

    /// Bidirectional byte channels used to communicate between the SwimOS runtime and Swim agents
    /// instances.
    pub mod channels {
        pub use swimos_utilities::byte_channel::{
            are_connected, byte_channel, ByteReader, ByteWriter,
        };
    }

    /// Error types that can be produced when serializing and deserializing messages within the SwimOS
    /// runtime.
    pub mod errors {
        pub use swimos_api::error::{FrameIoError, InvalidFrame};
        pub use swimos_recon::parser::{AsyncParseError, ParseError};
    }
}

/// The core Swim server application runtime.
#[cfg(feature = "server")]
pub mod server {
    pub use swimos_server_app::{
        BoxServer, DeflateConfig, IntrospectionConfig, RemoteConnectionsConfig, Server,
        ServerBuilder, ServerHandle, WindowBits,
    };

    /// Configuration for TLS support in the server.
    pub mod tls {
        pub use swimos_remote::tls::{
            CertChain, CertFormat, CertificateFile, ClientConfig, PrivateKey, ServerConfig,
            TlsConfig,
        };
    }

    /// Error types that can be produced when initializing and executing the server.
    pub mod errors {
        pub use swimos_remote::tls::TlsError;
        pub use swimos_remote::ConnectionError;
        pub use swimos_server_app::{
            AmbiguousRoutes, ServerBuilderError, ServerError, UnresolvableRoute,
        };
    }
}

#[cfg(feature = "agent")]
pub mod agent;
