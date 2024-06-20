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

/// Defines the low level API for implementing Swim agents.
pub mod api {
    pub use swimos_api::agent::{Agent, DownlinkKind, UplinkKind};
    pub use swimos_utilities::handlers::NoHandler;

    pub mod address {
        pub use swimos_api::address::{Address, RelativeAddress};
    }

    /// Error
    pub mod error {
        pub use swimos_api::error::{
            AgentInitError, AgentRuntimeError, AgentTaskError, DownlinkFailureReason,
            DownlinkRuntimeError, DownlinkTaskError, NodeIntrospectionError, OpenStoreError,
            StoreError,
        };
    }

    pub mod agent {
        pub use swimos_api::agent::{
            AgentConfig, AgentContext, AgentInitResult, AgentTask, BoxAgent, DownlinkKind,
            LaneConfig, StoreKind, WarpLaneKind,
        };

        #[cfg(feature = "server")]
        pub use swimos_server_app::AgentExt;
    }
}

pub mod utilities {
    pub use swimos_utilities::future::{Quantity, RetryStrategy};
}

pub mod route {
    pub use swimos_utilities::routing::RouteUri;
    pub use swimos_utilities::routing::{ApplyError, ParseError, RoutePattern, UnapplyError};
}

pub mod io {
    pub mod channels {
        pub use swimos_utilities::byte_channel::{
            are_connected, byte_channel, ByteReader, ByteWriter,
        };
    }
    pub mod errors {
        pub use swimos_api::error::{FrameIoError, InvalidFrame};
        pub use swimos_recon::parser::{AsyncParseError, ParseError};
    }
}

#[cfg(feature = "server")]
pub mod server {
    pub use swimos_server_app::{
        BoxServer, DeflateConfig, IntrospectionConfig, RemoteConnectionsConfig, Server,
        ServerBuilder, ServerHandle, WindowBits,
    };

    pub mod tls {
        pub use swimos_remote::tls::{
            CertChain, CertFormat, CertificateFile, ClientConfig, PrivateKey, ServerConfig,
            TlsConfig,
        };
    }

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
