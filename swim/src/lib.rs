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

#[cfg(feature = "form")]
pub use swim_form as form;
pub use swim_model as model;

pub mod api {
    pub use swim_api::agent::{Agent, UplinkKind};
    pub use swim_api::downlink::{Downlink, DownlinkKind};

    pub mod agent {
        pub use swim_api::agent::{
            AgentConfig, AgentContext, AgentInitResult, AgentTask, BoxAgent, LaneConfig,
        };

        #[cfg(feature = "server")]
        pub use swim_server_app::AgentExt;

        pub mod error {
            pub use swim_api::error::{AgentInitError, AgentRuntimeError, AgentTaskError};
        }
    }

    pub mod downlink {
        pub use swim_api::downlink::DownlinkConfig;

        pub mod error {
            pub use swim_api::error::DownlinkTaskError;
        }
    }
}

pub mod route {
    pub use swim_utilities::routing::uri::{BadRelativeUri, RelativeUri};
    pub use swim_utilities::routing::route_pattern::{RoutePattern, ApplyError, UnapplyError, ParseError};
}

pub mod io {
    pub mod channels {
        pub use swim_utilities::io::byte_channel::{
            are_connected, byte_channel, ByteReader, ByteWriter,
        };
    }
    pub mod errors {
        pub use swim_api::error::{FrameIoError, InvalidFrame};
        pub use swim_recon::parser::AsyncParseError;
    }
}

#[cfg(feature = "server")]
pub mod server {
    pub use swim_server_app::{RemoteConnectionsConfig, Server, ServerBuilder, ServerHandle};
}

#[cfg(feature = "agent")]
pub use swim_agent as agent;
