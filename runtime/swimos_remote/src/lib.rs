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

pub mod dns;
mod net;
pub mod plain;
mod task;
#[cfg(feature = "tls")]
pub mod tls;
mod ws;

pub use task::RemoteTask;

pub use net::{
    BadUrl, ClientConnections, ConnResult, ConnectionError, ExternalConnections, Listener,
    ListenerError, ListenerResult, Scheme, SchemeHostPort, ServerConnections,
};

pub mod websocket {

    pub use super::ws::{
        RatchetClient, RatchetError, WebsocketClient, WebsocketServer, Websockets, WsOpenFuture,
    };

    pub const WARP: &str = "warp0";
}
