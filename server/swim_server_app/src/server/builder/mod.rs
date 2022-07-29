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

use std::{
    net::{Ipv4Addr, SocketAddr, SocketAddrV4},
    path::PathBuf,
    sync::Arc,
};

use ratchet::{
    deflate::{DeflateConfig, DeflateExtProvider},
    NoExtProvider, ProtocolRegistry, WebSocketStream,
};
use swim_api::agent::Agent;
use swim_runtime::{
    remote::{
        net::{dns::Resolver, plain::TokioPlainTextNetworking, tls::TokioTlsNetworking},
        ExternalConnections,
    },
    ws::ext::RatchetNetworking,
};
use swim_utilities::routing::route_pattern::RoutePattern;

use crate::{
    config::SwimServerConfig,
    error::AmbiguousRoutes,
    plane::{PlaneBuilder, PlaneModel},
};

use super::{runtime::SwimServer, Server};

pub struct ServerBuilder {
    bind_to: SocketAddr,
    plane: PlaneBuilder,
    enable_tls: bool,
    deflate: Option<DeflateConfig>,
    config: SwimServerConfig,
}

const DEFAULT_PORT: u16 = 8080;

impl Default for ServerBuilder {
    fn default() -> Self {
        Self {
            bind_to: SocketAddr::V4(SocketAddrV4::new(Ipv4Addr::new(0, 0, 0, 0), DEFAULT_PORT)),
            plane: Default::default(),
            enable_tls: false,
            deflate: Default::default(),
            config: Default::default(),
        }
    }
}

impl ServerBuilder {
    pub fn set_bind_addr(mut self, addr: SocketAddr) -> Self {
        self.bind_to = addr;
        self
    }

    pub fn add_route<A: Agent + Send + 'static>(mut self, pattern: RoutePattern, agent: A) -> Self {
        self.plane.add_route(pattern, agent);
        self
    }

    pub fn add_tls_support(mut self) -> Self {
        self.enable_tls = true;
        self
    }

    pub fn add_deflate_support(self) -> Self {
        self.configure_deflate_support(Default::default())
    }

    pub fn configure_deflate_support(mut self, config: DeflateConfig) -> Self {
        self.deflate = Some(config);
        self
    }

    pub fn update_config<F>(mut self, f: F) -> Self
    where
        F: FnOnce(&mut SwimServerConfig),
    {
        f(&mut self.config);
        self
    }

    pub async fn build(self) -> Result<Box<dyn Server>, AmbiguousRoutes> {
        let ServerBuilder {
            bind_to,
            plane,
            enable_tls,
            deflate,
            config,
        } = self;
        let routes = plane.build()?;
        let resolver = Arc::new(Resolver::new().await);
        if enable_tls {
            let networking = TokioPlainTextNetworking::new(resolver);
            Ok(with_websockets(
                bind_to, routes, networking, config, deflate,
            ))
        } else {
            //TODO Make this support actual identities.
            let networking =
                TokioTlsNetworking::new::<_, Box<PathBuf>>(std::iter::empty(), resolver);
            Ok(with_websockets(
                bind_to, routes, networking, config, deflate,
            ))
        }
    }
}

fn with_websockets<N>(
    bind_to: SocketAddr,
    routes: PlaneModel,
    networking: N,
    config: SwimServerConfig,
    deflate: Option<DeflateConfig>,
) -> Box<dyn Server>
where
    N: ExternalConnections,
    N::Socket: WebSocketStream,
{
    let subprotocols = ProtocolRegistry::new(vec!["warp0"]).unwrap();
    if let Some(deflate_config) = deflate {
        let websockets = RatchetNetworking {
            config: config.websockets,
            provider: DeflateExtProvider::with_config(deflate_config),
            subprotocols,
        };
        Box::new(SwimServer::new(
            routes, bind_to, networking, websockets, config,
        ))
    } else {
        let websockets = RatchetNetworking {
            config: config.websockets,
            provider: NoExtProvider,
            subprotocols,
        };
        Box::new(SwimServer::new(
            routes, bind_to, networking, websockets, config,
        ))
    }
}
