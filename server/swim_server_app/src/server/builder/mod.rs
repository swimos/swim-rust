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
    ffi::OsStr,
    net::{Ipv4Addr, SocketAddr, SocketAddrV4},
    path::{Path, PathBuf},
    sync::Arc,
};

use ratchet::{
    deflate::{DeflateConfig, DeflateExtProvider},
    NoExtProvider, ProtocolRegistry, WebSocketStream,
};
use swim_api::{agent::Agent, error::StoreError, store::StoreDisabled};
use swim_persistence::rocks::{default_db_opts, RocksOpts};
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
    error::ServerBuilderError,
    plane::{PlaneBuilder, PlaneModel},
};

use super::{
    runtime::SwimServer,
    store::{rocks::create_rocks_store, ServerPersistence},
    BoxServer, Server,
};

/// Builder for a swim server that will listen on a socket and run a suite of agents.
pub struct ServerBuilder {
    bind_to: SocketAddr,
    plane: PlaneBuilder,
    enable_tls: bool,
    deflate: Option<DeflateConfig>,
    config: SwimServerConfig,
    #[cfg(feature = "persistence")]
    store_options: StoreConfig,
}

#[non_exhaustive]
enum StoreConfig {
    NoStore,
    #[cfg(feature = "persistence")]
    RockStore {
        path: Option<PathBuf>,
        options: crate::RocksOpts,
    },
}

impl Default for StoreConfig {
    fn default() -> Self {
        StoreConfig::NoStore
    }
}

const DEFAULT_PORT: u16 = 0;

impl ServerBuilder {
    pub fn with_plane_name(name: &str) -> Self {
        Self {
            bind_to: SocketAddr::V4(SocketAddrV4::new(Ipv4Addr::new(0, 0, 0, 0), DEFAULT_PORT)),
            plane: PlaneBuilder::with_name(name),
            enable_tls: false,
            deflate: Default::default(),
            config: Default::default(),
            store_options: Default::default(),
        }
    }

    /// #Arguments
    ///
    /// * `addr` - The address to which the server will bind (default: 0.0.0.0:8080).
    pub fn set_bind_addr(mut self, addr: SocketAddr) -> Self {
        self.bind_to = addr;
        self
    }

    /// Add a new route to the plane that the server will run.
    ///
    /// #Arguments
    ///
    /// * `pattern` - The route pattern against which to match incoming envelopes.
    /// * `agent` - The agent definition.
    pub fn add_route<A: Agent + Send + 'static>(mut self, pattern: RoutePattern, agent: A) -> Self {
        self.plane.add_route(pattern, agent);
        self
    }

    /// Enable TLS on the server.
    pub fn add_tls_support(mut self) -> Self {
        self.enable_tls = true;
        self
    }

    /// Enable the deflate extension for websocket connections.
    pub fn add_deflate_support(self) -> Self {
        self.configure_deflate_support(Default::default())
    }

    /// Enable the deflate extension for websocket connections.
    ///  
    /// #Arguments
    /// * `config` - Configuration parameters for the compression.
    pub fn configure_deflate_support(mut self, config: DeflateConfig) -> Self {
        self.deflate = Some(config);
        self
    }

    /// Alter the server configuration parameters.
    ///
    /// # Arguments
    /// * `f` - A function that can mutate the configuration.
    pub fn update_config<F>(mut self, f: F) -> Self
    where
        F: FnOnce(&mut SwimServerConfig),
    {
        f(&mut self.config);
        self
    }

    /// Attempt to make a server instance. This will fail if the routes specified for the
    /// agents are ambiguous.
    pub async fn build(self) -> Result<BoxServer, ServerBuilderError> {
        let ServerBuilder {
            bind_to,
            plane,
            enable_tls,
            deflate,
            config,
            store_options,
        } = self;
        let routes = plane.build()?;
        let resolver = Arc::new(Resolver::new().await);
        if enable_tls {
            //TODO Make this support actual identities.
            let networking =
                TokioTlsNetworking::new::<_, Box<PathBuf>>(std::iter::empty(), resolver);
            Ok(BoxServer(with_store(
                bind_to,
                routes,
                networking,
                config,
                deflate,
                store_options,
            )?))
        } else {
            let networking = TokioPlainTextNetworking::new(resolver);
            Ok(BoxServer(with_store(
                bind_to,
                routes,
                networking,
                config,
                deflate,
                store_options,
            )?))
        }
    }
}

fn with_store<N>(
    bind_to: SocketAddr,
    routes: PlaneModel,
    networking: N,
    config: SwimServerConfig,
    deflate: Option<DeflateConfig>,
    store_config: StoreConfig,
) -> Result<Box<dyn Server>, StoreError>
where
    N: ExternalConnections,
    N::Socket: WebSocketStream,
{
    match store_config {
        #[cfg(feature = "persistence")]
        StoreConfig::RockStore { path, options } => {
            let store = create_rocks_store(path, options)?;
            Ok(with_websockets(
                bind_to, routes, networking, config, deflate, store,
            ))
        }
        _ => Ok(with_websockets(
            bind_to,
            routes,
            networking,
            config,
            deflate,
            StoreDisabled,
        )),
    }
}

fn with_websockets<N, Store>(
    bind_to: SocketAddr,
    routes: PlaneModel,
    networking: N,
    config: SwimServerConfig,
    deflate: Option<DeflateConfig>,
    store: Store,
) -> Box<dyn Server>
where
    N: ExternalConnections,
    N::Socket: WebSocketStream,
    Store: ServerPersistence + Send + Sync + 'static,
{
    let subprotocols = ProtocolRegistry::new(vec!["warp0"]).unwrap();
    if let Some(deflate_config) = deflate {
        let websockets = RatchetNetworking {
            config: config.websockets,
            provider: DeflateExtProvider::with_config(deflate_config),
            subprotocols,
        };
        Box::new(SwimServer::new(
            routes, bind_to, networking, websockets, config, store,
        ))
    } else {
        let websockets = RatchetNetworking {
            config: config.websockets,
            provider: NoExtProvider,
            subprotocols,
        };
        Box::new(SwimServer::new(
            routes, bind_to, networking, websockets, config, store,
        ))
    }
}

#[cfg(feature = "persistence")]
impl ServerBuilder {
    pub fn enable_rocks_store(mut self) -> Self {
        self.store_options = StoreConfig::RockStore {
            path: None,
            options: default_db_opts(),
        };
        self
    }

    pub fn set_store_path<P: AsRef<OsStr>>(mut self, base_path: P) -> Self {
        let db_path = PathBuf::from(Path::new(&base_path));
        match &mut self.store_options {
            StoreConfig::RockStore { path, .. } => {
                *path = Some(db_path);
            }
            _ => {
                self.store_options = StoreConfig::RockStore {
                    path: Some(db_path),
                    options: default_db_opts(),
                };
            }
        }
        self
    }

    pub fn modify_rocks_opts<F: FnOnce(&mut RocksOpts)>(mut self, f: F) -> Self {
        match &mut self.store_options {
            StoreConfig::RockStore { options, .. } => {
                f(options);
            }
            _ => {
                let mut options = default_db_opts();
                f(&mut options);
                self.store_options = StoreConfig::RockStore {
                    path: None,
                    options,
                };
            }
        }
        self
    }
}
