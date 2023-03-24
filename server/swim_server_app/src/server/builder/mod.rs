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

use std::{
    net::{Ipv4Addr, SocketAddr, SocketAddrV4},
    sync::Arc,
};

use ratchet::{
    deflate::{DeflateConfig, DeflateExtProvider},
    NoExtProvider, ProtocolRegistry, WebSocketStream,
};
use swim_api::{agent::Agent, error::StoreError, store::StoreDisabled};
use swim_runtime::{
    net::{dns::Resolver, plain::TokioPlainTextNetworking, ExternalConnections},
    ws::ext::RatchetNetworking,
};
use swim_tls::{RustlsNetworking, TlsConfig};
use swim_utilities::routing::route_pattern::RoutePattern;

use crate::{
    config::SwimServerConfig,
    error::ServerBuilderError,
    introspection::IntrospectionConfig,
    plane::{PlaneBuilder, PlaneModel},
};

use super::{
    runtime::{SwimServer, Transport},
    store::{in_memory::InMemoryPersistence, ServerPersistence},
    BoxServer, Server,
};

/// Builder for a swim server that will listen on a socket and run a suite of agents.
pub struct ServerBuilder {
    bind_to: SocketAddr,
    plane: PlaneBuilder,
    tls_config: Option<TlsConfig>,
    deflate: Option<DeflateConfig>,
    config: SwimServerConfig,
    store_options: StoreConfig,
    introspection: Option<IntrospectionConfig>,
}

#[non_exhaustive]
#[derive(Default)]
enum StoreConfig {
    #[default]
    NoStore,
    InMemory,
    #[cfg(feature = "rocks_store")]
    RockStore {
        path: Option<std::path::PathBuf>,
        options: crate::RocksOpts,
    },
}

const DEFAULT_PORT: u16 = 0;

impl ServerBuilder {
    pub fn with_plane_name(name: &str) -> Self {
        Self {
            bind_to: SocketAddr::V4(SocketAddrV4::new(Ipv4Addr::new(0, 0, 0, 0), DEFAULT_PORT)),
            plane: PlaneBuilder::with_name(name),
            tls_config: None,
            deflate: Default::default(),
            config: Default::default(),
            store_options: Default::default(),
            introspection: Default::default(),
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
    pub fn add_tls_support(mut self, config: TlsConfig) -> Self {
        self.tls_config = Some(config);
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

    /// Enable agent and lane introspection in the server.
    pub fn enable_introspection(mut self) -> Self {
        self.introspection = Some(IntrospectionConfig::default());
        self
    }

    /// Configure the introspection system (this implicitly enables introspection).
    /// #Arguments
    /// * `config` - Configuration parameters for the introspection system.
    pub fn configure_introspection(mut self, config: IntrospectionConfig) -> Self {
        self.introspection = Some(config);
        self
    }

    /// Enable the in memory persistence store. The state of agents will be kept across restarts but will
    /// be lost when the process stops.
    pub fn with_in_memory_store(mut self) -> Self {
        self.store_options = StoreConfig::InMemory;
        self
    }

    /// Attempt to make a server instance. This will fail if the routes specified for the
    /// agents are ambiguous.
    pub async fn build(self) -> Result<BoxServer, ServerBuilderError> {
        let ServerBuilder {
            bind_to,
            plane,
            tls_config,
            deflate,
            config,
            store_options,
            introspection,
        } = self;
        let routes = plane.build()?;
        if introspection.is_some() {
            routes.check_meta_collisions()?;
        }
        let resolver = Arc::new(Resolver::new().await);
        let config = AppConfig {
            server: config,
            store: store_options,
            deflate,
            introspection,
        };
        if let Some(tls_conf) = tls_config {
            let networking = RustlsNetworking::try_from_config(resolver, tls_conf)?;
            Ok(BoxServer(with_store(bind_to, routes, networking, config)?))
        } else {
            let networking = TokioPlainTextNetworking::new(resolver);
            Ok(BoxServer(with_store(bind_to, routes, networking, config)?))
        }
    }
}

struct AppConfig {
    server: SwimServerConfig,
    store: StoreConfig,
    deflate: Option<DeflateConfig>,
    introspection: Option<IntrospectionConfig>,
}

fn with_store<N>(
    bind_to: SocketAddr,
    routes: PlaneModel,
    networking: N,
    mut config: AppConfig,
) -> Result<Box<dyn Server>, StoreError>
where
    N: ExternalConnections,
    N::Socket: WebSocketStream,
{
    let store_config = std::mem::take(&mut config.store);
    match store_config {
        #[cfg(feature = "rocks_store")]
        StoreConfig::RockStore { path, options } => {
            let store = super::store::rocks::create_rocks_store(path, options)?;
            Ok(with_websockets(bind_to, routes, networking, config, store))
        }
        StoreConfig::InMemory => Ok(with_websockets(
            bind_to,
            routes,
            networking,
            config,
            InMemoryPersistence::default(),
        )),
        _ => Ok(with_websockets(
            bind_to,
            routes,
            networking,
            config,
            StoreDisabled,
        )),
    }
}

fn with_websockets<N, Store>(
    bind_to: SocketAddr,
    routes: PlaneModel,
    networking: N,
    config: AppConfig,
    store: Store,
) -> Box<dyn Server>
where
    N: ExternalConnections,
    N::Socket: WebSocketStream,
    Store: ServerPersistence + Send + Sync + 'static,
{
    let AppConfig {
        server: server_config,
        deflate,
        introspection,
        ..
    } = config;
    let subprotocols = ProtocolRegistry::new(vec!["warp0"]).unwrap();
    if let Some(deflate_config) = deflate {
        let websockets = RatchetNetworking {
            config: server_config.websockets,
            provider: DeflateExtProvider::with_config(deflate_config),
            subprotocols,
        };
        Box::new(SwimServer::new(
            routes,
            bind_to,
            Transport::new(networking, websockets),
            server_config,
            store,
            introspection,
        ))
    } else {
        let websockets = RatchetNetworking {
            config: server_config.websockets,
            provider: NoExtProvider,
            subprotocols,
        };
        Box::new(SwimServer::new(
            routes,
            bind_to,
            Transport::new(networking, websockets),
            server_config,
            store,
            introspection,
        ))
    }
}

#[cfg(feature = "rocks_store")]
const _: () = {
    use swim_persistence::rocks::default_db_opts;
    impl ServerBuilder {
        pub fn enable_rocks_store(mut self) -> Self {
            self.store_options = StoreConfig::RockStore {
                path: None,
                options: default_db_opts(),
            };
            self
        }

        pub fn set_rocks_store_path<P: AsRef<std::ffi::OsStr>>(mut self, base_path: P) -> Self {
            let db_path = std::path::PathBuf::from(std::path::Path::new(&base_path));
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

        pub fn modify_rocks_opts<F: FnOnce(&mut crate::RocksOpts)>(mut self, f: F) -> Self {
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
};
