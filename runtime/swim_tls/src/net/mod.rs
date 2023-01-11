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

mod client;
mod server;

use std::{net::SocketAddr, sync::Arc};

pub use client::RustlsClientNetworking;
use futures::TryFutureExt;
use futures::{future::BoxFuture, FutureExt};
pub use server::{RustlsListener, RustlsServerNetworking};
use swim_runtime::net::{
    dns::{BoxDnsResolver, Resolver},
    ClientConnections, ConnResult, IoResult, Scheme, ServerConnections,
};

use crate::{
    config::{CertFormat, Certificate, TlsConfig},
    errors::TlsError,
    maybe::MaybeTlsStream,
};

use self::server::MaybeRustlsListener;

fn load_cert_file(file: Certificate) -> Result<Vec<rustls::Certificate>, TlsError> {
    let Certificate { format, body } = file;
    let certs = match format {
        CertFormat::Pem => {
            let mut body_ref = body.as_ref();
            rustls_pemfile::certs(&mut body_ref).map_err(TlsError::InvalidPem)?
        }
        CertFormat::Der => vec![body],
    };
    Ok(certs.into_iter().map(rustls::Certificate).collect())
}

#[derive(Clone)]
pub struct RustlsNetworking {
    client: RustlsClientNetworking,
    server: RustlsServerNetworking,
}

impl RustlsNetworking {
    pub fn new(client: RustlsClientNetworking, server: RustlsServerNetworking) -> Self {
        RustlsNetworking { client, server }
    }

    pub fn try_from_config(resolver: Arc<Resolver>, config: TlsConfig) -> Result<Self, TlsError> {
        let TlsConfig {
            client: client_conf,
            server: server_conf,
        } = config;
        let client = RustlsClientNetworking::try_from_config(resolver, client_conf)?;
        let server = RustlsServerNetworking::try_from(server_conf)?;
        Ok(RustlsNetworking { client, server })
    }
}

impl ClientConnections for RustlsNetworking {
    type ClientSocket = MaybeTlsStream;

    fn try_open(
        &self,
        scheme: Scheme,
        host: Option<&str>,
        addr: SocketAddr,
    ) -> BoxFuture<'_, ConnResult<Self::ClientSocket>> {
        self.client.try_open(scheme, host, addr)
    }

    fn dns_resolver(&self) -> BoxDnsResolver {
        self.client.dns_resolver()
    }

    fn lookup(&self, host: String, port: u16) -> BoxFuture<'static, IoResult<Vec<SocketAddr>>> {
        self.client.lookup(host, port)
    }
}

impl ServerConnections for RustlsNetworking {
    type ServerSocket = MaybeTlsStream;

    type ListenerType = MaybeRustlsListener;

    fn bind(
        &self,
        addr: SocketAddr,
    ) -> BoxFuture<'static, ConnResult<(SocketAddr, Self::ListenerType)>> {
        self.server
            .make_listener(addr)
            .map_ok(|(addr, listener)| (addr, MaybeRustlsListener::from(listener)))
            .boxed()
    }
}
