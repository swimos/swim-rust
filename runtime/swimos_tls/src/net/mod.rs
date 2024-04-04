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

mod client;
mod server;
#[cfg(test)]
mod tests;

use std::{net::SocketAddr, sync::Arc};

pub use client::RustTlsClientNetworking;
use futures::future::Either;
use futures::TryFutureExt;
use futures::{future::BoxFuture, FutureExt};
pub use server::{RustTlsListener, RustTlsServerNetworking};
use swimos_api::net::Scheme;
use swimos_remote::net::plain::TokioPlainTextNetworking;
use swimos_remote::net::{
    dns::{BoxDnsResolver, Resolver},
    ClientConnections, ConnResult, IoResult, ServerConnections,
};

use crate::{
    config::{CertFormat, CertificateFile, TlsConfig},
    errors::TlsError,
    maybe::MaybeTlsStream,
    ClientConfig,
};

use self::server::MaybeRustTlsListener;

fn load_cert_file(file: CertificateFile) -> Result<Vec<rustls::Certificate>, TlsError> {
    let CertificateFile { format, body } = file;
    let certs = match format {
        CertFormat::Pem => {
            let mut body_ref = body.as_ref();
            rustls_pemfile::certs(&mut body_ref).map_err(TlsError::InvalidPem)?
        }
        CertFormat::Der => vec![body],
    };
    Ok(certs.into_iter().map(rustls::Certificate).collect())
}

/// Combined implementation of [`ClientConnections`] and [`ServerConnections`] that wraps
/// [`RustTlsClientNetworking`], [`RustTlsServerNetworking`] and [`TokioPlainTextNetworking`]. The server part is adapted to
/// produce [`MaybeTlsStream`] connections so that there is a unified client/server socket type,
/// inducing an implementation of [`super::ExternalConnections`].
#[derive(Clone)]
pub struct RustNetworking {
    client: RustTlsClientNetworking,
    server: Either<TokioPlainTextNetworking, RustTlsServerNetworking>,
}

impl RustNetworking {
    pub fn new_plain_text(
        client: RustTlsClientNetworking,
        server: TokioPlainTextNetworking,
    ) -> Self {
        RustNetworking {
            client,
            server: Either::Left(server),
        }
    }

    pub fn new_tls(client: RustTlsClientNetworking, server: RustTlsServerNetworking) -> Self {
        RustNetworking {
            client,
            server: Either::Right(server),
        }
    }

    pub fn try_plain_text_from_config(
        resolver: Arc<Resolver>,
        client_config: ClientConfig,
    ) -> Result<Self, TlsError> {
        let client = RustTlsClientNetworking::try_from_config(resolver.clone(), client_config)?;
        let server = TokioPlainTextNetworking::new(resolver);
        Ok(RustNetworking {
            client,
            server: Either::Left(server),
        })
    }

    pub fn try_tls_from_config(
        resolver: Arc<Resolver>,
        config: TlsConfig,
    ) -> Result<Self, TlsError> {
        let TlsConfig {
            client: client_conf,
            server: server_conf,
        } = config;
        let client = RustTlsClientNetworking::try_from_config(resolver, client_conf)?;
        let server = RustTlsServerNetworking::try_from(server_conf)?;
        Ok(RustNetworking {
            client,
            server: Either::Right(server),
        })
    }
}

impl ClientConnections for RustNetworking {
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

impl ServerConnections for RustNetworking {
    type ServerSocket = MaybeTlsStream;

    type ListenerType = MaybeRustTlsListener;

    fn bind(
        &self,
        addr: SocketAddr,
    ) -> BoxFuture<'static, ConnResult<(SocketAddr, Self::ListenerType)>> {
        match &self.server {
            Either::Left(plain_text_server) => plain_text_server
                .bind(addr)
                .map_ok(|(addr, listener)| (addr, MaybeRustTlsListener::from(listener)))
                .boxed(),
            Either::Right(tls_server) => tls_server
                .make_listener(addr)
                .map_ok(|(addr, listener)| (addr, MaybeRustTlsListener::from(listener)))
                .boxed(),
        }
    }
}
