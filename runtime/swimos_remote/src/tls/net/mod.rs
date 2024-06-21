// Copyright 2015-2024 Swim Inc.
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

use std::net::SocketAddr;

use crate::dns::BoxDnsResolver;
use crate::net::{ClientConnections, ConnectionResult, Scheme, ServerConnections};
use crate::plain::TokioPlainTextNetworking;
pub use client::RustlsClientNetworking;
use futures::future::Either;
use futures::TryFutureExt;
use futures::{future::BoxFuture, FutureExt};
pub use server::{RustlsListener, RustlsServerNetworking};

use crate::tls::{
    config::{CertFormat, CertificateFile},
    errors::TlsError,
    maybe::MaybeTlsStream,
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
/// [`RustlsClientNetworking`], [`RustlsServerNetworking`] and [`TokioPlainTextNetworking`]. The server part is adapted to
/// produce [`MaybeTlsStream`] connections so that there is a unified client/server socket type,
/// inducing an implementation of [`crate::ExternalConnections`].
#[derive(Clone)]
pub struct RustlsNetworking {
    client: RustlsClientNetworking,
    server: Either<TokioPlainTextNetworking, RustlsServerNetworking>,
}

impl RustlsNetworking {
    pub fn new_plain_text(
        client: RustlsClientNetworking,
        server: TokioPlainTextNetworking,
    ) -> Self {
        RustlsNetworking {
            client,
            server: Either::Left(server),
        }
    }

    pub fn new_tls(client: RustlsClientNetworking, server: RustlsServerNetworking) -> Self {
        RustlsNetworking {
            client,
            server: Either::Right(server),
        }
    }
}

impl ClientConnections for RustlsNetworking {
    type ClientSocket = MaybeTlsStream;

    fn try_open(
        &self,
        scheme: Scheme,
        host: Option<&str>,
        addr: SocketAddr,
    ) -> BoxFuture<'_, ConnectionResult<Self::ClientSocket>> {
        self.client.try_open(scheme, host, addr)
    }

    fn dns_resolver(&self) -> BoxDnsResolver {
        self.client.dns_resolver()
    }

    fn lookup(
        &self,
        host: String,
        port: u16,
    ) -> BoxFuture<'static, std::io::Result<Vec<SocketAddr>>> {
        self.client.lookup(host, port)
    }
}

impl ServerConnections for RustlsNetworking {
    type ServerSocket = MaybeTlsStream;

    type ListenerType = MaybeRustTlsListener;

    fn bind(
        &self,
        addr: SocketAddr,
    ) -> BoxFuture<'static, ConnectionResult<(SocketAddr, Self::ListenerType)>> {
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
