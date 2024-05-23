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

use std::{net::SocketAddr, sync::Arc};

use futures::{future::BoxFuture, FutureExt};
use rustls::{OwnedTrustAnchor, RootCertStore, ServerName};

use swimos_net::Scheme;
use swimos_remote::net::{
    dns::{BoxDnsResolver, DnsResolver, Resolver},
    ClientConnections, ConnResult, ConnectionError, IoResult,
};
use tokio::net::TcpStream;
use tokio_rustls::{TlsConnector, TlsStream};

use crate::{config::ClientConfig, errors::TlsError, maybe::MaybeTlsStream};

/// [`ClientConnections`] implementation that supports opening both secure and insecure connections.
#[derive(Clone)]
pub struct RustlsClientNetworking {
    resolver: Arc<Resolver>,
    connector: TlsConnector,
}

impl RustlsClientNetworking {
    pub fn new(resolver: Arc<Resolver>, connector: TlsConnector) -> Self {
        RustlsClientNetworking {
            resolver,
            connector,
        }
    }

    pub fn try_from_config(
        resolver: Arc<Resolver>,
        config: ClientConfig,
    ) -> Result<Self, TlsError> {
        let ClientConfig {
            use_webpki_roots,
            custom_roots,
        } = config;
        let mut root_store = RootCertStore::empty();
        if use_webpki_roots {
            root_store.add_server_trust_anchors(webpki_roots::TLS_SERVER_ROOTS.0.iter().map(
                |ta| {
                    OwnedTrustAnchor::from_subject_spki_name_constraints(
                        ta.subject,
                        ta.spki,
                        ta.name_constraints,
                    )
                },
            ));
        }

        for cert in custom_roots {
            for c in super::load_cert_file(cert)? {
                root_store.add(&c)?;
            }
        }

        let config = rustls::ClientConfig::builder()
            .with_safe_defaults()
            .with_root_certificates(root_store)
            .with_no_client_auth();

        let connector = TlsConnector::from(Arc::new(config));
        Ok(RustlsClientNetworking::new(resolver, connector))
    }
}

impl ClientConnections for RustlsClientNetworking {
    type ClientSocket = MaybeTlsStream;

    fn try_open(
        &self,
        scheme: Scheme,
        host: Option<&str>,
        addr: SocketAddr,
    ) -> BoxFuture<'_, ConnResult<Self::ClientSocket>> {
        match scheme {
            Scheme::Ws => async move {
                let stream = TcpStream::connect(addr).await?;
                Ok(MaybeTlsStream::Plain(stream))
            }
            .boxed(),
            Scheme::Wss => {
                let domain = if let Some(host_name) = host {
                    ServerName::try_from(host_name).map_err(|err| {
                        ConnectionError::BadParameter(Box::new(TlsError::BadHostName(err)))
                    })
                } else {
                    Ok(ServerName::IpAddress(addr.ip()))
                };
                async move {
                    let stream = TcpStream::connect(addr).await?;
                    let RustlsClientNetworking { connector, .. } = self;

                    let client = connector.connect(domain?, stream).await.map_err(|err| {
                        let tls_err = TlsError::HandshakeFailed(err);
                        ConnectionError::NegotiationFailed(Box::new(tls_err))
                    })?;
                    Ok(MaybeTlsStream::Tls(TlsStream::Client(client)))
                }
                .boxed()
            }
        }
    }

    fn dns_resolver(&self) -> BoxDnsResolver {
        Box::new(self.resolver.clone())
    }

    fn lookup(&self, host: String, port: u16) -> BoxFuture<'static, IoResult<Vec<SocketAddr>>> {
        self.resolver.resolve(host, port)
    }
}
