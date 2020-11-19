// Copyright 2015-2020 SWIM.AI inc.
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

use crate::routing::remote::table::HostAndPort;
use futures::Future;
use futures::FutureExt;
use futures_util::future::BoxFuture;
use std::io;
use std::net::SocketAddr;
use tokio::net::lookup_host;

/// A trait for defining DNS resolvers.
pub trait HttpResolver {
    /// A future which resolves to either a vector of resolved socket addresses for the provided
    /// host and port, or an IO error.
    type ResolveFuture: Future<Output = io::Result<Vec<SocketAddr>>> + 'static;

    /// Perform a DNS query for A and AAAA records for the provided address. This *may* resolve to
    /// multiple IP addresses.
    fn resolve(&self, host: HostAndPort) -> Self::ResolveFuture;
}

/// A resolver which will use the operating system's `getaddrinfo` function to resolve the provided
/// host to an IP address and map the results to a `SocketAddr`.
#[derive(Clone, Debug)]
pub struct GetAddressInfoResolver;

impl HttpResolver for GetAddressInfoResolver {
    type ResolveFuture = BoxFuture<'static, io::Result<Vec<SocketAddr>>>;

    fn resolve(&self, host: HostAndPort) -> Self::ResolveFuture {
        let (host, port) = host.split();
        Box::pin(
            lookup_host(format!("{}:{}", host, port)).map(|r| r.map(|it| it.collect::<Vec<_>>())),
        )
    }
}

#[derive(Debug, Clone)]
pub enum Resolver {
    #[cfg(not(feature = "trust-dns"))]
    GetAddressInfo(GetAddressInfoResolver),
    #[cfg(feature = "trust-dns")]
    TrustDns(trust_dns_impl::TrustDnsResolver),
}

impl Resolver {
    pub async fn new() -> Resolver {
        #[cfg(feature = "trust-dns")]
        {
            Resolver::TrustDns(trust_dns_impl::TrustDnsResolver::new().await)
        }
        #[cfg(not(feature = "trust-dns"))]
        {
            Resolver::GetAddressInfo(GetAddressInfoResolver)
        }
    }
}

impl HttpResolver for Resolver {
    type ResolveFuture = BoxFuture<'static, io::Result<Vec<SocketAddr>>>;

    fn resolve(&self, host: HostAndPort) -> Self::ResolveFuture {
        match self {
            #[cfg(not(feature = "trust-dns"))]
            Resolver::GetAddressInfo(resolver) => resolver.resolve(host).boxed(),
            #[cfg(feature = "trust-dns")]
            Resolver::TrustDns(resolver) => resolver.resolve(host).boxed(),
        }
    }
}

#[cfg(feature = "trust-dns")]
mod trust_dns_impl {
    use crate::routing::remote::net::dns::HttpResolver;
    use crate::routing::remote::table::HostAndPort;
    use futures::future::BoxFuture;
    use std::io;
    use std::net::{SocketAddr, ToSocketAddrs};
    use trust_dns_resolver::{
        system_conf, AsyncResolver, TokioConnection, TokioConnectionProvider,
    };

    /// A DNS resolver built using the Trust-DNS Proto library.
    #[derive(Clone, Debug)]
    pub struct TrustDnsResolver {
        inner: AsyncResolver<TokioConnection, TokioConnectionProvider>,
    }

    impl TrustDnsResolver {
        pub async fn new() -> TrustDnsResolver {
            let (config, opts) = system_conf::read_system_conf()
                .expect("Failed to retrieve host system configuration file for Trust DNS resolver");
            let resolver = AsyncResolver::new(config, opts, tokio::runtime::Handle::current())
                .await
                .unwrap();

            TrustDnsResolver { inner: resolver }
        }
    }

    impl HttpResolver for TrustDnsResolver {
        type ResolveFuture = BoxFuture<'static, io::Result<Vec<SocketAddr>>>;

        fn resolve(&self, host_and_port: HostAndPort) -> Self::ResolveFuture {
            let resolver = self.inner.clone();
            Box::pin(async move {
                let (host, port) = host_and_port.split();
                let lookup = resolver.lookup_ip(host).await?;
                let mut addresses = Vec::new();

                for addr in lookup {
                    match (addr, port).to_socket_addrs() {
                        Ok(sock) => addresses.extend(sock),
                        Err(e) => return Err(e),
                    }
                }

                Ok(addresses)
            })
        }
    }
}
