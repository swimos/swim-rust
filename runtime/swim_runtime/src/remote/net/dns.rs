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

use crate::remote::table::SchemeHostPort;
use crate::remote::SchemeSocketAddr;
use futures::future::BoxFuture;
use futures::Future;
use futures::FutureExt;
use std::io;
use tokio::net::lookup_host;

/// A trait for defining DNS resolvers.
pub trait DnsResolver {
    /// A future which resolves to either a vector of resolved socket addresses for the provided
    /// host and port, or an IO error.
    type ResolveFuture: Future<Output = io::Result<Vec<SchemeSocketAddr>>> + 'static;

    /// Perform a DNS query for A and AAAA records for the provided address. This *may* resolve to
    /// multiple IP addresses.
    fn resolve(&self, host: SchemeHostPort) -> Self::ResolveFuture;
}

/// A resolver which will use the operating system's `getaddrinfo` function to resolve the provided
/// host to an IP address and map the results to a `SocketAddr`.
#[derive(Clone, Debug)]
pub struct GetAddressInfoResolver;

impl DnsResolver for GetAddressInfoResolver {
    type ResolveFuture = BoxFuture<'static, io::Result<Vec<SchemeSocketAddr>>>;

    fn resolve(&self, host: SchemeHostPort) -> Self::ResolveFuture {
        let (scheme, host, port) = host.split();

        Box::pin(lookup_host(format!("{}:{}", host, port)).map(move |r| {
            r.map(|it| {
                it.map(|socket_addr| SchemeSocketAddr::new(scheme, socket_addr))
                    .collect::<Vec<_>>()
            })
        }))
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

impl DnsResolver for Resolver {
    type ResolveFuture = BoxFuture<'static, io::Result<Vec<SchemeSocketAddr>>>;

    fn resolve(&self, host: SchemeHostPort) -> Self::ResolveFuture {
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
    use crate::remote::net::dns::DnsResolver;
    use crate::remote::table::SchemeHostPort;
    use futures::future::BoxFuture;
    use std::io;
    use std::net::{SocketAddr, ToSocketAddrs};
    use trust_dns_resolver::{system_conf, TokioAsyncResolver};

    /// A DNS resolver built using the Trust-DNS Proto library.
    #[derive(Clone, Debug)]
    pub struct TrustDnsResolver {
        inner: TokioAsyncResolver,
    }

    impl TrustDnsResolver {
        pub async fn new() -> TrustDnsResolver {
            let (config, opts) = system_conf::read_system_conf()
                .expect("Failed to retrieve host system configuration file for Trust DNS resolver");
            let resolver = TokioAsyncResolver::tokio(config, opts).unwrap();

            TrustDnsResolver { inner: resolver }
        }
    }

    impl DnsResolver for TrustDnsResolver {
        type ResolveFuture = BoxFuture<'static, io::Result<Vec<SocketAddr>>>;

        fn resolve(&self, host_and_port: SchemeHostPort) -> Self::ResolveFuture {
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
