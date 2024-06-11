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

use futures::future::BoxFuture;
use futures::Future;
use futures::FutureExt;
use std::io;
use std::net::SocketAddr;
use std::sync::Arc;
use tokio::net::lookup_host;

/// A trait for defining DNS resolvers.
pub trait DnsResolver {
    /// A future which resolves to either a vector of resolved socket addresses for the provided
    /// host and port, or an IO error.
    type ResolveFuture: Future<Output = io::Result<Vec<SocketAddr>>> + Send + 'static;

    /// Perform a DNS query for A and AAAA records for the provided address. This *may* resolve to
    /// multiple IP addresses.
    fn resolve(&self, host: String, port: u16) -> Self::ResolveFuture;
}

pub type DnsFut = BoxFuture<'static, io::Result<Vec<SocketAddr>>>;
pub type BoxDnsResolver = Box<dyn DnsResolver<ResolveFuture = DnsFut> + Send + 'static>;

impl<R> DnsResolver for Box<R>
where
    R: DnsResolver + ?Sized,
{
    type ResolveFuture = R::ResolveFuture;

    fn resolve(&self, host: String, port: u16) -> Self::ResolveFuture {
        (**self).resolve(host, port)
    }
}

impl<R> DnsResolver for Arc<R>
where
    R: DnsResolver,
{
    type ResolveFuture = R::ResolveFuture;

    fn resolve(&self, host: String, port: u16) -> Self::ResolveFuture {
        (**self).resolve(host, port)
    }
}

/// A resolver which will use the operating system's `getaddrinfo` function to resolve the provided
/// host to an IP address and map the results to a `SocketAddr`.
#[derive(Clone, Debug)]
struct GetAddressInfoResolver;

impl DnsResolver for GetAddressInfoResolver {
    type ResolveFuture = BoxFuture<'static, io::Result<Vec<SocketAddr>>>;

    fn resolve(&self, host: String, port: u16) -> Self::ResolveFuture {
        Box::pin(
            lookup_host(format!("{}:{}", host, port))
                .map(move |r| r.map(|it| it.collect::<Vec<_>>())),
        )
    }
}

#[derive(Debug, Clone)]
pub struct Resolver {
    #[cfg(not(feature = "trust-dns"))]
    inner: GetAddressInfoResolver,
    #[cfg(feature = "trust-dns")]
    inner: trust_dns_impl::TrustDnsResolver,
}

impl Resolver {
    #[cfg(feature = "trust-dns")]
    pub async fn new() -> Resolver {
        Resolver { inner: trust_dns_impl::TrustDnsResolver::new().await }
    }

    #[cfg(not(feature = "trust-dns"))]
    pub async fn new() -> Resolver {
        Resolver { inner: GetAddressInfoResolver }
    }
}

impl DnsResolver for Resolver {
    type ResolveFuture = BoxFuture<'static, io::Result<Vec<SocketAddr>>>;

    fn resolve(&self, host: String, port: u16) -> Self::ResolveFuture {
        self.inner.resolve(host, port).boxed()
    }
}

#[cfg(feature = "trust-dns")]
mod trust_dns_impl {
    use crate::net::dns::DnsResolver;
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

        fn resolve(&self, host: String, port: u16) -> Self::ResolveFuture {
            let resolver = self.inner.clone();
            Box::pin(async move {
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
