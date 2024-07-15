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

#[doc(hidden)]
pub type DnsFut = BoxFuture<'static, io::Result<Vec<SocketAddr>>>;

#[doc(hidden)]
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

/// The default DNS resolver. If the `hickory_dns` feature flag is enabled, this will use the `hickory_dns`
/// implementation, otherwise it will use the operating system's built-in DNS support.
#[derive(Debug, Clone)]
pub struct Resolver {
    #[cfg(not(feature = "hickory_dns"))]
    inner: GetAddressInfoResolver,
    #[cfg(feature = "hickory_dns")]
    inner: hickory_dns_impl::HickoryDnsResolver,
}

impl Resolver {
    #[cfg(feature = "hickory_dns")]
    pub async fn new() -> Resolver {
        Resolver {
            inner: hickory_dns_impl::HickoryDnsResolver::new().await,
        }
    }

    #[cfg(not(feature = "hickory_dns"))]
    pub async fn new() -> Resolver {
        Resolver {
            inner: GetAddressInfoResolver,
        }
    }
}

impl DnsResolver for Resolver {
    type ResolveFuture = BoxFuture<'static, io::Result<Vec<SocketAddr>>>;

    fn resolve(&self, host: String, port: u16) -> Self::ResolveFuture {
        self.inner.resolve(host, port).boxed()
    }
}

#[cfg(feature = "hickory_dns")]
mod hickory_dns_impl {
    use crate::dns::DnsResolver;
    use futures::future::BoxFuture;
    use hickory_resolver::{system_conf, TokioAsyncResolver};
    use std::io;
    use std::net::{SocketAddr, ToSocketAddrs};

    /// A DNS resolver built using the Hickory-DNS Proto library.
    #[derive(Clone, Debug)]
    pub struct HickoryDnsResolver {
        inner: TokioAsyncResolver,
    }

    impl HickoryDnsResolver {
        pub async fn new() -> HickoryDnsResolver {
            let (config, opts) = system_conf::read_system_conf().expect(
                "Failed to retrieve host system configuration file for Hickory DNS resolver",
            );
            HickoryDnsResolver {
                inner: TokioAsyncResolver::tokio(config, opts),
            }
        }
    }

    impl DnsResolver for HickoryDnsResolver {
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
