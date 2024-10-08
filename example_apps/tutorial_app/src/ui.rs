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

use std::fmt::Display;
use std::future::Future;
use std::net::{Ipv4Addr, SocketAddr, SocketAddrV4};
use std::time::Duration;

use futures::pin_mut;
use hyper::server::conn::http1::Builder;
use hyper::service::service_fn;
use hyper_util::rt::TokioIo;
use hyper_util::server::graceful::GracefulShutdown;

use tokio::net::TcpListener;
use tokio::select;
use tokio::sync::oneshot;

#[derive(Clone, Copy, Debug)]
struct TimeoutError;

impl Display for TimeoutError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "Forcibly stopping web server after timeout.")
    }
}

impl std::error::Error for TimeoutError {}

pub async fn run_web_server<Shutdown>(
    shutdown: Shutdown,
    addr: oneshot::Receiver<SocketAddr>,
) -> Result<(), Box<dyn std::error::Error + Send + Sync>>
where
    Shutdown: Future<Output = ()>,
{
    addr.await?;
    let bind_to = SocketAddr::V4(SocketAddrV4::new(Ipv4Addr::new(0, 0, 0, 0), 0));
    let listener = TcpListener::bind(bind_to).await?;
    let resolver = hyper_staticfile::Resolver::new("static-files/");
    let addr = listener.local_addr()?;
    println!("Web server bound to: {}", addr);

    let make_svc = service_fn(move |request| {
        let resolver = resolver.clone();
        async move {
            let result = resolver
                .resolve_request(&request)
                .await
                .expect("Failed to access files.");
            let response = hyper_staticfile::ResponseBuilder::new()
                .request(&request)
                .build(result)?;

            Ok::<_, http::Error>(response)
        }
    });

    let shutdown_handle = GracefulShutdown::new();

    pin_mut!(shutdown, listener);

    loop {
        let (stream, _) = select! {
            biased;
            _ = &mut shutdown => {
                tokio::time::timeout(Duration::from_secs(2), shutdown_handle.shutdown())
                   .await
                   .map_err(|_| TimeoutError)?;
               return Ok(());
            }
            result = listener.accept() => result?,
        };

        let conn = Builder::new().serve_connection(TokioIo::new(stream), make_svc.clone());
        tokio::spawn(shutdown_handle.watch(conn));
    }
}
