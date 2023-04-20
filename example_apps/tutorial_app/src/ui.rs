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

use std::future::Future;
use std::net::{Ipv4Addr, SocketAddr, SocketAddrV4};

use hyper::service::{make_service_fn, service_fn};
use hyper::Server;
use std::convert::Infallible;
use std::path::Path;
use tokio::net::TcpListener;
use tokio::sync::oneshot;

pub async fn run_web_server<Shutdown>(
    shutdown: Shutdown,
    addr: oneshot::Receiver<SocketAddr>,
) -> Result<(), Box<dyn std::error::Error>>
where
    Shutdown: Future<Output = ()>,
{
    addr.await?;
    let bind_to = SocketAddr::V4(SocketAddrV4::new(Ipv4Addr::new(0, 0, 0, 0), 0));
    let listener = TcpListener::bind(bind_to).await?;
    let root = Path::new("static-files/");

    let addr = listener.local_addr()?;
    println!("Web server bound to: {}", addr);

    let make_svc = make_service_fn(move |_conn| async move {
        Ok::<_, Infallible>(service_fn(move |request| async move {
            let result = hyper_staticfile::resolve(&root, &request)
                .await
                .expect("Failed to access files.");
            let response = hyper_staticfile::ResponseBuilder::new()
                .request(&request)
                .build(result)?;

            Ok::<_, http::Error>(response)
        }))
    });

    let server = Server::from_tcp(listener.into_std()?)?.serve(make_svc);

    server.with_graceful_shutdown(shutdown).await?;
    Ok(())
}
