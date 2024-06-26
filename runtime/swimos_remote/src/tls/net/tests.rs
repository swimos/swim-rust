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

use std::{net::SocketAddr, path::PathBuf, sync::Arc, time::Duration};

use crate::dns::Resolver;
use crate::net::{ClientConnections, ConnectionError, Listener, ListenerError, Scheme};
use futures::{future::join, StreamExt};
use rustls::crypto::aws_lc_rs;

use crate::tls::{
    CertChain, CertificateFile, ClientConfig, PrivateKey, RustlsClientNetworking,
    RustlsServerNetworking, ServerConfig,
};

const CERTS_PATH: &str = "test-data/certs";
const SERVER_CERT: &str = "server-cert.der";
const SERVER_KEY: &str = "server-key.der";
const CA_CERT: &str = "ca-cert.der";

fn test_data_path(file_name: &str) -> PathBuf {
    let mut path = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
    path.push(CERTS_PATH);
    path.push(file_name);
    path
}

fn make_server_config() -> ServerConfig {
    let ca_cert = std::fs::read(test_data_path(CA_CERT)).expect("Failed to load CA cert.");
    let server_cert =
        std::fs::read(test_data_path(SERVER_CERT)).expect("Failed to load server cert.");
    let server_key = std::fs::read(test_data_path(SERVER_KEY)).expect("Failed to load server key.");

    let chain = CertChain(vec![
        CertificateFile::der(server_cert),
        CertificateFile::der(ca_cert),
    ]);

    let key = PrivateKey::der(server_key);
    ServerConfig {
        chain,
        key,
        enable_log_file: false,
    }
}

fn make_client_config() -> ClientConfig {
    let ca_cert = std::fs::read(test_data_path(CA_CERT)).expect("Failed to load CA cert.");

    ClientConfig {
        use_webpki_roots: true,
        custom_roots: vec![CertificateFile::der(ca_cert)],
    }
}

#[tokio::test]
async fn perform_handshake() {
    let crypto_provider = Arc::new(aws_lc_rs::default_provider());
    let server_net = RustlsServerNetworking::build(make_server_config(), crypto_provider.clone())
        .expect("Invalid server config.");
    let client_net = RustlsClientNetworking::build(
        Arc::new(Resolver::new().await),
        make_client_config(),
        crypto_provider,
    )
    .expect("Invalid client config.");

    let (bound_to, listener) = server_net
        .make_listener("127.0.0.1:0".parse().unwrap())
        .await
        .expect("Failed to bind to port.");

    let server_task = run_server(listener);
    let client_task = run_client(client_net, bound_to, Some("localhost"));

    tokio::time::timeout(Duration::from_secs(5), join(server_task, client_task))
        .await
        .expect("Test timed out.");
}

async fn run_server<L, S>(listener: L)
where
    L: Listener<S>,
    S: Unpin + Send + Sync + 'static,
{
    let mut stream = listener.into_stream();
    match stream.next().await {
        Some(Ok(_)) => {}
        Some(Err(ListenerError::ListenerFailed(err) | ListenerError::AcceptFailed(err))) => {
            panic!("IO error establishing connection. {:?}", err)
        }
        Some(Err(ListenerError::NegotiationFailed(err))) => {
            panic!("Server TLS handshake failed: {:?}", err)
        }
        None => panic!("Listener terminated unexpectedly."),
    }
}

async fn run_client<N>(net: N, bound_to: SocketAddr, host: Option<&str>)
where
    N: ClientConnections,
{
    if let Err(err) = net.try_open(Scheme::Wss, host, bound_to).await {
        match err {
            ConnectionError::ConnectionFailed(err) => {
                panic!("IO error establishing connection. {:?}", err)
            }
            ConnectionError::NegotiationFailed(err) => {
                panic!("Client TLS handshake failed: {:?}", err)
            }
            ConnectionError::BadParameter(err) => panic!("Bad parameter: {:?}", err),
        }
    }
}
