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

use crate::ws::error::WebSocketError;
use crate::ws::{StreamType, WebSocketConfig};
use native_tls::{Certificate, TlsAcceptor, TlsConnector};
use std::env;
use std::fs::File;
use std::io::{BufReader, Read};
use std::net::{TcpStream, ToSocketAddrs};
use std::str::FromStr;
use tokio::io::{AsyncRead, AsyncWrite};
use tokio_tungstenite::client_async_tls_with_config;
use tokio_tungstenite::tungstenite::client::{AutoStream, IntoClientRequest};
use tokio_tungstenite::tungstenite::http::uri::Scheme;
use tokio_tungstenite::tungstenite::http::Request;
use url::Url;

const UNSUPPORTED_SCHEME: &str = "Unsupported URL scheme";

/// If the request scheme is `warp` or `warps` then it is replaced with a supported format.
fn maybe_normalise_url<T>(request: &Request<T>) -> Result<(), WebSocketError> {
    let uri = request.uri().clone();
    let new_scheme = match uri.scheme_str() {
        Some("warp") => Some("ws"),
        Some("warps") => Some("wss"),
        Some(s) => Some(s),
        None => None,
    }
    .ok_or_else(|| WebSocketError::Url(String::from(UNSUPPORTED_SCHEME)))?;

    request
        .uri()
        .scheme()
        .replace(&Scheme::from_str(new_scheme).unwrap());

    Ok(())
}

pub async fn connect(url: Url, config: &WebSocketConfig) -> Result<(), WebSocketError> {
    let mut request = url.as_str().into_client_request().unwrap();
    let port = request
        .uri()
        .port_u16()
        .or_else(|| match request.uri().scheme_str() {
            Some("wss") => Some(443),
            Some("ws") => Some(80),
            _ => None,
        })
        .ok_or_else(|| WebSocketError::Url(String::from(UNSUPPORTED_SCHEME)))?;

    maybe_normalise_url(&request);
    unimplemented!()
}

fn build_stream<A>(
    host: &str,
    addr: A,
    config: &WebSocketConfig,
) -> Result<AutoStream, WebSocketError>
where
    A: ToSocketAddrs,
{
    let stream = TcpStream::connect(addr).unwrap();

    match &config.stream_type {
        StreamType::Plain => Ok(AutoStream::Plain(stream)),
        StreamType::Tls(tls_config) => {
            let mut tls_conn_builder = TlsConnector::builder();
            let mut reader = BufReader::new(File::open(tls_config).unwrap());

            let mut buf = vec![];
            reader.read_to_end(&mut buf);

            match Certificate::from_pem(&buf) {
                Ok(cert) => {
                    tls_conn_builder.add_root_certificate(cert);
                }
                Err(e) => return Err(WebSocketError::Tls(e.to_string())),
            }

            let connector = tls_conn_builder
                .build()
                .map_err(|e| WebSocketError::Tls(e.to_string()))?;
            let connected = connector.connect(&host, stream);

            match connected {
                Ok(s) => Ok(AutoStream::Tls(s)),
                Err(e) => Err(WebSocketError::Tls(e.to_string())),
            }
        }
    }
}
