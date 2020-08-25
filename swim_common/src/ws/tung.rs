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
use futures::{StreamExt, TryFutureExt};
use futures_util::SinkExt;
use http::request::Parts;
use http::{Response, Uri};
use native_tls::{Certificate, TlsAcceptor, TlsConnector};
use std::env;
use std::fs::File;
use std::io::{BufReader, Read};
use std::str::FromStr;
use tokio::io::{AsyncRead, AsyncWrite};
use tokio::net::TcpStream;
use tokio::net::ToSocketAddrs;
use tokio_native_tls::{TlsConnector as TokioTlsConnector, TlsStream};
use tokio_tungstenite::stream::Stream as StreamSwitcher;
use tokio_tungstenite::tungstenite::client::IntoClientRequest;
use tokio_tungstenite::tungstenite::http::uri::Scheme;
use tokio_tungstenite::tungstenite::http::Request;
use tokio_tungstenite::tungstenite::Message;
use tokio_tungstenite::{client_async_tls_with_config, client_async_with_config, WebSocketStream};
use url::Url;

pub type MaybeTlsStream<S> = StreamSwitcher<S, TlsStream<S>>;

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
        .replace(&Scheme::from_str(new_scheme)?);

    Ok(())
}

pub async fn connect(
    url: Url,
    config: &mut WebSocketConfig,
) -> Result<(WebSocketStream<MaybeTlsStream<TcpStream>>, Response<()>), WebSocketError> {
    let uri: Uri = url.as_str().parse()?;
    let mut request = Request::get(uri).body(())?;
    config
        .extensions
        .iter_mut()
        .for_each(|mut e| e.on_request(&mut request));

    let port = request
        .uri()
        .port_u16()
        .or_else(|| match request.uri().scheme_str() {
            Some("wss") => Some(443),
            Some("ws") => Some(80),
            _ => None,
        })
        .ok_or_else(|| WebSocketError::Url(String::from(UNSUPPORTED_SCHEME)))?;

    maybe_normalise_url(&request)?;

    let domain = match request.uri().host() {
        Some(d) => d.to_string(),
        None => {
            return Err(WebSocketError::Url(String::from(
                "Malformatted URI. Missing host",
            )))
        }
    };

    let host = format!("{}:{}", domain, port);
    let stream = build_stream(&host, domain, config).await?;
    let res = client_async_with_config(request, stream, None)
        .await
        .map_err(|_| WebSocketError::Protocol);

    match res {
        Ok(r) => {
            let (stream, mut response) = r;

            config
                .extensions
                .iter_mut()
                .for_each(|mut e| e.on_response(&mut response));

            Ok((stream, response))
        }
        Err(e) => Err(e),
    }
}

async fn build_stream(
    host: &str,
    domain: String,
    config: &WebSocketConfig,
) -> Result<MaybeTlsStream<TcpStream>, WebSocketError> {
    let stream = TcpStream::connect(host)
        .await
        .map_err(|e| WebSocketError::Message(e.to_string()))?;

    match &config.stream_type {
        StreamType::Plain => Ok(StreamSwitcher::Plain(stream)),
        StreamType::Tls(tls_config) => {
            let mut tls_conn_builder = TlsConnector::builder();

            // todo: make config obj
            if let Some(path) = tls_config {
                let mut reader = BufReader::new(
                    File::open(path).map_err(|e| WebSocketError::Tls(e.to_string()))?,
                );

                let mut buf = vec![];
                reader.read_to_end(&mut buf);

                match Certificate::from_pem(&buf) {
                    Ok(cert) => {
                        tls_conn_builder.add_root_certificate(cert);
                    }
                    Err(e) => return Err(WebSocketError::Tls(e.to_string())),
                }
            }

            let connector = tls_conn_builder
                .build()
                .map_err(|e| WebSocketError::Tls(e.to_string()))?;
            let connector = TokioTlsConnector::from(connector);
            let connected = connector.connect(&domain, stream).await;

            match connected {
                Ok(s) => Ok(StreamSwitcher::Tls(s)),
                Err(e) => Err(WebSocketError::Tls(e.to_string())),
            }
        }
    }
}

#[tokio::test]
async fn c() {
    let mut config = WebSocketConfig {
        stream_type: StreamType::Tls(None),
        extensions: vec![],
    };

    let r = connect(
        Url::from_str("wss://echo.websocket.org").unwrap(),
        &mut config,
    )
    .await;

    match r {
        Ok(sock) => {
            let (stream, response) = sock;
            println!("{:?}", response);

            let (mut sink, mut stream) = stream.split();

            let r = sink.send(Message::text("helloooooooooo")).await;
            println!("{:?}", r);

            while let Some(msg) = stream.next().await {
                println!("{:?}", msg);
            }
        }
        Err(e) => println!("{}", e.to_string()),
    }
}
