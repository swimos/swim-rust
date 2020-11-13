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

use crate::connections::factory::tungstenite::{CompressionConfig, MaybeTlsStream, TError};
use http::{Request, Response};
use native_tls::TlsConnector;
use std::borrow::Cow;
use std::error::Error;
use std::fmt::{Display, Formatter};
use swim_common::ws::error::{ConnectionError, WebSocketError};
use swim_common::ws::{Protocol, WsMessage};
use tokio::net::TcpStream;
use tokio_native_tls::TlsConnector as TokioTlsConnector;
use tokio_tungstenite::stream::Stream as StreamSwitcher;
use tokio_tungstenite::tungstenite::extensions::deflate::DeflateExt;
use tokio_tungstenite::tungstenite::extensions::uncompressed::UncompressedExt;
use tokio_tungstenite::tungstenite::extensions::WebSocketExtension;
use tokio_tungstenite::tungstenite::protocol::frame::Frame;
use tokio_tungstenite::tungstenite::Message;
use utilities::future::TransformMut;

/// A Tungstenite WebSocket extension that is either `DeflateExt` or `UncompressedExt`.
pub enum CompressionSwitcher {
    Compressed(DeflateExt),
    Uncompressed(UncompressedExt),
}

const MAX_MESSAGE_SIZE: usize = 64 << 20;

impl CompressionSwitcher {
    pub fn from_config(config: CompressionConfig) -> CompressionSwitcher {
        match config {
            CompressionConfig::Uncompressed => {
                CompressionSwitcher::Uncompressed(UncompressedExt::new(Some(MAX_MESSAGE_SIZE)))
            }
            CompressionConfig::Deflate(config) => {
                CompressionSwitcher::Compressed(DeflateExt::new(config))
            }
        }
    }
}

impl Default for CompressionSwitcher {
    fn default() -> Self {
        CompressionSwitcher::Uncompressed(UncompressedExt::default())
    }
}

#[derive(Debug)]
pub struct CompressionError(String);

impl Error for CompressionError {}

impl From<CompressionError> for TError {
    fn from(e: CompressionError) -> Self {
        TError::ExtensionError(Cow::from(e.to_string()))
    }
}

impl Display for CompressionError {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("CompressionError")
            .field("error", &self.0)
            .finish()
    }
}

impl WebSocketExtension for CompressionSwitcher {
    type Error = CompressionError;

    fn new(max_message_size: Option<usize>) -> Self {
        CompressionSwitcher::Uncompressed(UncompressedExt::new(max_message_size))
    }

    fn enabled(&self) -> bool {
        match self {
            CompressionSwitcher::Uncompressed(ext) => ext.enabled(),
            CompressionSwitcher::Compressed(ext) => ext.enabled(),
        }
    }

    fn on_make_request<T>(&mut self, request: Request<T>) -> Request<T> {
        match self {
            CompressionSwitcher::Uncompressed(ext) => ext.on_make_request(request),
            CompressionSwitcher::Compressed(ext) => ext.on_make_request(request),
        }
    }

    fn on_receive_request<T>(
        &mut self,
        request: &Request<T>,
        response: &mut Response<T>,
    ) -> Result<(), Self::Error> {
        match self {
            CompressionSwitcher::Uncompressed(ext) => ext
                .on_receive_request(request, response)
                .map_err(|e| CompressionError(e.to_string())),
            CompressionSwitcher::Compressed(ext) => ext
                .on_receive_request(request, response)
                .map_err(|e| CompressionError(e.to_string())),
        }
    }

    fn on_response<T>(&mut self, response: &Response<T>) -> Result<(), Self::Error> {
        match self {
            CompressionSwitcher::Uncompressed(ext) => ext
                .on_response(response)
                .map_err(|e| CompressionError(e.to_string())),
            CompressionSwitcher::Compressed(ext) => ext
                .on_response(response)
                .map_err(|e| CompressionError(e.to_string())),
        }
    }

    fn on_send_frame(&mut self, frame: Frame) -> Result<Frame, Self::Error> {
        match self {
            CompressionSwitcher::Uncompressed(ext) => ext
                .on_send_frame(frame)
                .map_err(|e| CompressionError(e.to_string())),
            CompressionSwitcher::Compressed(ext) => ext
                .on_send_frame(frame)
                .map_err(|e| CompressionError(e.to_string())),
        }
    }

    fn on_receive_frame(&mut self, frame: Frame) -> Result<Option<Message>, Self::Error> {
        match self {
            CompressionSwitcher::Uncompressed(ext) => ext
                .on_receive_frame(frame)
                .map_err(|e| CompressionError(e.to_string())),
            CompressionSwitcher::Compressed(ext) => ext
                .on_receive_frame(frame)
                .map_err(|e| CompressionError(e.to_string())),
        }
    }
}

pub fn get_stream_type<T>(
    request: &Request<T>,
    protocol: &Protocol,
) -> Result<Protocol, WebSocketError> {
    match request.uri().scheme_str() {
        Some("ws") => Ok(Protocol::PlainText),
        Some("wss") => match protocol {
            Protocol::PlainText => Err(WebSocketError::BadConfiguration(
                "Attempted to connect to a secure WebSocket without a TLS configuration".into(),
            )),
            tls => Ok(tls.clone()),
        },
        Some(s) => Err(WebSocketError::unsupported_scheme(s)),
        None => Err(WebSocketError::missing_scheme()),
    }
}

pub async fn build_stream(
    host: &str,
    domain: String,
    stream_type: Protocol,
) -> Result<MaybeTlsStream<TcpStream>, WebSocketError> {
    let socket = TcpStream::connect(host)
        .await
        .map_err(|e| WebSocketError::Message(e.to_string()))?;

    match stream_type {
        Protocol::PlainText => Ok(StreamSwitcher::Plain(socket)),
        Protocol::Tls(certificate) => {
            let mut tls_conn_builder = TlsConnector::builder();
            tls_conn_builder.add_root_certificate(certificate);

            let connector = tls_conn_builder.build()?;
            let stream = TokioTlsConnector::from(connector);
            let connected = stream.connect(&domain, socket).await;

            match connected {
                Ok(s) => Ok(StreamSwitcher::Tls(s)),
                Err(e) => Err(WebSocketError::Tls(e.to_string())),
            }
        }
    }
}

pub struct SinkTransformer;
impl TransformMut<WsMessage> for SinkTransformer {
    type Out = Message;

    fn transform(&mut self, input: WsMessage) -> Self::Out {
        match input {
            WsMessage::Text(s) => Message::Text(s),
            WsMessage::Binary(v) => Message::Binary(v),
        }
    }
}

pub struct StreamTransformer;
impl TransformMut<Result<Message, TError>> for StreamTransformer {
    type Out = Result<WsMessage, ConnectionError>;

    fn transform(&mut self, input: Result<Message, TError>) -> Self::Out {
        match input {
            Ok(i) => match i {
                Message::Text(s) => Ok(WsMessage::Text(s)),
                Message::Binary(v) => Ok(WsMessage::Binary(v)),
                _ => Err(ConnectionError::ReceiveMessageError),
            },
            Err(_) => Err(ConnectionError::ConnectError),
        }
    }
}
