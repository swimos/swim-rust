// Copyright 2015-2021 SWIM.AI inc.
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

use crate::connections::factory::tungstenite::MaybeTlsStream;
use http::Request;
use swim_runtime::error::{
    ConnectionError, InvalidUriError, InvalidUriErrorKind, TError, TungsteniteError,
};
use swim_runtime::ws::tls::connect_tls;
use swim_runtime::ws::{Protocol, WsMessage};
use swim_utilities::future::TransformMut;
use tokio::net::TcpStream;
use tokio_tungstenite::stream::Stream as StreamSwitcher;
use tokio_tungstenite::tungstenite::Message;

pub fn get_stream_type<T>(
    request: &Request<T>,
    protocol: &Protocol,
) -> Result<Protocol, InvalidUriError> {
    match request.uri().scheme_str() {
        Some("ws") => Ok(Protocol::PlainText),
        Some("wss") => match protocol {
            Protocol::PlainText => Err(InvalidUriError::new(
                InvalidUriErrorKind::InvalidScheme,
                Some(
                    "Attempted to connect to a secure WebSocket without a TLS configuration".into(),
                ),
            )),
            tls => Ok(tls.clone()),
        },
        Some(s) => Err(InvalidUriError::new(
            InvalidUriErrorKind::UnsupportedScheme,
            Some(s.into()),
        )),
        None => Err(InvalidUriError::new(
            InvalidUriErrorKind::MissingScheme,
            None,
        )),
    }
}

pub async fn build_stream(
    host: &str,
    stream_type: Protocol,
) -> Result<MaybeTlsStream<TcpStream>, ConnectionError> {
    let socket = TcpStream::connect(host)
        .await
        .map_err(|e| ConnectionError::Io(e.into()))?;

    match stream_type {
        Protocol::PlainText => Ok(StreamSwitcher::Plain(socket)),
        Protocol::Tls(certificate) => match connect_tls(host, certificate).await {
            Ok(s) => Ok(StreamSwitcher::Tls(s)),
            Err(e) => Err(e.into()),
        },
    }
}

pub struct SinkTransformer;

impl TransformMut<WsMessage> for SinkTransformer {
    type Out = Message;

    fn transform(&mut self, input: WsMessage) -> Self::Out {
        input.into()
    }
}

pub struct StreamTransformer;

impl TransformMut<Result<Message, TError>> for StreamTransformer {
    type Out = Result<WsMessage, ConnectionError>;

    fn transform(&mut self, input: Result<Message, TError>) -> Self::Out {
        match input {
            Ok(r) => Ok(r.into()),
            Err(e) => Err(TungsteniteError(e).into()),
        }
    }
}
