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
use http::uri::Scheme;
use http::{Request, Response, Uri};
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
use tokio_tungstenite::tungstenite::Message;
use tokio_tungstenite::{client_async_tls_with_config, client_async_with_config, WebSocketStream};
use url::Url;

pub type MaybeTlsStream<S> = StreamSwitcher<S, TlsStream<S>>;
