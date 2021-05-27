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

use std::net::SocketAddr;
use std::pin::Pin;

use crate::routing::remote::net::dns::{DnsResolver, Resolver};
use crate::routing::remote::net::plain::TokioPlainTextNetworking;
use crate::routing::remote::table::SchemeHostPort;
use crate::routing::remote::{ExternalConnections, IoResult, Listener, Scheme, SchemeSocketAddr};
use either::Either;
use futures::stream::{Fuse, StreamExt};
use futures::task::{Context, Poll};
use futures::FutureExt;
use futures::Stream;
use futures_util::future::BoxFuture;
use std::io;
use std::path::PathBuf;
use std::sync::Arc;
use tokio::net::{TcpListener, TcpStream};
use url::Url;

pub mod dns;
pub mod plain;
#[cfg(feature = "tokio_native_tls")]
pub mod tls;
