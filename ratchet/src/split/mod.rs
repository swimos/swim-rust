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

use crate::codec::Codec;
use crate::errors::Error;
use crate::handshake::{exec_client_handshake, HandshakeResult, ProtocolRegistry};
use crate::{
    Deflate, Extension, ExtensionProvider, Request, Role, WebSocketConfig, WebSocketStream,
};
use futures::io::{ReadHalf, WriteHalf};
use futures::AsyncReadExt;
use std::sync::Arc;
use tokio::sync::Mutex;
use tokio_util::compat::{Compat, TokioAsyncReadCompatExt};

// todo replace once futures::BiLock is stabilised
//  https://github.com/rust-lang/futures-rs/issues/2289
type Writer<S> = Arc<Mutex<WriteHalf<Compat<S>>>>;

type SplitSocket<S, E> = (WebSocketTx<S, E>, WebSocketRx<S, E>);

// todo maybe split extension
// todo owned decoder and split encoder
pub struct WebSocketRx<S, E = Deflate> {
    writer: Writer<S>,
    codec: Codec,
    reader: ReadHalf<Compat<S>>,
    role: Role,
    extension: E,
    config: WebSocketConfig,
}

// todo split encoder
pub struct WebSocketTx<S, E = Deflate> {
    writer: Writer<S>,
    codec: Codec,
    role: Role,
    extension: E,
    config: WebSocketConfig,
}

pub async fn client<S, E>(
    config: WebSocketConfig,
    mut stream: S,
    request: Request,
    codec: Codec,
    extension: E,
    subprotocols: ProtocolRegistry,
) -> Result<(SplitSocket<S, E::Extension>, Option<String>), Error>
where
    S: WebSocketStream,
    E: ExtensionProvider,
{
    let HandshakeResult {
        protocol,
        extension,
        io_buf,
    } = exec_client_handshake(&mut stream, request, extension, subprotocols).await?;

    let (read_half, write_half) = stream.compat().split();
    let writer = Arc::new(Mutex::new(write_half));
    let tx = WebSocketTx {
        writer: writer.clone(),
        codec: codec.clone(),
        role: Role::Client,
        extension: extension.clone(),
        config: config.clone(),
    };

    let rx = WebSocketRx {
        writer,
        codec,
        reader: read_half,
        role: Role::Client,
        extension,
        config,
    };

    Ok(((tx, rx), protocol))
}

impl<S, E> WebSocketTx<S, E>
where
    S: WebSocketStream,
    E: Extension,
{
    pub async fn read_frame_contents(&mut self, _bytes: &mut [u8]) -> Result<usize, Error> {
        let mut guard = self.writer.lock().await;
        let _contents = &mut (*guard);
        unimplemented!()
    }
}
