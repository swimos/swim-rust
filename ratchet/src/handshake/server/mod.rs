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

use crate::errors::Error;
use crate::handshake::io::BufferedIo;
use crate::{WebSocketConfig, WebSocketStream};
use bytes::BytesMut;
use tokio::io::{AsyncRead, AsyncWrite};
use tokio_native_tls::TlsConnector;

pub async fn exec_client_handshake<S>(
    _config: &WebSocketConfig,
    stream: &mut S,
    _connector: Option<TlsConnector>,
) -> Result<(), Error>
where
    S: AsyncRead + AsyncWrite + Unpin,
{
    let machine = HandshakeMachine::new(stream, Vec::new(), Vec::new());
    machine.exec().await
}

struct HandshakeMachine<'s, S> {
    buffered: BufferedIo<'s, S>,
    subprotocols: Vec<&'static str>,
    extensions: Vec<&'static str>,
}

impl<'s, S> HandshakeMachine<'s, S>
where
    S: AsyncRead + AsyncWrite + Unpin,
{
    pub fn new(
        socket: &'s mut S,
        subprotocols: Vec<&'static str>,
        extensions: Vec<&'static str>,
    ) -> HandshakeMachine<'s, S> {
        HandshakeMachine {
            buffered: BufferedIo::new(socket, BytesMut::new()),
            subprotocols,
            extensions,
        }
    }

    pub async fn exec(self) -> Result<(), Error> {
        unimplemented!()
    }
}
