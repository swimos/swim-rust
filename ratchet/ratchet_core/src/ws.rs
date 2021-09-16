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

use crate::errors::{CloseError, Error, ErrorKind, ProtocolError};
use crate::framed::{FramedIo, Item};
use crate::handshake::{exec_client_handshake, HandshakeResult, ProtocolRegistry};
use crate::protocol::{
    CloseCode, CloseReason, ControlCode, DataCode, HeaderFlags, Message, MessageType, OpCode,
    PayloadType, Role,
};
use crate::split::{split, Receiver, Sender};
use crate::{Request, WebSocketConfig, WebSocketStream};
use bytes::BytesMut;
use ratchet_ext::{Extension, ExtensionProvider, SplittableExtension};

pub const CONTROL_MAX_SIZE: usize = 125;
pub const CONTROL_DATA_MISMATCH: &str = "Unexpected control frame data";

pub struct WebSocket<S, E> {
    framed: FramedIo<S>,
    control_buffer: BytesMut,
    _extension: E,
    closed: bool,
}

impl<S, E> WebSocket<S, E>
where
    S: WebSocketStream,
    E: Extension,
{
    pub(crate) fn from_parts(
        framed: FramedIo<S>,
        control_buffer: BytesMut,
        _extension: E,
        closed: bool,
    ) -> WebSocket<S, E> {
        WebSocket {
            framed,
            control_buffer,
            _extension,
            closed,
        }
    }

    pub fn from_upgraded(
        config: WebSocketConfig,
        stream: S,
        extension: E,
        read_buffer: BytesMut,
        role: Role,
    ) -> WebSocket<S, E> {
        let WebSocketConfig { max_size } = config;
        WebSocket {
            framed: FramedIo::new(stream, read_buffer, role, max_size),
            _extension: extension,
            control_buffer: BytesMut::with_capacity(CONTROL_MAX_SIZE),
            closed: false,
        }
    }

    pub fn role(&self) -> Role {
        if self.framed.is_server() {
            Role::Server
        } else {
            Role::Client
        }
    }

    pub async fn read(&mut self, read_buffer: &mut BytesMut) -> Result<Message, Error> {
        let WebSocket {
            framed,
            closed,
            control_buffer,
            ..
        } = self;

        if *closed {
            return Err(Error::with_cause(ErrorKind::Close, CloseError::Closed));
        }

        loop {
            match framed.read_next(read_buffer).await {
                Ok(item) => match item {
                    Item::Binary => return Ok(Message::Binary),
                    Item::Text => return Ok(Message::Text),
                    Item::Ping(payload) => {
                        framed
                            .write(
                                OpCode::ControlCode(ControlCode::Pong),
                                HeaderFlags::FIN,
                                payload,
                            )
                            .await?;
                        return Ok(Message::Ping);
                    }
                    Item::Pong(payload) => {
                        if control_buffer.is_empty() {
                            continue;
                        } else {
                            return if control_buffer[..].eq(&payload[..]) {
                                control_buffer.clear();
                                Ok(Message::Pong)
                            } else {
                                self.closed = true;
                                self.framed
                                    .write_close(CloseReason {
                                        code: CloseCode::Protocol,
                                        description: Some(CONTROL_DATA_MISMATCH.to_string()),
                                    })
                                    .await?;

                                return Err(Error::with_cause(
                                    ErrorKind::Protocol,
                                    CONTROL_DATA_MISMATCH.to_string(),
                                ));
                            };
                        }
                    }
                    Item::Close(reason) => {
                        *closed = true;
                        return match reason {
                            Some(reason) => {
                                framed.write_close(reason.clone()).await?;
                                Ok(Message::Close(Some(reason)))
                            }
                            None => {
                                framed
                                    .write(
                                        OpCode::ControlCode(ControlCode::Close),
                                        HeaderFlags::FIN,
                                        &mut [],
                                    )
                                    .await?;
                                Ok(Message::Close(None))
                            }
                        };
                    }
                },
                Err(e) => {
                    self.closed = true;

                    if !e.is_io() {
                        let reason = CloseReason::new(CloseCode::Protocol, Some(e.to_string()));
                        self.framed.write_close(reason).await?;
                    }

                    return Err(e);
                }
            }
        }
    }

    pub async fn write(
        &mut self,
        buf: &mut BytesMut,
        message_type: PayloadType,
    ) -> Result<(), Error> {
        if self.closed {
            return Err(Error::with_cause(ErrorKind::Close, CloseError::Closed));
        }

        let op_code = match message_type {
            PayloadType::Text => OpCode::DataCode(DataCode::Text),
            PayloadType::Binary => OpCode::DataCode(DataCode::Binary),
            PayloadType::Ping => {
                if buf.len() > CONTROL_MAX_SIZE {
                    return Err(Error::with_cause(
                        ErrorKind::Protocol,
                        ProtocolError::FrameOverflow,
                    ));
                } else {
                    self.control_buffer.clear();
                    self.control_buffer
                        .clone_from_slice(&buf[..CONTROL_MAX_SIZE]);
                    OpCode::ControlCode(ControlCode::Ping)
                }
            }
        };

        match self.framed.write(op_code, HeaderFlags::FIN, buf).await {
            Ok(()) => Ok(()),
            Err(e) => {
                self.closed = true;
                Err(e.into())
            }
        }
    }

    pub async fn close(mut self, reason: Option<String>) -> Result<(), Error> {
        self.framed
            .write_close(CloseReason::new(CloseCode::Normal, reason))
            .await
    }

    pub async fn write_fragmented(
        &mut self,
        buf: &mut BytesMut,
        message_type: MessageType,
        fragment_size: usize,
    ) -> Result<(), Error> {
        if self.closed {
            return Err(Error::with_cause(ErrorKind::Close, CloseError::Closed));
        }

        self.framed
            .write_fragmented(buf, message_type, fragment_size)
            .await
    }

    pub fn is_closed(&self) -> bool {
        self.closed
    }

    // todo add docs about:
    //  - https://github.com/tokio-rs/tokio/issues/3200
    //  - https://github.com/tokio-rs/tls/issues/40
    pub fn split(self) -> Result<(Sender<S, E::Encoder>, Receiver<S, E::Decoder>), Error>
    where
        E: SplittableExtension,
    {
        if self.is_closed() {
            return Err(Error::with_cause(ErrorKind::Close, CloseError::Closed));
        } else {
            let WebSocket {
                framed,
                control_buffer,
                _extension,
                ..
            } = self;
            Ok(split(framed, control_buffer, _extension))
        }
    }
}

pub struct Upgraded<S, E> {
    pub socket: WebSocket<S, E>,
    pub subprotocol: Option<String>,
}

pub async fn client<S, E>(
    config: WebSocketConfig,
    mut stream: S,
    request: Request,
    extension: E,
    subprotocols: ProtocolRegistry,
) -> Result<Upgraded<S, E::Extension>, Error>
where
    S: WebSocketStream,
    E: ExtensionProvider,
{
    let mut read_buffer = BytesMut::new();
    let HandshakeResult {
        subprotocol,
        extension,
    } = exec_client_handshake(
        &mut stream,
        request,
        extension,
        subprotocols,
        &mut read_buffer,
    )
    .await?;

    Ok(Upgraded {
        socket: WebSocket::from_upgraded(config, stream, extension, read_buffer, Role::Client),
        subprotocol,
    })
}
