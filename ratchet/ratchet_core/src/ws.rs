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
use crate::ext::NegotiatedExtension;
use crate::framed::{FramedIo, Item};
use crate::protocol::{
    CloseCode, CloseReason, ControlCode, DataCode, HeaderFlags, Message, MessageType, OpCode,
    PayloadType, Role,
};
use crate::{WebSocketConfig, WebSocketStream};
use bytes::BytesMut;
use log::{error, trace};
use ratchet_ext::{Extension, ExtensionEncoder, FrameHeader as ExtFrameHeader};

#[cfg(feature = "split")]
use crate::split::{split, Receiver, Sender};
#[cfg(feature = "split")]
use ratchet_ext::SplittableExtension;

pub const CONTROL_MAX_SIZE: usize = 125;
pub const CONTROL_DATA_MISMATCH: &str = "Unexpected control frame data";

#[cfg(feature = "split")]
type SplitSocket<S, E> = (
    Sender<S, <E as SplittableExtension>::SplitEncoder>,
    Receiver<S, <E as SplittableExtension>::SplitDecoder>,
);

/// A WebSocket stream.
#[derive(Debug)]
pub struct WebSocket<S, E> {
    framed: FramedIo<S>,
    control_buffer: BytesMut,
    extension: NegotiatedExtension<E>,
    closed: bool,
}

impl<S, E> WebSocket<S, E>
where
    S: WebSocketStream,
    E: Extension,
{
    #[cfg(feature = "split")]
    pub(crate) fn from_parts(
        framed: FramedIo<S>,
        control_buffer: BytesMut,
        extension: NegotiatedExtension<E>,
        closed: bool,
    ) -> WebSocket<S, E> {
        WebSocket {
            framed,
            control_buffer,
            extension,
            closed,
        }
    }

    /// Initialise a new `WebSocket` from a stream that has already executed a handshake.
    ///
    /// # Arguments
    /// `config` - The configuration to initialise the WebSocket with.
    /// `stream` - The stream that the handshake was executed on.
    /// `extension` - A negotiated extension that will be used for the session.
    /// `read_buffer` - The read buffer which will be used for the session. This **may** contain any
    /// unread data received after performing the handshake that was not required.
    /// `role` - The role that this WebSocket will take.
    pub fn from_upgraded(
        config: WebSocketConfig,
        stream: S,
        extension: NegotiatedExtension<E>,
        read_buffer: BytesMut,
        role: Role,
    ) -> WebSocket<S, E> {
        let WebSocketConfig { max_size } = config;
        WebSocket {
            framed: FramedIo::new(stream, read_buffer, role, max_size, extension.bits().into()),
            extension,
            control_buffer: BytesMut::with_capacity(CONTROL_MAX_SIZE),
            closed: false,
        }
    }

    /// Returns the role of this WebSocket.
    pub fn role(&self) -> Role {
        if self.framed.is_server() {
            Role::Server
        } else {
            Role::Client
        }
    }

    /// Attempt to read some data from the WebSocket. Returning either the type of the message
    /// received or the error that was produced.
    ///
    /// # Errors
    /// If an error is produced during a read operation the contents of `read_buffer` must be
    /// considered to be dirty.
    ///
    /// # Note
    /// Ratchet transparently handles ping messages received from the peer by returning a pong frame
    /// and this function will return `Message::Pong` if one has been received. As per [RFC6455](https://datatracker.ietf.org/doc/html/rfc6455)
    /// these may be interleaved between data frames. In the event of one being received while
    /// reading a continuation, this function will then yield `Message::Ping` and the `read_buffer`
    /// will contain the data received up to that point. The callee must ensure that the contents
    /// of `read_buffer` are **not** then modified before calling `read` again.
    pub async fn read(&mut self, read_buffer: &mut BytesMut) -> Result<Message, Error> {
        let WebSocket {
            framed,
            closed,
            control_buffer,
            extension,
            ..
        } = self;

        if *closed {
            return Err(Error::with_cause(ErrorKind::Close, CloseError));
        }

        loop {
            match framed.read_next(read_buffer, extension).await {
                Ok(item) => match item {
                    Item::Binary => return Ok(Message::Binary),
                    Item::Text => return Ok(Message::Text),
                    Item::Ping(payload) => {
                        trace!("Received a ping frame. Responding with pong");
                        framed
                            .write(
                                OpCode::ControlCode(ControlCode::Pong),
                                HeaderFlags::FIN,
                                payload,
                                |_, _| Ok(()),
                            )
                            .await?;
                        return Ok(Message::Ping);
                    }
                    Item::Pong(payload) => {
                        if control_buffer.is_empty() {
                            trace!("Received an unsolicited pong frame. Ignoring");
                            continue;
                        } else {
                            return if control_buffer[..].eq(&payload[..]) {
                                control_buffer.clear();
                                trace!("Received pong frame");
                                Ok(Message::Pong)
                            } else {
                                trace!("Received a pong frame with an incorrect payload. Closing the connection");
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
                                        |_, _| Ok(()),
                                    )
                                    .await?;
                                Ok(Message::Close(None))
                            }
                        };
                    }
                },
                Err(e) => {
                    error!("WebSocket read failure: {:?}", e);
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

    /// Constructs a new text WebSocket message with a payload of `data`.
    pub async fn write_text<I>(&mut self, data: I) -> Result<(), Error>
    where
        I: AsRef<str>,
    {
        self.write(data.as_ref(), PayloadType::Text).await
    }

    /// Constructs a new binary WebSocket message with a payload of `data`.
    pub async fn write_binary<I>(&mut self, data: I) -> Result<(), Error>
    where
        I: AsRef<[u8]>,
    {
        self.write(data.as_ref(), PayloadType::Binary).await
    }

    /// Constructs a new ping WebSocket message with a payload of `data`.
    pub async fn write_ping<I>(&mut self, data: I) -> Result<(), Error>
    where
        I: AsRef<[u8]>,
    {
        self.write(data.as_ref(), PayloadType::Ping).await
    }

    /// Constructs a new WebSocket message of `message_type` and with a payload of `buf.
    pub async fn write<A>(&mut self, buf: A, message_type: PayloadType) -> Result<(), Error>
    where
        A: AsRef<[u8]>,
    {
        let buf = buf.as_ref();
        if self.closed {
            return Err(Error::with_cause(ErrorKind::Close, CloseError));
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

        let encoder = &mut self.extension;
        match self
            .framed
            .write(op_code, HeaderFlags::FIN, buf, |payload, header| {
                extension_encode(encoder, payload, header)
            })
            .await
        {
            Ok(()) => Ok(()),
            Err(e) => {
                self.closed = true;
                Err(e)
            }
        }
    }

    /// Close this WebSocket with the reason provided.
    pub async fn close(&mut self, reason: Option<String>) -> Result<(), Error> {
        self.closed = true;
        self.framed
            .write_close(CloseReason::new(CloseCode::Normal, reason))
            .await
    }

    /// Close this WebSocket with the reason provided.
    pub async fn close_with(&mut self, reason: CloseReason) -> Result<(), Error> {
        self.closed = true;
        self.framed.write_close(reason).await
    }

    /// Constructs a new WebSocket message of `message_type` and with a payload of `buf_ref` and
    /// chunked by `fragment_size`. If the length of the buffer is less than the chunk size then
    /// only a single message is sent.
    pub async fn write_fragmented<A>(
        &mut self,
        buf: A,
        message_type: MessageType,
        fragment_size: usize,
    ) -> Result<(), Error>
    where
        A: AsRef<[u8]>,
    {
        if self.closed {
            return Err(Error::with_cause(ErrorKind::Close, CloseError));
        }
        let encoder = &mut self.extension;
        self.framed
            .write_fragmented(buf, message_type, fragment_size, |payload, header| {
                extension_encode(encoder, payload, header)
            })
            .await
    }

    /// Returns whether this WebSocket is closed.
    pub fn is_closed(&self) -> bool {
        self.closed
    }

    /// Attempt to split the `WebSocket` into its sender and receiver halves.
    ///
    /// # Note
    /// This function does **not** split the IO. It, instead, places the IO into a `BiLock` and
    /// requires exclusive access to it in order to perform any read or write operations.
    ///
    /// In addition to this, the internal framed writer is placed into a `BiLock` so
    /// the receiver half can transparently handle control frames that may be received.
    ///
    /// See: [Tokio#3200](https://github.com/tokio-rs/tokio/issues/3200) and [Tokio#40](https://github.com/tokio-rs/tls/issues/40)
    ///
    /// # Errors
    /// This function will only error if the `WebSocket` is already closed.
    #[cfg(feature = "split")]
    pub fn split(self) -> Result<SplitSocket<S, E>, Error>
    where
        E: SplittableExtension,
    {
        if self.is_closed() {
            Err(Error::with_cause(ErrorKind::Close, CloseError))
        } else {
            let WebSocket {
                framed,
                control_buffer,
                extension,
                ..
            } = self;
            Ok(split(framed, control_buffer, extension))
        }
    }
}

pub fn extension_encode<E>(
    extension: &mut E,
    buf: &mut BytesMut,
    header: &mut ExtFrameHeader,
) -> Result<(), Error>
where
    E: ExtensionEncoder,
{
    extension
        .encode(buf, header)
        .map_err(|e| Error::with_cause(ErrorKind::Extension, e))
}
