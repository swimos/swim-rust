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
use ratchet_ext::{Extension, ExtensionEncoder, FrameHeader as ExtFrameHeader};

#[cfg(feature = "split")]
use crate::split::{split, Receiver, Sender};
#[cfg(feature = "split")]
use ratchet_ext::SplittableExtension;
use std::ops::Index;
use std::slice::SliceIndex;

pub const CONTROL_MAX_SIZE: usize = 125;
pub const CONTROL_DATA_MISMATCH: &str = "Unexpected control frame data";

#[cfg(feature = "split")]
type SplitSocket<S, E> = (
    Sender<S, <E as SplittableExtension>::SplitEncoder>,
    Receiver<S, <E as SplittableExtension>::SplitDecoder>,
);

pub enum Data<'l> {
    Owned(BytesMut),
    Mutable(&'l mut [u8]),
    Borrowed(&'l [u8]),
}

impl<'l, I> Index<I> for Data<'l>
where
    I: SliceIndex<[u8], Output = [u8]>,
{
    type Output = I::Output;

    #[inline]
    fn index(&self, index: I) -> &Self::Output {
        match self {
            Data::Owned(data) => &data[index],
            Data::Mutable(data) => &data[index],
            Data::Borrowed(data) => &data[index],
        }
    }
}

impl<'l> Data<'l> {
    fn len(&self) -> usize {
        match self {
            Data::Owned(data) => data.len(),
            Data::Mutable(data) => data.len(),
            Data::Borrowed(data) => data.len(),
        }
    }
}

impl<'l> AsRef<[u8]> for Data<'l> {
    fn as_ref(&self) -> &[u8] {
        match self {
            Data::Owned(data) => data.as_ref(),
            Data::Mutable(data) => data,
            Data::Borrowed(data) => data,
        }
    }
}

impl<'l> From<&'l [u8]> for Data<'l> {
    fn from(slice: &'l [u8]) -> Self {
        Data::Borrowed(slice)
    }
}

impl<'l> From<&'l mut [u8]> for Data<'l> {
    fn from(slice: &'l mut [u8]) -> Self {
        Data::Mutable(slice)
    }
}

impl<'l> From<BytesMut> for Data<'l> {
    fn from(bytes: BytesMut) -> Self {
        Data::Owned(bytes)
    }
}

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
                                        |_, _| Ok(()),
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

    /// Constructs a new WebSocket message of `message_type` and with a payload of `buf_ref.
    ///
    /// `buf_ref` must be a mutable byte slice as it will be mutated if masking is required.
    pub async fn write(&mut self, buf: Data<'_>, message_type: PayloadType) -> Result<(), Error> {
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
    pub async fn close(mut self, reason: Option<String>) -> Result<(), Error> {
        self.framed
            .write_close(CloseReason::new(CloseCode::Normal, reason))
            .await
    }

    /// Constructs a new WebSocket message of `message_type` and with a payload of `buf_ref` and
    /// chunked by `fragment_size`. If the length of the buffer is less than the chunk size then
    /// only a single message is sent. The buffer must be a mutable byte slice as it will be
    /// mutated if masking is required.
    pub async fn write_fragmented<A>(
        &mut self,
        buf: A,
        message_type: MessageType,
        fragment_size: usize,
    ) -> Result<(), Error>
    where
        A: AsMut<[u8]>,
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
