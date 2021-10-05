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

#[cfg(test)]
mod tests;
mod bilock;

use crate::ext::NegotiatedExtension;

use crate::extensions::{
    ExtensionDecoder, ExtensionEncoder, ReunitableExtension, SplittableExtension,
};
use crate::framed::{
    read_next, write_close, write_fragmented, CodecFlags, FramedIoParts, FramedRead, FramedWrite,
    Item,
};
use crate::protocol::{
    CloseCode, CloseReason, ControlCode, DataCode, HeaderFlags, MessageType, OpCode,
};
use crate::ws::{extension_encode, CONTROL_DATA_MISMATCH, CONTROL_MAX_SIZE};
use crate::{
    framed, CloseError, Error, ErrorKind, Message, PayloadType, ProtocolError, Role, WebSocket,
    WebSocketStream,
};
use bilock::{bilock, BiLock};
use bitflags::_core::sync::atomic::Ordering;
use bytes::BytesMut;
use ratchet_ext::{ExtensionDecoder, ExtensionEncoder, ReunitableExtension, SplittableExtension};
use std::fmt::Debug;
use std::sync::atomic::AtomicBool;
use std::sync::Arc;

// todo sink and stream implementations
pub fn split<S, E>(
    framed: framed::FramedIo<S>,
    control_buffer: BytesMut,
    extension: NegotiatedExtension<E>,
) -> (Sender<S, E::SplitEncoder>, Receiver<S, E::SplitDecoder>)
where
    S: WebSocketStream,
    E: SplittableExtension,
{
    let FramedIoParts {
        io,
        reader,
        writer,
        flags,
        max_size,
    } = framed.into_parts();

    let closed = Arc::new(AtomicBool::new(false));
    let (read_half, write_half) = bilock(io);
    let (sender_writer, reader_writer) = bilock(WriteHalf {
        control_buffer,
        split_writer: write_half,
        writer,
    });

    let (ext_encoder, ext_decoder) = extension.split();

    let role = if flags.contains(CodecFlags::ROLE) {
        Role::Server
    } else {
        Role::Client
    };

    let sender = Sender {
        role,
        closed: closed.clone(),
        split_writer: sender_writer,
        ext_encoder,
    };
    let receiver = Receiver {
        role,
        closed,
        framed: FramedIo {
            flags,
            max_size,
            read_half,
            reader,
            split_writer: reader_writer,
            ext_decoder,
        },
    };

    (sender, receiver)
}

impl<S> WriteHalf<S>
where
    S: WebSocketStream,
{
    async fn write<A, E>(
        &mut self,
        mut buf_ref: A,
        message_type: PayloadType,
        is_server: bool,
        extension: &mut E,
    ) -> Result<(), Error>
    where
        A: AsMut<[u8]>,
        E: ExtensionEncoder,
    {
        let WriteHalf {
            split_writer,
            writer,
            control_buffer,
        } = self;
        let buf = buf_ref.as_mut();

        match message_type {
            PayloadType::Text => writer
                .write(
                    split_writer,
                    is_server,
                    OpCode::DataCode(DataCode::Text),
                    HeaderFlags::FIN,
                    buf,
                    |payload, header| extension_encode(extension, payload, header),
                )
                .await
                .map_err(Into::into),
            PayloadType::Binary => writer
                .write(
                    split_writer,
                    is_server,
                    OpCode::DataCode(DataCode::Binary),
                    HeaderFlags::FIN,
                    buf,
                    |payload, header| extension_encode(extension, payload, header),
                )
                .await
                .map_err(Into::into),
            PayloadType::Ping => {
                if buf.len() > CONTROL_MAX_SIZE {
                    return Err(Error::with_cause(
                        ErrorKind::Protocol,
                        ProtocolError::FrameOverflow,
                    ));
                } else {
                    control_buffer.clear();
                    control_buffer.clone_from_slice(&buf[..CONTROL_MAX_SIZE]);

                    writer
                        .write(
                            split_writer,
                            is_server,
                            OpCode::ControlCode(ControlCode::Ping),
                            HeaderFlags::FIN,
                            buf,
                            |payload, header| extension_encode(extension, payload, header),
                        )
                        .await
                        .map_err(Into::into)
                }
            }
        }
    }
}

#[derive(Debug)]
struct WriteHalf<S> {
    split_writer: BiLock<S>,
    writer: FramedWrite,
    control_buffer: BytesMut,
}

#[derive(Debug)]
struct FramedIo<S, E> {
    flags: CodecFlags,
    max_size: usize,
    read_half: BiLock<S>,
    reader: FramedRead,
    split_writer: BiLock<WriteHalf<S>>,
    ext_decoder: NegotiatedExtension<E>,
}

#[derive(Debug)]
pub struct Sender<S, E> {
    role: Role,
    closed: Arc<AtomicBool>,
    split_writer: BiLock<WriteHalf<S>>,
    ext_encoder: NegotiatedExtension<E>,
}

#[derive(Debug)]
pub struct Receiver<S, E> {
    role: Role,
    closed: Arc<AtomicBool>,
    framed: FramedIo<S, E>,
}

impl<S, E> Sender<S, E>
where
    S: WebSocketStream,
    E: ExtensionEncoder,
{
    pub fn reunite<Ext>(
        self,
        receiver: Receiver<S, Ext::SplitDecoder>,
    ) -> Result<WebSocket<S, Ext>, ReuniteError<S, Ext::SplitEncoder, Ext::SplitDecoder>>
    where
        S: Debug,
        Ext: ReunitableExtension<SplitEncoder = E>,
    {
        reunite::<S, Ext>(self, receiver)
    }

    pub fn role(&self) -> Role {
        self.role
    }

    pub fn is_closed(&self) -> bool {
        self.closed.load(Ordering::Relaxed)
    }

    pub async fn write(
        &mut self,
        buf: &mut BytesMut,
        message_type: PayloadType,
    ) -> Result<(), Error> {
        if self.is_closed() {
            return Err(Error::with_cause(ErrorKind::Close, CloseError::Closed));
        }

        let writer = &mut *self.split_writer.lock().await;
        match writer
            .write(
                buf,
                message_type,
                self.role.is_server(),
                &mut self.ext_encoder,
            )
            .await
        {
            Ok(()) => Ok(()),
            Err(e) => {
                self.closed.store(true, Ordering::Relaxed);
                Err(e.into())
            }
        }
    }

    pub async fn write_fragmented(
        &mut self,
        buf: &mut BytesMut,
        message_type: MessageType,
        fragment_size: usize,
    ) -> Result<(), Error> {
        if self.is_closed() {
            return Err(Error::with_cause(ErrorKind::Close, CloseError::Closed));
        }
        let WriteHalf {
            split_writer,
            writer,
            ..
        } = &mut *self.split_writer.lock().await;
        let ext_encoder = &mut self.ext_encoder;
        let write_result = write_fragmented(
            split_writer,
            writer,
            buf,
            message_type,
            fragment_size,
            self.role.is_server(),
            |payload, header| extension_encode(ext_encoder, payload, header),
        )
        .await;

        if write_result.is_err() {
            self.closed.store(true, Ordering::Relaxed);
        }
        write_result
    }

    pub async fn close(self, reason: Option<String>) -> Result<(), Error> {
        let WriteHalf {
            split_writer,
            writer,
            ..
        } = &mut *self.split_writer.lock().await;
        let write_result = write_close(
            split_writer,
            writer,
            CloseReason::new(CloseCode::Normal, reason),
            self.role.is_server(),
        )
        .await;

        if write_result.is_err() {
            self.closed.store(true, Ordering::Relaxed);
        }
        write_result
    }
}

impl<S, E> Receiver<S, E>
where
    S: WebSocketStream,
    E: ExtensionDecoder,
{
    pub fn role(&self) -> Role {
        self.role
    }

    pub async fn read(&mut self, read_buffer: &mut BytesMut) -> Result<Message, Error> {
        let Receiver {
            role,
            closed,
            framed,
            ..
        } = self;
        let FramedIo {
            flags,
            max_size,
            read_half,
            reader,
            split_writer,
            ext_decoder,
        } = framed;
        let is_server = role.is_server();

        loop {
            match read_next(
                read_half,
                reader,
                flags,
                *max_size,
                read_buffer,
                ext_decoder,
            )
            .await
            {
                Ok(item) => match item {
                    Item::Binary => return Ok(Message::Binary),
                    Item::Text => return Ok(Message::Text),
                    Item::Ping(payload) => {
                        let WriteHalf {
                            split_writer,
                            writer,
                            ..
                        } = &mut *split_writer.lock().await;

                        writer
                            .write(
                                split_writer,
                                is_server,
                                OpCode::ControlCode(ControlCode::Pong),
                                HeaderFlags::FIN,
                                payload,
                                |_, _| Ok(()),
                            )
                            .await?;
                        return Ok(Message::Ping);
                    }
                    Item::Pong(payload) => {
                        let WriteHalf {
                            split_writer,
                            writer,
                            control_buffer,
                            ..
                        } = &mut *split_writer.lock().await;

                        if control_buffer.is_empty() {
                            continue;
                        } else {
                            return if control_buffer[..].eq(&payload[..]) {
                                control_buffer.clear();
                                Ok(Message::Pong)
                            } else {
                                closed.store(true, Ordering::Relaxed);
                                write_close(
                                    split_writer,
                                    writer,
                                    CloseReason {
                                        code: CloseCode::Protocol,
                                        description: Some(CONTROL_DATA_MISMATCH.to_string()),
                                    },
                                    is_server,
                                )
                                .await?;

                                return Err(Error::with_cause(
                                    ErrorKind::Protocol,
                                    CONTROL_DATA_MISMATCH.to_string(),
                                ));
                            };
                        }
                    }
                    Item::Close(reason) => {
                        closed.store(true, Ordering::Relaxed);
                        let WriteHalf {
                            split_writer,
                            writer,
                            ..
                        } = &mut *split_writer.lock().await;

                        return match reason {
                            Some(reason) => {
                                write_close(split_writer, writer, reason.clone(), is_server)
                                    .await?;
                                Ok(Message::Close(Some(reason)))
                            }
                            None => {
                                writer
                                    .write(
                                        split_writer,
                                        is_server,
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
                    closed.store(true, Ordering::Relaxed);
                    if !e.is_io() {
                        let reason = CloseReason::new(CloseCode::Protocol, Some(e.to_string()));
                        let WriteHalf {
                            split_writer,
                            writer,
                            ..
                        } = &mut *split_writer.lock().await;
                        write_close(split_writer, writer, reason, is_server).await?;
                    }

                    return Err(e);
                }
            }
        }
    }

    pub async fn close(self, reason: Option<String>) -> Result<(), Error> {
        let WriteHalf {
            split_writer,
            writer,
            ..
        } = &mut *self.framed.split_writer.lock().await;
        let write_result = write_close(
            split_writer,
            writer,
            CloseReason::new(CloseCode::Normal, reason),
            self.role.is_server(),
        )
        .await;

        if write_result.is_err() {
            self.closed.store(true, Ordering::Relaxed);
        }
        write_result
    }

    pub fn is_closed(&self) -> bool {
        self.closed.load(Ordering::Relaxed)
    }
}

#[derive(Debug)]
pub struct ReuniteError<S, E, D> {
    pub sender: Sender<S, E>,
    pub receiver: Receiver<S, D>,
}

fn reunite<S, E>(
    sender: Sender<S, E::SplitEncoder>,
    receiver: Receiver<S, E::SplitDecoder>,
) -> Result<WebSocket<S, E>, ReuniteError<S, E::SplitEncoder, E::SplitDecoder>>
where
    S: WebSocketStream + Debug,
    E: ReunitableExtension,
{
    if sender
        .split_writer
        .same_bilock(&receiver.framed.split_writer)
    {
        let Sender {
            split_writer: sender_writer,
            ext_encoder,
            ..
        } = sender;
        let Receiver { closed, framed, .. } = receiver;
        let FramedIo {
            flags,
            max_size,
            read_half,
            reader,
            ext_decoder,
            split_writer: reader_writer,
        } = framed;

        let WriteHalf {
            split_writer,
            writer,
            control_buffer,
            ..
        } = sender_writer
            .reunite(reader_writer)
            // This is safe as we have checked the pointers
            .expect("Failed to reunite writer");

        let framed = framed::FramedIo::from_parts(FramedIoParts {
            // This is safe as we have checked the pointers
            io: read_half
                .reunite(split_writer)
                .expect("Failed to reunite IO"),
            reader,
            writer,
            flags,
            max_size,
        });

        Ok(WebSocket::from_parts(
            framed,
            control_buffer,
            NegotiatedExtension::reunite(ext_encoder, ext_decoder),
            closed.load(Ordering::Relaxed),
        ))
    } else {
        return Err(ReuniteError { sender, receiver });
    }
}
