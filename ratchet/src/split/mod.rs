#[cfg(test)]
mod tests;

use crate::extensions::{ExtensionDecoder, ExtensionEncoder, SplittableExtension};
use crate::framed::{
    read_next, write_close, write_fragmented, CodecFlags, FramedIoParts, FramedRead, FramedWrite,
    Item,
};
use crate::protocol::{
    CloseCode, CloseReason, ControlCode, DataCode, HeaderFlags, MessageType, OpCode,
};
use crate::ws::{CONTROL_DATA_MISMATCH, CONTROL_MAX_SIZE};
use crate::{
    framed, CloseError, Error, ErrorKind, Message, PayloadType, ProtocolError, Role, WebSocket,
    WebSocketStream,
};
use bitflags::_core::sync::atomic::Ordering;
use bytes::BytesMut;
use std::fmt::Debug;
use std::sync::atomic::AtomicBool;
use std::sync::Arc;
use tokio::io::{ReadHalf, WriteHalf as TokioWriteHalf};
use tokio::sync::Mutex;

// Replace Mutexes with Future's BiLock when it's been stabilised
// See:
//      https://github.com/rust-lang/futures-rs/issues/2289
//      https://github.com/rust-lang/futures-rs/pull/2384

// todo sink and stream implementations
// todo split extension
pub fn split<S, E>(
    framed: framed::FramedIo<S>,
    control_buffer: BytesMut,
    extension: E,
) -> (Sender<S, E::Encoder>, Receiver<S, E::Decoder>)
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
    let (read_half, write_half) = tokio::io::split(io);
    let split_writer = Arc::new(Mutex::new(WriteHalf {
        control_buffer,
        split_writer: write_half,
        writer,
    }));

    let (ext_encoder, ext_decoder) = extension.split();

    let role = if flags.contains(CodecFlags::ROLE) {
        Role::Server
    } else {
        Role::Client
    };

    let sender = Sender {
        role,
        closed: closed.clone(),
        split_writer: split_writer.clone(),
        extension: ext_encoder,
    };
    let receiver = Receiver {
        role,
        closed,
        framed: FramedIo {
            flags,
            max_size,
            read_half,
            reader,
            split_writer,
        },
        extension: ext_decoder,
    };

    (sender, receiver)
}

impl<S> WriteHalf<S>
where
    S: WebSocketStream,
{
    async fn write<A>(
        &mut self,
        mut buf_ref: A,
        message_type: PayloadType,
        is_server: bool,
    ) -> Result<(), Error>
    where
        A: AsMut<[u8]>,
    {
        let WriteHalf {
            split_writer,
            writer,
            control_buffer,
        } = self;
        let buf = buf_ref.as_mut();

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
                    control_buffer.clear();
                    control_buffer.clone_from_slice(&buf[..CONTROL_MAX_SIZE]);
                    OpCode::ControlCode(ControlCode::Ping)
                }
            }
        };

        writer
            .write(split_writer, is_server, op_code, HeaderFlags::FIN, buf)
            .await
            .map_err(Into::into)
    }
}

#[derive(Debug)]
struct WriteHalf<S> {
    split_writer: TokioWriteHalf<S>,
    writer: FramedWrite,
    control_buffer: BytesMut,
}

#[derive(Debug)]
struct FramedIo<S> {
    flags: CodecFlags,
    max_size: usize,
    read_half: ReadHalf<S>,
    reader: FramedRead,
    split_writer: Arc<Mutex<WriteHalf<S>>>,
}

#[derive(Debug)]
pub struct Sender<S, E> {
    role: Role,
    closed: Arc<AtomicBool>,
    split_writer: Arc<Mutex<WriteHalf<S>>>,
    extension: E,
}

#[derive(Debug)]
pub struct Receiver<S, E> {
    role: Role,
    closed: Arc<AtomicBool>,
    framed: FramedIo<S>,
    extension: E,
}

impl<S, E> Sender<S, E>
where
    S: WebSocketStream,
    E: ExtensionEncoder,
{
    pub fn reunite<Ext>(
        self,
        receiver: Receiver<S, Ext::Decoder>,
    ) -> Result<WebSocket<S, Ext>, ReuniteError<S, Ext::Encoder, Ext::Decoder>>
    where
        S: Debug,
        E: ExtensionEncoder<United = Ext>,
        Ext: SplittableExtension<Encoder = E>,
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
        match writer.write(buf, message_type, self.role.is_server()).await {
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
        let write_result = write_fragmented(
            split_writer,
            writer,
            buf,
            message_type,
            fragment_size,
            self.role.is_server(),
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
        } = framed;
        let is_server = role.is_server();

        loop {
            match read_next(read_half, reader, flags, *max_size, read_buffer).await {
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
    sender: Sender<S, E::Encoder>,
    receiver: Receiver<S, E::Decoder>,
) -> Result<WebSocket<S, E>, ReuniteError<S, E::Encoder, E::Decoder>>
where
    S: WebSocketStream + Debug,
    E: SplittableExtension,
{
    if Arc::ptr_eq(&sender.split_writer, &receiver.framed.split_writer) {
        let Sender {
            split_writer,
            extension: ext_encoder,
            ..
        } = sender;
        let Receiver {
            closed,
            framed,
            extension: ext_decoder,
            ..
        } = receiver;
        let FramedIo {
            flags,
            max_size,
            read_half,
            reader,
            split_writer: io_split_writer,
        } = framed;
        drop(io_split_writer);

        let WriteHalf {
            split_writer,
            writer,
            control_buffer,
            ..
        } = Arc::try_unwrap(split_writer).unwrap().into_inner();

        let framed = framed::FramedIo::from_parts(FramedIoParts {
            io: read_half.unsplit(split_writer),
            reader,
            writer,
            flags,
            max_size,
        });

        Ok(WebSocket::from_parts(
            framed,
            control_buffer,
            E::reunite(ext_encoder, ext_decoder),
            closed.load(Ordering::Relaxed),
        ))
    } else {
        return Err(ReuniteError { sender, receiver });
    }
}
