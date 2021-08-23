mod codec;
mod io;

use crate::codec::CodecFlags;
use crate::conn::codec::{Continuation, Item};
use crate::conn::io::FramedIo;
use crate::errors::{CloseError, ErrorKind};
use crate::handshake::{exec_client_handshake, HandshakeResult, ProtocolRegistry};
use crate::owned::Message;
use crate::protocol::frame::CloseCode;
use crate::protocol::frame::{CloseReason, ControlCode, DataCode, Frame, FrameHeader, OpCode};
use crate::protocol::HeaderFlags;
use crate::{Error, Extension, Request, Role, WebSocketStream};
use crate::{ExtensionProvider, NoExt, WebSocketConfig};
use bytes::{Buf, BufMut, BytesMut};
use codec::Codec;
use codec::FragmentBuffer;
use either::Either;
use futures::{AsyncReadExt, AsyncWriteExt};
use std::convert::TryFrom;
use std::iter::FromIterator;
use tokio_util::codec::{Decoder, Encoder};
use tokio_util::compat::{Compat, TokioAsyncReadCompatExt};

pub struct WebSocket<S, E = NoExt> {
    pub io: FramedIo<S>,
    pub write_buffer: BytesMut,
    pub read_buffer: BytesMut,
    pub codec: Codec,
    pub extension: E,
    pub fragment_buffer: FragmentBuffer,
    pub open: bool,
}

enum State {
    NoCont,
    ReadingCont(Kind, BytesMut),
}

enum Kind {
    Binary,
    Text,
}

bitflags::bitflags! {
    pub struct ReadFlags: u8 {
        const R_CONT    = 0b0001;
        const W_CONT    = 0b0010;
        const IS_TEXT   = 0b0100;

        const IS_CONT   = Self::R_CONT.bits | Self::W_CONT.bits;
    }
}

impl<S, E> WebSocket<S, E>
where
    S: WebSocketStream,
    E: Extension,
{
    pub async fn read(&mut self) -> Result<Message, Error> {
        let WebSocket {
            io,
            read_buffer,
            write_buffer,
            codec,
            open,
            ..
        } = self;

        if !*open {
            return Err(Error::with_cause(ErrorKind::IO, CloseError::Normal));
        }

        let mut state = State::NoCont;

        loop {
            let (header, payload) = io.read_frame(read_buffer).await?;
            // let frame = Frame::new(header, payload.freeze());

            match header.opcode {
                OpCode::DataCode(d) => match d {
                    DataCode::Continuation => {
                        if header.flags.contains(HeaderFlags::FIN) {
                            match state {
                                State::NoCont => panic!(),
                                State::ReadingCont(kind, mut buf) => {
                                    buf.put(payload);
                                    match kind {
                                        Kind::Binary => return Ok(Message::Binary(buf.freeze())),
                                        Kind::Text => return Ok(Message::Text(buf.freeze())),
                                    }
                                }
                            }
                        } else {
                            match &mut state {
                                State::NoCont => panic!(),
                                State::ReadingCont(_, buf) => {
                                    buf.put(payload);
                                    continue;
                                }
                            }
                        }
                    }
                    DataCode::Text => {
                        if header.flags.contains(HeaderFlags::FIN) {
                            return Ok(Message::Text(payload.freeze()));
                        } else {
                            state = State::ReadingCont(Kind::Text, payload);
                            continue;
                        }
                    }
                    DataCode::Binary => {
                        if header.flags.contains(HeaderFlags::FIN) {
                            return Ok(Message::Binary(payload.freeze()));
                        } else {
                            state = State::ReadingCont(Kind::Binary, payload);
                            continue;
                        }
                    }
                },
                OpCode::ControlCode(c) => match c {
                    ControlCode::Close => {
                        let reason = if payload.len() < 2 {
                            None
                        } else {
                            let close_reason = std::str::from_utf8(&payload[2..])?.to_string();
                            let description = if close_reason.is_empty() {
                                None
                            } else {
                                Some(close_reason)
                            };

                            let code_no = u16::from_be_bytes([payload[0], payload[1]]);
                            let close_code = CloseCode::try_from(code_no)?;
                            let reason = CloseReason::new(close_code, description);

                            Some(reason)
                        };

                        // println!("{:?}", reason);

                        // let item = Item::Close(reason.clone());

                        // codec.encode(item, write_buffer)?;
                        // io.write(write_buffer).await?;
                        // *open = false;

                        return Ok(Message::Close(reason));
                    }
                    ControlCode::Ping => {
                        // let item = Item::Pong(payload.clone().freeze());
                        // codec.encode(item, write_buffer)?;
                        // io.write(write_buffer).await?;

                        return Ok(Message::Ping(payload.freeze()));
                    }
                    ControlCode::Pong => return Ok(Message::Pong(payload.freeze())),
                },
            }
        }
    }

    pub async fn write(&mut self, message: Message) -> Result<(), Error> {
        let WebSocket {
            io,
            write_buffer,
            codec,
            ..
        } = self;

        let message = match message {
            Message::Text(b) => Item::Text(b),
            Message::Binary(b) => Item::Binary(b),
            Message::Ping(b) => Item::Ping(b),
            Message::Pong(b) => Item::Pong(b),
            Message::Close(b) => Item::Close(b),
        };

        codec.encode(message, write_buffer)?;
        io.write(write_buffer).await
    }
}

pub async fn client<S, E>(
    config: WebSocketConfig,
    mut stream: S,
    request: Request,
    extension: E,
) -> Result<(WebSocket<S, E::Extension>, Option<String>), Error>
where
    S: WebSocketStream,
    E: ExtensionProvider,
{
    let HandshakeResult {
        protocol,
        extension,
        io_buf,
    } = exec_client_handshake(&mut stream, request, extension, ProtocolRegistry::default()).await?;
    let codec = Codec::new(Role::Client, usize::MAX);
    let socket = WebSocket {
        io: FramedIo::new(stream),
        write_buffer: Default::default(),
        read_buffer: io_buf,
        codec,
        extension,
        fragment_buffer: FragmentBuffer::new(usize::MAX),
        open: true,
    };
    Ok((socket, protocol))
}
