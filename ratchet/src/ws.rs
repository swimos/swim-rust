use crate::errors::{CloseError, Error, ErrorKind};
use crate::framed::{read_into, FramedIo, ReadError, CONTROL_FRAME_LEN};
use crate::handshake::{exec_client_handshake, HandshakeResult, ProtocolRegistry};
use crate::protocol::{
    CloseCode, CloseReason, ControlCode, DataCode, HeaderFlags, Message, MessageType, OpCode, Role,
};
use crate::{Extension, ExtensionProvider, Request, WebSocketConfig, WebSocketStream};
use bytes::BytesMut;

const CONTROL_MAX_SIZE: usize = 125;

pub struct WebSocket<S, E> {
    inner: WebSocketInner<S, E>,
}

impl<S, E> WebSocket<S, E>
where
    S: WebSocketStream,
    E: Extension,
{
    pub async fn read(&mut self, read_buffer: &mut BytesMut) -> Result<Message, Error> {
        self.inner.read(read_buffer).await
    }

    pub async fn write(
        &mut self,
        buf: &mut BytesMut,
        message_type: MessageType,
    ) -> Result<(), Error> {
        self.inner.write(buf, message_type).await
    }

    pub async fn close(mut self, reason: Option<String>) -> Result<(), Error> {
        self.inner
            .framed
            .write_close(CloseReason::new(CloseCode::Normal, reason))
            .await
    }

    pub fn split(self) -> ((), ()) {
        unimplemented!()
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
    let WebSocketConfig { max_size } = config;
    let mut read_buffer = BytesMut::new();

    let HandshakeResult {
        protocol,
        extension,
    } = exec_client_handshake(
        &mut stream,
        request,
        extension,
        ProtocolRegistry::default(),
        &mut read_buffer,
    )
    .await?;

    let socket = WebSocket {
        inner: WebSocketInner {
            framed: FramedIo::new(stream, read_buffer, Role::Client, max_size),
            _extension: extension,
            control_buffer: BytesMut::with_capacity(CONTROL_MAX_SIZE),
            closed: false,
        },
    };
    Ok((socket, protocol))
}

struct WebSocketInner<S, E> {
    framed: FramedIo<S>,
    control_buffer: BytesMut,
    _extension: E,
    closed: bool,
}

impl<S, E> WebSocketInner<S, E>
where
    S: WebSocketStream,
    E: Extension,
{
    async fn read(&mut self, read_buffer: &mut BytesMut) -> Result<Message, Error> {
        let WebSocketInner {
            framed,
            closed,
            control_buffer,
            ..
        } = self;

        if *closed {
            return Err(Error::with_cause(ErrorKind::Close, CloseError::Closed));
        }

        match read_into(framed, read_buffer, control_buffer, closed).await {
            Ok(message) => Ok(message),
            Err(e) => {
                let ReadError { close_with, error } = e;
                self.closed = true;

                if let Some(reason) = close_with {
                    // This will only be 'None' if an IO error was encountered and so we cannot
                    // write a reason
                    self.framed.write_close(reason).await?;
                }
                return Err(error);
            }
        }
    }

    async fn write(&mut self, buf: &mut BytesMut, message_type: MessageType) -> Result<(), Error> {
        if self.closed {
            return Err(Error::with_cause(ErrorKind::Close, CloseError::Closed));
        }

        let op_code = match message_type {
            MessageType::Text => OpCode::DataCode(DataCode::Text),
            MessageType::Binary => OpCode::DataCode(DataCode::Binary),
            MessageType::Ping => {
                if buf.len() > CONTROL_MAX_SIZE {
                    return Err(Error::with_cause(ErrorKind::Protocol, CONTROL_FRAME_LEN));
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
}
