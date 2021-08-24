use crate::errors::{CloseError, Error, ErrorKind};
use crate::framed::{read_into, FramedIo};
use crate::handshake::{exec_client_handshake, HandshakeResult, ProtocolRegistry};
use crate::protocol::{
    CloseCode, CloseReason, ControlCode, DataCode, HeaderFlags, Message, MessageType, OpCode, Role,
};
use crate::{Extension, ExtensionProvider, Request, WebSocketConfig, WebSocketStream};
use bytes::BytesMut;

pub struct WebSocket<S, E> {
    framed: FramedIo<S>,
    _extension: E,
    closed: bool,
}

impl<S, E> WebSocket<S, E>
where
    S: WebSocketStream,
    E: Extension,
{
    pub async fn read(&mut self, read_buffer: &mut BytesMut) -> Result<Message, Error> {
        let WebSocket { framed, closed, .. } = self;

        if *closed {
            return Err(Error::with_cause(ErrorKind::Close, CloseError::Closed));
        }

        match read_into(framed, read_buffer, closed).await {
            Ok(message) => Ok(message),
            Err(e) => {
                // todo handle error and close the socket
                panic!("{:?}", e)
            }
        }
    }

    pub async fn write(
        &mut self,
        buf: &mut BytesMut,
        message_type: MessageType,
    ) -> Result<(), Error> {
        let WebSocket { framed, closed, .. } = self;

        if *closed {
            return Err(Error::with_cause(ErrorKind::Close, CloseError::Closed));
        }

        let op_code = match message_type {
            MessageType::Text => OpCode::DataCode(DataCode::Text),
            MessageType::Binary => OpCode::DataCode(DataCode::Binary),
            MessageType::Ping => OpCode::ControlCode(ControlCode::Ping),
        };

        match framed.write(op_code, HeaderFlags::FIN, buf).await {
            Ok(()) => Ok(()),
            Err(e) => {
                if e.is_close() {
                    *closed = true;
                }
                Err(e)
            }
        }
    }

    pub async fn close(mut self, reason: Option<String>) -> Result<(), Error> {
        self.framed
            .write_close(CloseReason::new(CloseCode::Normal, reason))
            .await
    }

    pub fn split(self) -> ((), ()) {
        unimplemented!()
    }
}

// todo add function to execute the handshake using a provided buffer that returns a websocket that
//  has no read or write buffers
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
        framed: FramedIo::new(stream, read_buffer, Role::Client, max_size),
        _extension: extension,
        closed: false,
    };
    Ok((socket, protocol))
}
