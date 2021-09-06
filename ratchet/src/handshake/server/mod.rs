use crate::errors::Error;
use crate::handshake::io::BufferedIo;
use crate::{WebSocketConfig, WebSocketStream};
use bytes::BytesMut;
use tokio_native_tls::TlsConnector;

pub async fn exec_client_handshake<S>(
    _config: &WebSocketConfig,
    stream: &mut S,
    _connector: Option<TlsConnector>,
) -> Result<(), Error>
where
    S: WebSocketStream,
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
    S: WebSocketStream,
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
