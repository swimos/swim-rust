use bytes::{Bytes, BytesMut};
use futures::stream::unfold;
use futures::Stream;
use ratchet::{CloseCode, ErrorKind, Extension, Message};
use ratchet::{CloseReason, Error, ExtensionDecoder, WebSocketStream};

#[derive(Debug, Clone, PartialEq)]
pub enum WsMessage {
    Text(String),
    Binary(Bytes),
    Ping,
    Pong,
    Close(Option<CloseReason>),
}

pub struct AutoWebSocket<S, E> {
    inner: ratchet::WebSocket<S, E>,
    buf: BytesMut,
}

impl<S, E> AutoWebSocket<S, E>
where
    S: WebSocketStream,
    E: Extension,
{
    pub fn new(inner: ratchet::WebSocket<S, E>) -> AutoWebSocket<S, E> {
        AutoWebSocket {
            inner,
            buf: BytesMut::default(),
        }
    }

    pub fn into_inner(self) -> ratchet::WebSocket<S, E> {
        self.inner
    }

    pub async fn write_text<I: AsRef<str>>(&mut self, buf: I) -> Result<(), Error> {
        self.inner.write_text(buf).await
    }

    pub async fn read(&mut self) -> Result<WsMessage, Error> {
        let AutoWebSocket { inner, buf } = self;

        match inner.read(buf).await? {
            Message::Text => match String::from_utf8(buf.to_vec()) {
                Ok(value) => {
                    buf.clear();
                    Ok(WsMessage::Text(value))
                }
                Err(e) => {
                    inner
                        .close_with(CloseReason::new(
                            CloseCode::Protocol,
                            Some("Invalid encoding".to_string()),
                        ))
                        .await?;
                    Err(Error::with_cause(ErrorKind::Encoding, e))
                }
            },
            Message::Binary => Ok(WsMessage::Binary(buf.split().freeze())),
            Message::Ping => Ok(WsMessage::Ping),
            Message::Pong => Ok(WsMessage::Pong),
            Message::Close(reason) => Ok(WsMessage::Close(reason)),
        }
    }
}

pub struct WebSocketReceiver<S, E> {
    inner: ratchet::Receiver<S, E>,
    buf: BytesMut,
}

impl<S, E> WebSocketReceiver<S, E>
where
    S: WebSocketStream,
    E: ExtensionDecoder,
{
    pub fn new(inner: ratchet::Receiver<S, E>) -> WebSocketReceiver<S, E> {
        WebSocketReceiver {
            inner,
            buf: BytesMut::default(),
        }
    }

    pub async fn read(&mut self) -> Result<WsMessage, Error> {
        let WebSocketReceiver { inner, buf } = self;

        match inner.read(buf).await? {
            Message::Text => match String::from_utf8(buf.to_vec()) {
                Ok(value) => {
                    buf.clear();
                    Ok(WsMessage::Text(value))
                }
                Err(e) => {
                    inner
                        .close_with(CloseReason::new(
                            CloseCode::Protocol,
                            Some("Invalid encoding".to_string()),
                        ))
                        .await?;
                    Err(Error::with_cause(ErrorKind::Encoding, e))
                }
            },
            Message::Binary => Ok(WsMessage::Binary(buf.split().freeze())),
            Message::Ping => Ok(WsMessage::Ping),
            Message::Pong => Ok(WsMessage::Pong),
            Message::Close(reason) => Ok(WsMessage::Close(reason)),
        }
    }
}

pub fn into_stream<S, E>(
    rx: ratchet::Receiver<S, E>,
) -> impl Stream<Item = Result<WsMessage, Error>>
where
    S: WebSocketStream,
    E: ExtensionDecoder,
{
    unfold(WebSocketReceiver::new(rx), |mut rx| async move {
        // todo: this should terminate after an error
        match rx.read().await {
            Ok(item) => Some((Ok(item), rx)),
            Err(e) => Some((Err(e), rx)),
        }
    })
}
