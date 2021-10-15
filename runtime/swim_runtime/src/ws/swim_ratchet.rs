use bytes::{Bytes, BytesMut};
use futures::stream::unfold;
use futures::Stream;
use ratchet::deflate::{Deflate, DeflateExtProvider, DeflateExtensionError};
use ratchet::{
    CloseCode, ErrorKind, Extension, ExtensionProvider, Header, HeaderMap, HeaderValue, Message,
};
use ratchet::{CloseReason, Error, ExtensionDecoder, WebSocketStream};
use std::borrow::Cow;
use std::sync::Arc;

#[derive(Debug, Clone, PartialEq)]
pub enum WsMessage {
    Text(String),
    Binary(Bytes),
    Ping,
    Pong,
    Close(Option<CloseReason>),
}

impl<I> From<I> for WsMessage
where
    I: Into<Cow<'static, str>>,
{
    fn from(f: I) -> Self {
        let str = f.into();
        WsMessage::Text(str.to_string())
    }
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
        match rx.read().await {
            Ok(item) => Some((Ok(item), rx)),
            Err(e) => Some((Err(e), rx)),
        }
    })
}

#[derive(Debug, Clone)]
pub enum CompressionSwitcherProvider {
    On(Arc<DeflateExtProvider>),
    Off,
}

impl ExtensionProvider for CompressionSwitcherProvider {
    type Extension = Deflate;
    type Error = DeflateExtensionError;

    fn apply_headers(&self, headers: &mut HeaderMap) {
        if let CompressionSwitcherProvider::On(e) = self {
            e.apply_headers(headers);
        }
    }

    fn negotiate_client(&self, headers: &[Header]) -> Result<Option<Self::Extension>, Self::Error> {
        match self {
            CompressionSwitcherProvider::On(ext) => ext.negotiate_client(headers),
            CompressionSwitcherProvider::Off => Ok(None),
        }
    }

    fn negotiate_server(
        &self,
        headers: &[Header],
    ) -> Result<Option<(Self::Extension, HeaderValue)>, Self::Error> {
        match self {
            CompressionSwitcherProvider::On(ext) => ext.negotiate_server(headers),
            CompressionSwitcherProvider::Off => Ok(None),
        }
    }
}
