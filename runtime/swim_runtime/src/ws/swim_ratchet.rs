use bytes::{Bytes, BytesMut};
use futures::stream::unfold;
use futures::Stream;
pub use ratchet::Message as WsMessageType;
use ratchet::{CloseReason, Error, ExtensionDecoder, WebSocketStream};
use std::borrow::Cow;

#[derive(Debug, Clone, PartialEq)]
pub struct WsMessage {
    pub payload: Bytes,
    pub kind: WsMessageType,
}

impl WsMessage {
    pub fn new(payload: Bytes, kind: WsMessageType) -> WsMessage {
        WsMessage { payload, kind }
    }

    pub fn text<A>(a: A) -> WsMessage
    where
        A: Into<Cow<'static, str>>,
    {
        let str = a.into().to_string();
        WsMessage {
            payload: Bytes::from(str),
            kind: WsMessageType::Text,
        }
    }

    /// Attempt to convert this message into a valid UTF-8 String. If this message is not text or it
    /// contains invalid UTF-8 then the original message is returned.
    pub fn try_into_text(self) -> Result<String, WsMessage> {
        let WsMessage { payload, kind } = self;
        match kind {
            WsMessageType::Text => match String::from_utf8(payload.to_vec()) {
                Ok(string) => Ok(string),
                Err(_) => Err(WsMessage { payload, kind }),
            },
            kind => Err(WsMessage { payload, kind }),
        }
    }

    /// Attempt to convert this message into a its binary contents if it is of `WsMessageType::Binary`.
    /// If this message is not binary then the original message is returned.
    pub fn try_into_binary(self) -> Result<Bytes, WsMessage> {
        let WsMessage { payload, kind } = self;
        match kind {
            WsMessageType::Binary => Ok(payload),
            kind => Err(WsMessage { payload, kind }),
        }
    }

    /// Attempt to convert this message into an optional close reason if it is of
    /// `WsMessageType::Close`. If this is not a close reason then the original message is returned.
    pub fn try_into_close(self) -> Result<Option<CloseReason>, WsMessage> {
        let WsMessage { payload, kind } = self;
        match kind {
            WsMessageType::Close(reason) => Ok(reason),
            kind => Err(WsMessage { payload, kind }),
        }
    }
}

pub struct Receiver<S, E> {
    inner: ratchet::Receiver<S, E>,
    buf: BytesMut,
}

impl<S, E> Receiver<S, E>
where
    S: WebSocketStream,
    E: ExtensionDecoder,
{
    pub fn new(inner: ratchet::Receiver<S, E>) -> Receiver<S, E> {
        Receiver {
            inner,
            buf: BytesMut::default(),
        }
    }

    pub async fn read(&mut self) -> Result<WsMessage, Error> {
        let Receiver { inner, buf } = self;

        match inner.read(buf).await? {
            WsMessageType::Text => Ok(WsMessage {
                payload: buf.split().freeze(),
                kind: WsMessageType::Text,
            }),
            WsMessageType::Binary => Ok(WsMessage {
                payload: buf.split().freeze(),
                kind: WsMessageType::Text,
            }),
            t @ WsMessageType::Ping | t @ WsMessageType::Pong => Ok(WsMessage {
                payload: Bytes::default(),
                kind: t,
            }),
            WsMessageType::Close(reason) => Ok(WsMessage {
                payload: Bytes::default(),
                kind: WsMessageType::Close(reason),
            }),
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
    unfold(Receiver::new(rx), |mut rx| async move {
        // todo: this should terminate after an error
        match rx.read().await {
            Ok(item) => Some((Ok(item), rx)),
            Err(e) => Some((Err(e), rx)),
        }
    })
}
