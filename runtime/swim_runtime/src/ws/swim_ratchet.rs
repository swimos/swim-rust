use bytes::{Bytes, BytesMut};
use ratchet::{CloseReason, Error, ExtensionDecoder, Message as MessageType, WebSocketStream};

pub struct Message {
    pub payload: Bytes,
    pub kind: MessageType,
}

impl Message {
    /// Attempt to convert this message into a valid UTF-8 String. If this message is not text or it
    /// contains invalid UTF-8 then the original message is returned.
    pub fn try_into_text(self) -> Result<String, Message> {
        let Message { payload, kind } = self;
        match kind {
            MessageType::Text => match String::from_utf8(payload.to_vec()) {
                Ok(string) => Ok(string),
                Err(_) => Err(Message { payload, kind }),
            },
            kind => Err(Message { payload, kind }),
        }
    }

    /// Attempt to convert this message into a its binary contents if it is of `MessageType::Binary`.
    /// If this message is not binary then the original message is returned.
    pub fn try_into_binary(self) -> Result<Bytes, Message> {
        let Message { payload, kind } = self;
        match kind {
            MessageType::Binary => Ok(payload),
            kind => Err(Message { payload, kind }),
        }
    }

    /// Attempt to convert this message into an optional close reason if it is of
    /// `MessageType::Close`. If this is not a close reason then the original message is returned.
    pub fn try_into_close(self) -> Result<Option<CloseReason>, Message> {
        let Message { payload, kind } = self;
        match kind {
            MessageType::Close(reason) => Ok(reason),
            kind => Err(Message { payload, kind }),
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

    pub async fn read(&mut self) -> Result<Message, Error> {
        let Receiver { inner, buf } = self;

        match inner.read(buf).await? {
            MessageType::Text => Ok(Message {
                payload: buf.split().freeze(),
                kind: MessageType::Text,
            }),
            MessageType::Binary => Ok(Message {
                payload: buf.split().freeze(),
                kind: MessageType::Text,
            }),
            t @ MessageType::Ping | t @ MessageType::Pong => Ok(Message {
                payload: Bytes::default(),
                kind: t,
            }),
            MessageType::Close(reason) => Ok(Message {
                payload: Bytes::default(),
                kind: MessageType::Close(reason),
            }),
        }
    }
}
