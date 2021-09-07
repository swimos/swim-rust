use crate::handshake::io::BufferedIo;
use crate::handshake::{ParseResult, Parser};
use crate::{
    Error, ExtensionProvider, ProtocolRegistry, WebSocket, WebSocketConfig, WebSocketStream,
};
use bytes::BytesMut;
use futures::future::BoxFuture;
use http::{HeaderMap, StatusCode};
use httparse::{Header, Status};
use std::marker::PhantomData;

pub enum UpgradeAction {
    Upgrade(WebsocketResponse),
    Reject(WebsocketResponse),
}

pub trait Interceptor {
    fn intercept<'buf, 's>(
        self,
        path: &'s str,
        subprotocol: &'s str,
        headers: &'buf [Header<'s>],
    ) -> BoxFuture<'buf, Result<UpgradeAction, Error>>
    where
        's: 'buf;
}

impl<F> Interceptor for F
where
    F: for<'s, 'buf> FnOnce(
        &'s str,
        &'s str,
        &'buf [Header<'s>],
    ) -> BoxFuture<'buf, Result<UpgradeAction, Error>>,
{
    fn intercept<'buf, 's>(
        self,
        path: &'s str,
        subprotocol: &'s str,
        headers: &'buf [Header<'s>],
    ) -> BoxFuture<'buf, Result<UpgradeAction, Error>>
    where
        's: 'buf,
    {
        self(path, subprotocol, headers)
    }
}

pub struct AcceptResult<'buf, 's: 'buf, S, E> {
    upgrader: WebSocketUpgrader<'buf, S, E>,
    path: &'s str,
    subprotocol: &'s str,
    headers: &'buf [Header<'s>],
}

pub async fn accept<'buf, 's, S, E>(
    mut stream: S,
    _config: WebSocketConfig,
    _extension: E,
    subprotocols: ProtocolRegistry,
) -> Result<AcceptResult<'buf, 's, S, E>, Error>
where
    's: 'buf,
    S: WebSocketStream,
    E: ExtensionProvider,
{
    let mut read_buf = BytesMut::new();
    let mut io = BufferedIo::new(&mut stream, &mut read_buf);
}

pub struct WebsocketResponse {
    code: StatusCode,
    headers: HeaderMap,
}

pub struct WebSocketUpgrader<'buf, S, E> {
    io: BufferedIo<'buf, S>,
    extension: E,
    _pd: PhantomData<&'buf ()>,
}

impl<'buf, S, E> WebSocketUpgrader<'buf, S, E>
where
    S: WebSocketStream,
    E: ExtensionProvider,
{
    pub async fn upgrade(
        self,
        _response: WebsocketResponse,
    ) -> Result<WebSocket<S, E::Extension>, Error> {
        unimplemented!()
    }

    pub async fn reject(self, _response: WebsocketResponse) -> Result<(), Error> {
        unimplemented!()
    }
}

struct RequestParser {}

impl Parser for RequestParser {
    type Output = ();

    fn parse(&mut self, buf: &[u8]) -> Result<ParseResult<Self::Output>, Error> {
        todo!()
    }
}
