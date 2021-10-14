use crate::ws2::sender::BoxWebSocketSender;
use crate::ws2::swim_ratchet::{accept_connection, open_connection};
use crate::ws2::WsConnections;
use futures::future::BoxFuture;
use futures::stream::BoxStream;
use ratchet::{CloseReason, Error, Message, WebSocketConfig};
use tokio::io::{AsyncRead, AsyncWrite};

pub struct RatchetExternalConnections {}

impl<Sock> WsConnections<Sock> for RatchetExternalConnections
where
    Sock: AsyncRead + AsyncWrite + Unpin + Send + Sync + Unpin + 'static,
{
    type Item = Message;
    type Close = CloseReason;
    type Error = Error;
    type Sink = BoxWebSocketSender<Self::Item, Self::Close, Self::Error>;
    type Stream = BoxStream<'static, Result<Self::Item, Self::Error>>;
    type Fut = BoxFuture<'static, Result<(Self::Sink, Self::Stream), Self::Error>>;

    fn open_connection(&self, socket: Sock, addr: String) -> Self::Fut {
        Box::pin(open_connection(WebSocketConfig::default(), socket, addr))
    }

    fn accept_connection(&self, socket: Sock) -> Self::Fut {
        Box::pin(accept_connection(WebSocketConfig::default(), socket))
    }
}
