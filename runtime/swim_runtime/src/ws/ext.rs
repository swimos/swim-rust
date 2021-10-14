use crate::ws::WsConnections;
use futures::future::BoxFuture;
use ratchet::{Error, ExtensionProvider, ProtocolRegistry, WebSocket};
use tokio::io::{AsyncRead, AsyncWrite};

pub struct RatchetNetworking<E> {
    config: ratchet::WebSocketConfig,
    provider: E,
    subprotocols: ProtocolRegistry,
}

impl<'c, Socket, E> WsConnections<Socket> for RatchetNetworking<E>
where
    Socket: AsyncRead + AsyncWrite + Unpin + Send + Sync + 'static,
    E: ExtensionProvider + Send + Sync,
    E::Extension: Send + Sync,
{
    type Ext = E::Extension;
    type Error = Error;

    fn open_connection(
        &self,
        socket: Socket,
        addr: String,
    ) -> BoxFuture<Result<WebSocket<Socket, Self::Ext>, Self::Error>> {
        let RatchetNetworking {
            config,
            provider,
            subprotocols,
        } = self;

        let config = *config;
        let ref_provider = provider;
        let subprotocols = subprotocols.clone();

        Box::pin(async move {
            let socket = ratchet::subscribe_with(config, socket, addr, ref_provider, subprotocols)
                .await?
                .into_websocket();
            Ok(socket)
        })
    }

    fn accept_connection(
        &self,
        socket: Socket,
    ) -> BoxFuture<Result<WebSocket<Socket, Self::Ext>, Self::Error>> {
        let RatchetNetworking {
            config,
            provider,
            subprotocols,
        } = self;

        let config = *config;
        let ref_provider = provider;
        let subprotocols = subprotocols.clone();

        Box::pin(async move {
            let socket = ratchet::accept_with(socket, config, ref_provider, subprotocols)
                .await?
                .upgrade()
                .await?
                .into_websocket();
            Ok(socket)
        })
    }
}
