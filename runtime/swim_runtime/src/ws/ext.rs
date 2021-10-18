use crate::ws::{WsConnections, WsOpenFuture};
use ratchet::{Error, ExtensionProvider, ProtocolRegistry, SplittableExtension};
use tokio::io::{AsyncRead, AsyncWrite};

pub struct RatchetNetworking<E> {
    pub config: ratchet::WebSocketConfig,
    pub provider: E,
    pub subprotocols: ProtocolRegistry,
}

impl<'c, Socket, E> WsConnections<Socket> for RatchetNetworking<E>
where
    Socket: AsyncRead + AsyncWrite + Unpin + Send + Sync + 'static,
    E: ExtensionProvider + Send + Sync,
    E::Extension: SplittableExtension + Send + Sync + 'static,
{
    type Ext = E::Extension;
    type Error = Error;

    fn open_connection(
        &self,
        socket: Socket,
        addr: String,
    ) -> WsOpenFuture<Socket, Self::Ext, Self::Error> {
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

    fn accept_connection(&self, socket: Socket) -> WsOpenFuture<Socket, Self::Ext, Self::Error> {
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
