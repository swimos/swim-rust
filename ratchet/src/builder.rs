use crate::codec::Codec;
use crate::errors::Error;
use crate::extensions::ext::NoExtProxy;
use crate::extensions::ExtensionHandshake;
use crate::handshake::ProtocolRegistry;
use crate::{client, Interceptor, TryIntoRequest, WebSocket, WebSocketConfig, WebSocketStream};
use tokio_native_tls::TlsConnector;

/// This gives the flexibility to build websockets in a 'friendlier' fashion as opposed to having
/// a bunch of functions like `connect_with_config_and_stream` and `connect_with_config` etc.
pub struct WebSocketClientBuilder<E> {
    config: Option<WebSocketConfig>,
    connector: Option<TlsConnector>,
    extension: E,
    subprotocols: ProtocolRegistry,
}

impl Default for WebSocketClientBuilder<NoExtProxy> {
    fn default() -> Self {
        WebSocketClientBuilder {
            config: None,
            connector: None,
            extension: NoExtProxy,
            subprotocols: ProtocolRegistry::default(),
        }
    }
}

impl<E: ExtensionHandshake> WebSocketClientBuilder<E> {
    pub async fn subscribe<S, I>(
        self,
        stream: S,
        request: I,
    ) -> Result<(WebSocket<S, Codec, E::Extension>, Option<String>), Error>
    where
        S: WebSocketStream,
        I: TryIntoRequest,
    {
        let WebSocketClientBuilder {
            config,
            extension,
            subprotocols,
            ..
        } = self;
        let request = request.try_into_request()?;

        client(
            config.unwrap_or_default(),
            stream,
            request,
            Codec,
            extension,
            subprotocols,
        )
        .await
    }

    pub fn config(mut self, config: WebSocketConfig) -> Self {
        self.config = Some(config);
        self
    }

    pub fn tls_connector(mut self, connector: TlsConnector) -> Self {
        self.connector = Some(connector);
        self
    }

    pub fn extension(mut self, extension: E) -> Self {
        self.extension = extension;
        self
    }

    pub fn subprotocols<I>(mut self, subprotocols: I) -> Self
    where
        I: IntoIterator<Item = &'static str>,
    {
        self.subprotocols = ProtocolRegistry::new(subprotocols);
        self
    }
}

#[derive(Default)]
pub struct WebSocketServerBuilder {
    config: Option<WebSocketConfig>,
    interceptor: Option<Box<dyn Interceptor>>,
    protocols: Option<Vec<&'static str>>,
}

impl WebSocketServerBuilder {
    pub async fn accept<S>(self, _stream: S) -> Result<Self, Error>
    where
        S: WebSocketStream,
    {
        unimplemented!()
    }

    pub fn config(mut self, config: WebSocketConfig) -> Self {
        self.config = Some(config);
        self
    }

    pub fn protocols(mut self, protocols: Vec<&'static str>) -> Self {
        self.protocols = Some(protocols);
        self
    }

    pub fn interceptor<I>(mut self, interceptor: I) -> Self
    where
        I: Interceptor + 'static,
    {
        self.interceptor = Some(Box::new(interceptor));
        self
    }
}
