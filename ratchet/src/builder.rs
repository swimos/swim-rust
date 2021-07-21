use crate::extensions::ext::NoExtProxy;
use crate::extensions::ExtensionHandshake;
use crate::{client, Interceptor, TryIntoRequest, WebSocket, WebSocketConfig, WebSocketStream, Error};
use tokio_native_tls::TlsConnector;

/// This gives the flexibility to build websockets in a 'friendlier' fashion as opposed to having
/// a bunch of functions like `connect_with_config_and_stream` and `connect_with_config` etc.
pub struct WebSocketClientBuilder<E> {
    config: Option<WebSocketConfig>,
    connector: Option<TlsConnector>,
    extension: E,
    subprotocols: Option<Vec<&'static str>>,
}

impl Default for WebSocketClientBuilder<NoExtProxy> {
    fn default() -> Self {
        WebSocketClientBuilder {
            config: None,
            connector: None,
            extension: NoExtProxy,
            subprotocols: None,
        }
    }
}

impl<E: ExtensionHandshake> WebSocketClientBuilder<E> {
    pub async fn subscribe<S, I>(
        self,
        _stream: S,
        _request: I,
    ) -> Result<(WebSocket<S, E::Extension>, Option<String>), Error>
        where
            S: WebSocketStream,
            I: TryIntoRequest,
    {
        unimplemented!()
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

    pub fn subprotocols<I>(mut self, subprotocols: Vec<&'static str>) -> Self
    {
        self.subprotocols = Some(subprotocols);
        self
    }
}

#[derive(Default)]
pub struct WebSocketServerBuilder {
    config: Option<WebSocketConfig>,
    interceptor: Option<Box<dyn Interceptor>>,
    subprotocols: Option<Vec<&'static str>>,
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
        self.subprotocols = Some(protocols);
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
