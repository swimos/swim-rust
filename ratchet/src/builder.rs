use crate::errors::Error;
use crate::extensions::ext::{NoExt, NoExtProxy};
use crate::extensions::{Extension, ExtensionHandshake};
use crate::{Interceptor, TryIntoRequest, WebSocket, WebSocketConfig, WebSocketStream};
use tokio_native_tls::TlsConnector;

/// This gives the flexibility to build websockets in a 'friendlier' fashion as opposed to having
/// a bunch of functions like `connect_with_config_and_stream` and `connect_with_config` etc.
pub struct WebSocketClientBuilder<E> {
    config: Option<WebSocketConfig>,
    connector: Option<TlsConnector>,
    extension: E,
}

impl Default for WebSocketClientBuilder<NoExtProxy> {
    fn default() -> Self {
        WebSocketClientBuilder {
            config: None,
            connector: None,
            extension: NoExtProxy,
        }
    }
}

impl<E: ExtensionHandshake> WebSocketClientBuilder<E> {
    pub async fn subscribe<S, I>(self, stream: S, request: I) -> Result<WebSocket<S>, Error>
    where
        S: WebSocketStream,
        I: TryIntoRequest,
    {
        let WebSocketClientBuilder {
            config,
            connector,
            extension,
        } = self;
        let request = request.try_into_request()?;

        WebSocket::client(
            config.unwrap_or_default(),
            stream,
            connector,
            request,
            extension,
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
        // Then it'd be built something like...
        // initialise defaults
        // WebSocket::server(config, stream, interceptor, protocol).await
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
