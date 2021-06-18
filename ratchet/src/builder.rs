use crate::{ConnectionError, Interceptor, TryIntoRequest, WebSocketConfig, WebSocketStream};
use futures::future::BoxFuture;
use http::{Request, Response};
use tokio_native_tls::native_tls::TlsConnector;

/// This gives the flexibility to build websockets in a 'friendlier' fashion as opposed to having
/// a bunch of functions like `connect_with_config_and_stream` and `connect_with_config` etc.
#[derive(Default)]
pub struct WebSocketClient {
    config: Option<WebSocketConfig>,
    stream: Option<Box<dyn WebSocketStream>>,
    connector: Option<TlsConnector>,
}

impl WebSocketClient {
    pub async fn subscribe<I>(self, request: I) -> Result<Self, ConnectionError>
    where
        I: TryIntoRequest,
    {
        let WebSocketClient {
            config,
            stream,
            connector,
        } = self;
        let request = request.try_into_request()?;

        // Then it'd be built something like...
        // initialise defaults
        // WebSocket::client(config, stream, connector, request).await
        unimplemented!()
    }

    pub fn config(mut self, config: WebSocketConfig) -> Self {
        self.config = Some(config);
        self
    }

    pub fn stream(mut self, config: WebSocketConfig) -> Self {
        self.config = Some(config);
        self
    }

    pub fn tls_connector(mut self, connector: TlsConnector) -> Self {
        self.connector = Some(connector);
        self
    }
}

#[derive(Default)]
struct WebSocketServer {
    config: Option<WebSocketConfig>,
    interceptor: Option<Box<dyn Interceptor>>,
}

impl WebSocketServer {
    pub async fn accept<S>(self, stream: S) -> Result<Self, ConnectionError>
    where
        S: WebSocketStream,
    {
        let WebSocketServer {
            config,
            interceptor,
        } = self;

        // Then it'd be built something like...
        // initialise defaults
        // WebSocket::server(config, stream, interceptor).await
        unimplemented!()
    }

    pub fn config(mut self, config: WebSocketConfig) -> Self {
        self.config = Some(config);
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
