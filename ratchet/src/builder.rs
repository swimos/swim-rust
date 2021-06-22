use crate::error::ConnectionError;
use crate::{Interceptor, TryIntoRequest, WebSocket, WebSocketConfig, WebSocketStream};
use futures::future::BoxFuture;
use http::{Request, Response};
use tokio_native_tls::TlsConnector;

/// This gives the flexibility to build websockets in a 'friendlier' fashion as opposed to having
/// a bunch of functions like `connect_with_config_and_stream` and `connect_with_config` etc.
#[derive(Default)]
pub struct WebSocketClientBuilder {
    config: Option<WebSocketConfig>,
    connector: Option<TlsConnector>,
}

impl WebSocketClientBuilder {
    pub async fn subscribe<S, I>(
        self,
        stream: S,
        request: I,
    ) -> Result<WebSocket<S>, ConnectionError>
    where
        S: WebSocketStream,
        I: TryIntoRequest,
    {
        let WebSocketClientBuilder { config, connector } = self;
        let request = request.try_into_request()?;

        WebSocket::client(config.unwrap_or_default(), stream, connector, request).await
    }

    pub fn config(mut self, config: WebSocketConfig) -> Self {
        self.config = Some(config);
        self
    }

    pub fn tls_connector(mut self, connector: TlsConnector) -> Self {
        self.connector = Some(connector);
        self
    }
}

#[derive(Default)]
struct WebSocketServerBuilder {
    config: Option<WebSocketConfig>,
    interceptor: Option<Box<dyn Interceptor>>,
    protocols: Option<Vec<&'static str>>,
}

impl WebSocketServerBuilder {
    pub async fn accept<S>(self, stream: S) -> Result<Self, ConnectionError>
    where
        S: WebSocketStream,
    {
        let WebSocketServerBuilder {
            config,
            interceptor,
            protocols,
        } = self;

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
