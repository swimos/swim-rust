// Copyright 2015-2021 SWIM.AI inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

use super::{client, WebSocket};
use crate::codec::{Codec, FragmentBuffer};
use crate::errors::Error;
use crate::extensions::ext::NoExtProxy;
use crate::extensions::ExtensionHandshake;
use crate::handshake::ProtocolRegistry;
use crate::Role;
pub use crate::{Interceptor, TryIntoRequest, WebSocketConfig, WebSocketStream};
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

impl<E> WebSocketClientBuilder<E>
where
    E: ExtensionHandshake,
{
    pub fn for_extension(extension: E) -> Self {
        WebSocketClientBuilder {
            config: None,
            connector: None,
            extension,
            subprotocols: Default::default(),
        }
    }

    pub async fn subscribe<S, I>(
        self,
        stream: S,
        request: I,
    ) -> Result<(WebSocket<S, E::Extension>, Option<String>), Error>
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
            Codec::new(Role::Client, usize::MAX, FragmentBuffer::new(usize::MAX)), // todo from config
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
