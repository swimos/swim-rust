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

use crate::errors::Error;
use crate::ext::NoExtProvider;
use crate::handshake::{ProtocolRegistry, UpgradedServer};
use crate::{subscribe_with, TryIntoRequest, UpgradedClient, WebSocketConfig, WebSocketStream};
use ratchet_ext::ExtensionProvider;
use std::borrow::Cow;

/// A builder to construct WebSocket clients.
///
/// If a lot of connections will be negotiated it is more efficient to directly use `subscribe_with`
/// than this builder as it is possible for `subscribe_with` to borrow the extension provider and
/// protocol registry.
#[derive(Debug)]
pub struct WebSocketClientBuilder<E> {
    config: Option<WebSocketConfig>,
    extension: E,
    subprotocols: ProtocolRegistry,
}

impl Default for WebSocketClientBuilder<NoExtProvider> {
    fn default() -> Self {
        WebSocketClientBuilder {
            config: None,
            extension: NoExtProvider,
            subprotocols: ProtocolRegistry::default(),
        }
    }
}

impl<E> WebSocketClientBuilder<E> {
    /// Attempt to execute a client handshake
    pub async fn subscribe<S, I>(
        self,
        stream: S,
        request: I,
    ) -> Result<UpgradedClient<S, E::Extension>, Error>
    where
        S: WebSocketStream,
        I: TryIntoRequest,
        E: ExtensionProvider,
    {
        let WebSocketClientBuilder {
            config,
            extension,
            subprotocols,
            ..
        } = self;
        let request = request.try_into_request()?;

        subscribe_with(
            config.unwrap_or_default(),
            stream,
            request,
            &extension,
            subprotocols,
        )
        .await
    }

    /// Sets the configuration that will be used for the connection.
    pub fn config(mut self, config: WebSocketConfig) -> Self {
        self.config = Some(config);
        self
    }

    /// Sets the extension that will be used for the connection.
    pub fn extension<T>(self, extension: T) -> WebSocketClientBuilder<T>
    where
        T: ExtensionProvider,
    {
        let WebSocketClientBuilder {
            config,
            subprotocols,
            ..
        } = self;
        WebSocketClientBuilder {
            config,
            extension,
            subprotocols,
        }
    }

    /// Sets the subprotocols that will be used for the connection.
    pub fn subprotocols<I>(mut self, subprotocols: I) -> Self
    where
        I: IntoIterator,
        I::Item: Into<Cow<'static, str>>,
    {
        self.subprotocols = ProtocolRegistry::new(subprotocols);
        self
    }
}

/// A builder to construct WebSocket servers.
///
/// If a lot of connections will be negotiated it is more efficient to directly use `accept_with`
/// than this builder as it is possible for `accept_with` to borrow the extension provider and
/// protocol registry.
#[derive(Debug)]
pub struct WebSocketServerBuilder<E> {
    config: Option<WebSocketConfig>,
    subprotocols: ProtocolRegistry,
    extension: E,
}

impl Default for WebSocketServerBuilder<NoExtProvider> {
    fn default() -> Self {
        WebSocketServerBuilder {
            config: None,
            extension: NoExtProvider,
            subprotocols: ProtocolRegistry::default(),
        }
    }
}

impl<E> WebSocketServerBuilder<E> {
    /// Accept `stream` and perform a server WebSocket handshake.
    pub async fn accept<S>(self, stream: S) -> Result<UpgradedServer<S, E::Extension>, Error>
    where
        S: WebSocketStream,
        E: ExtensionProvider,
    {
        let WebSocketServerBuilder {
            config,
            subprotocols,
            extension,
        } = self;
        let upgrader =
            crate::accept_with(stream, config.unwrap_or_default(), extension, subprotocols).await?;
        upgrader.upgrade().await
    }

    /// Sets the configuration that will be used for the connection.
    pub fn config(mut self, config: WebSocketConfig) -> Self {
        self.config = Some(config);
        self
    }

    /// Sets the extension that will be used for the connection.
    pub fn extension<T>(self, extension: T) -> WebSocketServerBuilder<T>
    where
        T: ExtensionProvider,
    {
        let WebSocketServerBuilder {
            config,
            subprotocols,
            ..
        } = self;
        WebSocketServerBuilder {
            config,
            extension,
            subprotocols,
        }
    }

    /// Sets the subprotocols that will be used for the connection.
    pub fn subprotocols<I>(mut self, subprotocols: I) -> Self
    where
        I: IntoIterator,
        I::Item: Into<Cow<'static, str>>,
    {
        self.subprotocols = ProtocolRegistry::new(subprotocols);
        self
    }
}
