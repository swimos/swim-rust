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
use crate::extensions::ext::NoExtProxy;
use crate::handshake::ProtocolRegistry;
use crate::ws::Upgraded;
use crate::ExtensionProvider;
use crate::{client, TryIntoRequest, WebSocketConfig, WebSocketStream};

pub struct WebSocketClientBuilder<E> {
    config: Option<WebSocketConfig>,
    extension: E,
    subprotocols: ProtocolRegistry,
}

impl Default for WebSocketClientBuilder<NoExtProxy> {
    fn default() -> Self {
        WebSocketClientBuilder {
            config: None,
            extension: NoExtProxy,
            subprotocols: ProtocolRegistry::default(),
        }
    }
}

impl<E> WebSocketClientBuilder<E>
where
    E: ExtensionProvider,
{
    pub async fn subscribe<S, I>(
        self,
        stream: S,
        request: I,
    ) -> Result<Upgraded<S, E::Extension>, Error>
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
            extension,
            subprotocols,
        )
        .await
    }

    pub fn config(mut self, config: WebSocketConfig) -> Self {
        self.config = Some(config);
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

pub struct WebSocketServerBuilder<E> {
    config: Option<WebSocketConfig>,
    subprotocols: ProtocolRegistry,
    extension: E,
}

impl Default for WebSocketServerBuilder<NoExtProxy> {
    fn default() -> Self {
        WebSocketServerBuilder {
            config: None,
            extension: NoExtProxy,
            subprotocols: ProtocolRegistry::default(),
        }
    }
}

impl<E> WebSocketServerBuilder<E>
where
    E: ExtensionProvider,
{
    pub async fn accept<S>(self, stream: S) -> Result<Upgraded<S, E::Extension>, Error>
    where
        S: WebSocketStream,
    {
        let WebSocketServerBuilder {
            config,
            subprotocols,
            extension,
        } = self;
        let upgrader =
            crate::accept(stream, config.unwrap_or_default(), extension, subprotocols).await?;
        upgrader.upgrade().await
    }

    pub fn config(mut self, config: WebSocketConfig) -> Self {
        self.config = Some(config);
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
