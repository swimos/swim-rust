// Copyright 2015-2023 Swim Inc.
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

#[cfg(not(feature = "deflate"))]
use ratchet::NoExtProvider;
use ratchet::WebSocketStream;
use std::marker::PhantomData;
use std::num::NonZeroUsize;
use swimos_remote::ws::RatchetClient;

use futures_util::future::BoxFuture;
#[cfg(feature = "deflate")]
use ratchet::deflate::{DeflateConfig, DeflateExtProvider};
use runtime::{
    start_runtime, ClientConfig, DownlinkRuntimeError, RawHandle, Transport, WebSocketConfig,
};
pub use runtime::{CommandError, Commander, RemotePath};
use std::sync::Arc;
pub use swimos_api::downlink::DownlinkConfig;
pub use swimos_downlink::lifecycle::{
    BasicEventDownlinkLifecycle, BasicMapDownlinkLifecycle, BasicValueDownlinkLifecycle,
    EventDownlinkLifecycle, MapDownlinkLifecycle, ValueDownlinkLifecycle,
};
use swimos_downlink::{
    ChannelError, DownlinkTask, EventDownlinkModel, MapDownlinkHandle, MapDownlinkModel, MapKey,
    MapValue, NotYetSyncedError, ValueDownlinkModel, ValueDownlinkSet,
};
use swimos_form::Form;
use swimos_remote::net::dns::Resolver;
use swimos_remote::net::plain::TokioPlainTextNetworking;
use swimos_remote::net::ClientConnections;
use swimos_runtime::downlink::{DownlinkOptions, DownlinkRuntimeConfig};
#[cfg(feature = "tls")]
use swimos_tls::{ClientConfig as TlsConfig, RustlsClientNetworking, TlsError};
use swimos_utilities::trigger;
use swimos_utilities::trigger::promise;
use tokio::sync::mpsc;
use tokio::sync::mpsc::error::SendError;
use tokio::sync::oneshot::error::RecvError;
pub use url::Url;

pub type DownlinkOperationResult<T> = Result<T, DownlinkRuntimeError>;

#[derive(Debug, Default)]
pub struct SwimClientBuilder {
    config: ClientConfig,
}

impl SwimClientBuilder {
    pub fn new(config: ClientConfig) -> SwimClientBuilder {
        SwimClientBuilder { config }
    }

    /// Sets the websocket configuration.
    pub fn set_websocket_config(mut self, to: WebSocketConfig) -> SwimClientBuilder {
        self.config.websocket = to;
        self
    }

    /// Size of the buffers to communicate with the socket.
    pub fn set_remote_buffer_size(mut self, to: NonZeroUsize) -> SwimClientBuilder {
        self.config.remote_buffer_size = to;
        self
    }

    /// Sets the buffer size between the runtime and transport tasks.
    pub fn set_transport_buffer_size(mut self, to: NonZeroUsize) -> SwimClientBuilder {
        self.config.transport_buffer_size = to;
        self
    }

    /// Sets the deflate extension configuration for WebSocket connections.
    #[cfg(feature = "deflate")]
    pub fn set_deflate_config(mut self, to: DeflateConfig) -> SwimClientBuilder {
        self.config.websocket.deflate_config = Some(to);
        self
    }

    /// Builds the client.
    pub async fn build(self) -> (SwimClient, BoxFuture<'static, ()>) {
        let SwimClientBuilder { config } = self;
        open_client(
            config,
            TokioPlainTextNetworking::new(Arc::new(Resolver::new().await)),
        )
        .await
    }

    /// Builds the client using the provided TLS configuration.
    #[cfg(feature = "tls")]
    pub async fn build_tls(
        self,
        tls_config: TlsConfig,
    ) -> Result<(SwimClient, BoxFuture<'static, ()>), TlsError> {
        let SwimClientBuilder { config } = self;
        Ok(open_client(
            config,
            RustlsClientNetworking::try_from_config(Arc::new(Resolver::new().await), tls_config)?,
        )
        .await)
    }
}

async fn open_client<Net>(
    config: ClientConfig,
    networking: Net,
) -> (SwimClient, BoxFuture<'static, ()>)
where
    Net: ClientConnections,
    Net::ClientSocket: WebSocketStream,
{
    let ClientConfig {
        websocket,
        remote_buffer_size,
        transport_buffer_size,
        registration_buffer_size,
        close_timeout,
        interpret_frame_data,
        ..
    } = config;

    let (stop_tx, stop_rx) = trigger::trigger();

    let (handle, task) = {
        #[cfg(feature = "deflate")]
        {
            let websockets = RatchetClient::from(ratchet::WebSocketConfig {
                max_message_size: websocket.max_message_size,
            });

            let provider =
                DeflateExtProvider::with_config(websocket.deflate_config.unwrap_or_default());

            start_runtime(
                registration_buffer_size,
                stop_rx,
                Transport::new(
                    networking,
                    websockets,
                    provider,
                    remote_buffer_size,
                    close_timeout,
                ),
                transport_buffer_size,
                interpret_frame_data,
            )
        }
        #[cfg(not(feature = "deflate"))]
        {
            let websockets = RatchetClient::from(ratchet::WebSocketConfig {
                max_message_size: websocket.max_message_size,
            });

            start_runtime(
                registration_buffer_size,
                stop_rx,
                Transport::new(
                    networking,
                    websockets,
                    NoExtProvider,
                    remote_buffer_size,
                    close_timeout,
                ),
                transport_buffer_size,
                interpret_frame_data,
            )
        }
    };

    let client = SwimClient {
        stop_tx,
        handle: ClientHandle {
            inner: Arc::new(handle),
        },
    };
    (client, task)
}

#[derive(Debug)]
pub struct SwimClient {
    stop_tx: trigger::Sender,
    handle: ClientHandle,
}

impl SwimClient {
    /// Returns a reference to a cloneable handle which may be used to open downlinks.
    pub fn handle(&self) -> ClientHandle {
        self.handle.clone()
    }

    /// Triggers the runtime to shutdown and awaits its competition.
    pub async fn shutdown(self) {
        self.stop_tx.trigger();
        self.handle.completed().await;
    }
}

/// A handle to the downlink runtime.
#[derive(Debug, Clone)]
pub struct ClientHandle {
    inner: Arc<RawHandle>,
}

impl ClientHandle {
    /// Returns a future that completes when the runtime has shutdown.
    pub async fn completed(&self) {
        self.inner.completed().await;
    }

    /// Returns a value downlink builder initialised with the default options.
    ///
    /// # Arguments
    /// * `path` - The path of the downlink top open.
    pub fn value_downlink<T>(
        &self,
        path: RemotePath,
    ) -> ValueDownlinkBuilder<'_, BasicValueDownlinkLifecycle<T>> {
        ValueDownlinkBuilder {
            handle: self,
            lifecycle: BasicValueDownlinkLifecycle::default(),
            path,
            options: DownlinkOptions::SYNC,
            runtime_config: Default::default(),
            downlink_config: Default::default(),
        }
    }

    /// Returns an event downlink builder initialised with the default options.
    ///
    /// # Arguments
    /// * `path` - The path of the downlink top open.
    pub fn event_downlink<T>(
        &self,
        path: RemotePath,
    ) -> EventDownlinkBuilder<'_, BasicEventDownlinkLifecycle<T>> {
        EventDownlinkBuilder {
            handle: self,
            lifecycle: BasicEventDownlinkLifecycle::default(),
            path,
            options: DownlinkOptions::SYNC,
            runtime_config: Default::default(),
            downlink_config: Default::default(),
        }
    }

    /// Returns a map downlink builder initialised with the default options.
    ///
    /// # Arguments
    /// * `path` - The path of the downlink top open.
    pub fn map_downlink<K, V>(
        &self,
        path: RemotePath,
    ) -> MapDownlinkBuilder<'_, BasicMapDownlinkLifecycle<K, V>> {
        MapDownlinkBuilder {
            handle: self,
            lifecycle: BasicMapDownlinkLifecycle::default(),
            path,
            options: DownlinkOptions::SYNC,
            runtime_config: Default::default(),
            downlink_config: Default::default(),
        }
    }
}

/// A builder for value downlinks.
pub struct ValueDownlinkBuilder<'h, L> {
    handle: &'h ClientHandle,
    lifecycle: L,
    path: RemotePath,
    options: DownlinkOptions,
    runtime_config: DownlinkRuntimeConfig,
    downlink_config: DownlinkConfig,
}

impl<'h, L> ValueDownlinkBuilder<'h, L> {
    /// Sets a new lifecycle that to be used.
    pub fn lifecycle<NL>(self, lifecycle: NL) -> ValueDownlinkBuilder<'h, NL> {
        let ValueDownlinkBuilder {
            handle,
            path,
            options,
            runtime_config,
            downlink_config,
            ..
        } = self;
        ValueDownlinkBuilder {
            handle,
            lifecycle,
            path,
            options,
            runtime_config,
            downlink_config,
        }
    }

    /// Sets link options for the downlink.
    pub fn options(mut self, options: DownlinkOptions) -> Self {
        self.options = options;
        self
    }

    /// Sets a new downlink runtime configuration.
    pub fn runtime_config(mut self, config: DownlinkRuntimeConfig) -> Self {
        self.runtime_config = config;
        self
    }

    /// Sets a new downlink configuration.
    pub fn downlink_config(mut self, config: DownlinkConfig) -> Self {
        self.downlink_config = config;
        self
    }

    /// Attempts to open the downlink.
    pub async fn open<T>(self) -> Result<ValueDownlinkView<T>, Arc<DownlinkRuntimeError>>
    where
        L: ValueDownlinkLifecycle<T> + Sync + 'static,
        T: Send + Sync + Form + Clone + 'static,
        T::Rec: Send,
    {
        let ValueDownlinkBuilder {
            handle,
            lifecycle,
            path,
            options,
            runtime_config,
            downlink_config,
        } = self;
        let (handle_tx, handle_rx) = mpsc::channel(downlink_config.buffer_size.get());
        let task = DownlinkTask::new(ValueDownlinkModel::new(handle_rx, lifecycle));
        let stop_rx = handle
            .inner
            .run_downlink(path, runtime_config, downlink_config, options, task)
            .await?;

        Ok(ValueDownlinkView {
            tx: handle_tx,
            stop_rx,
        })
    }
}

#[derive(Debug)]
pub enum ValueDownlinkOperationError {
    NotYetSynced,
    DownlinkStopped,
}

impl<T> From<SendError<T>> for ValueDownlinkOperationError {
    fn from(_: SendError<T>) -> Self {
        ValueDownlinkOperationError::DownlinkStopped
    }
}

impl From<RecvError> for ValueDownlinkOperationError {
    fn from(_: RecvError) -> Self {
        ValueDownlinkOperationError::DownlinkStopped
    }
}

impl From<NotYetSyncedError> for ValueDownlinkOperationError {
    fn from(_: NotYetSyncedError) -> Self {
        ValueDownlinkOperationError::NotYetSynced
    }
}

/// A view over a value downlink.
#[derive(Debug, Clone)]
pub struct ValueDownlinkView<T> {
    tx: mpsc::Sender<ValueDownlinkSet<T>>,
    stop_rx: promise::Receiver<Result<(), DownlinkRuntimeError>>,
}

impl<T> ValueDownlinkView<T> {
    /// Sets the value of the downlink to 'to'
    pub async fn set(&self, to: T) -> Result<(), ValueDownlinkOperationError> {
        self.tx.send(ValueDownlinkSet { to }).await?;
        Ok(())
    }

    /// Returns a receiver that completes with the result of downlink's internal task.
    pub fn stop_notification(&self) -> promise::Receiver<Result<(), DownlinkRuntimeError>> {
        self.stop_rx.clone()
    }
}

/// A builder for map downlinks.
pub struct MapDownlinkBuilder<'h, L> {
    handle: &'h ClientHandle,
    lifecycle: L,
    path: RemotePath,
    options: DownlinkOptions,
    runtime_config: DownlinkRuntimeConfig,
    downlink_config: DownlinkConfig,
}

impl<'h, L> MapDownlinkBuilder<'h, L> {
    /// Sets a new lifecycle that to be used.
    pub fn lifecycle<NL>(self, lifecycle: NL) -> MapDownlinkBuilder<'h, NL> {
        let MapDownlinkBuilder {
            handle,
            path,
            options,
            runtime_config,
            downlink_config,
            ..
        } = self;
        MapDownlinkBuilder {
            handle,
            lifecycle,
            path,
            options,
            runtime_config,
            downlink_config,
        }
    }

    /// Sets link options for the downlink.
    pub fn options(mut self, options: DownlinkOptions) -> Self {
        self.options = options;
        self
    }

    /// Sets a new downlink runtime configuration.
    pub fn runtime_config(mut self, config: DownlinkRuntimeConfig) -> Self {
        self.runtime_config = config;
        self
    }

    /// Sets a new downlink configuration.
    pub fn downlink_config(mut self, config: DownlinkConfig) -> Self {
        self.downlink_config = config;
        self
    }

    /// Attempts to open the downlink.
    pub async fn open<K, V>(self) -> Result<MapDownlinkView<K, V>, Arc<DownlinkRuntimeError>>
    where
        L: MapDownlinkLifecycle<K, V> + Sync + 'static,
        K: MapKey,
        V: MapValue,
        K::Rec: Send,
        V::Rec: Send,
        K::BodyRec: Send,
        V::BodyRec: Send,
    {
        let MapDownlinkBuilder {
            handle,
            lifecycle,
            path,
            options,
            runtime_config,
            downlink_config,
        } = self;

        let (tx, rx) = mpsc::channel(downlink_config.buffer_size.get());
        let task = DownlinkTask::new(MapDownlinkModel::new(rx, lifecycle));
        let stop_rx = handle
            .inner
            .run_downlink(path, runtime_config, downlink_config, options, task)
            .await?;

        Ok(MapDownlinkView {
            inner: MapDownlinkHandle::new(tx),
            stop_rx,
        })
    }
}

/// A view over a map downlink.
#[derive(Debug, Clone)]
pub struct MapDownlinkView<K, V> {
    inner: MapDownlinkHandle<K, V>,
    stop_rx: promise::Receiver<Result<(), DownlinkRuntimeError>>,
}

impl<K, V> MapDownlinkView<K, V> {
    /// Updates or inserts the key-value pair into the map.
    pub async fn update(&self, key: K, value: V) -> Result<(), ChannelError> {
        self.inner.update(key, value).await
    }

    /// Removes the value corresponding to the key.
    pub async fn remove(&self, key: K) -> Result<(), ChannelError> {
        self.inner.remove(key).await
    }

    /// Clears the map, removing all of the elements.
    pub async fn clear(&self) -> Result<(), ChannelError> {
        self.inner.clear().await
    }

    /// Retains the last `n` elements in the map.
    pub async fn take(&self, n: u64) -> Result<(), ChannelError> {
        self.inner.take(n).await
    }

    /// Retains the first `n` elements in the map.
    pub async fn drop(&self, n: u64) -> Result<(), ChannelError> {
        self.inner.drop(n).await
    }

    /// Returns a receiver that completes with the result of downlink's internal task.
    pub fn stop_notification(&self) -> promise::Receiver<Result<(), DownlinkRuntimeError>> {
        self.stop_rx.clone()
    }
}

/// A builder for value downlinks.
pub struct EventDownlinkBuilder<'h, L> {
    handle: &'h ClientHandle,
    lifecycle: L,
    path: RemotePath,
    options: DownlinkOptions,
    runtime_config: DownlinkRuntimeConfig,
    downlink_config: DownlinkConfig,
}

impl<'h, L> EventDownlinkBuilder<'h, L> {
    /// Sets a new lifecycle that to be used.
    pub fn lifecycle<NL>(self, lifecycle: NL) -> EventDownlinkBuilder<'h, NL> {
        let EventDownlinkBuilder {
            handle,
            path,
            options,
            runtime_config,
            downlink_config,
            ..
        } = self;
        EventDownlinkBuilder {
            handle,
            lifecycle,
            path,
            options,
            runtime_config,
            downlink_config,
        }
    }

    /// Sets link options for the downlink.
    pub fn options(mut self, options: DownlinkOptions) -> Self {
        self.options = options;
        self
    }

    /// Sets a new downlink runtime configuration.
    pub fn runtime_config(mut self, config: DownlinkRuntimeConfig) -> Self {
        self.runtime_config = config;
        self
    }

    /// Sets a new downlink configuration.
    pub fn downlink_config(mut self, config: DownlinkConfig) -> Self {
        self.downlink_config = config;
        self
    }

    /// Attempts to open the downlink.
    pub async fn open<T>(self) -> Result<EventDownlinkView<T>, Arc<DownlinkRuntimeError>>
    where
        L: EventDownlinkLifecycle<T> + Sync + 'static,
        T: Send + Sync + Form + Clone + 'static,
        T::Rec: Send,
    {
        let EventDownlinkBuilder {
            handle,
            lifecycle,
            path,
            options,
            runtime_config,
            downlink_config,
        } = self;
        let task = DownlinkTask::new(EventDownlinkModel::new(lifecycle));
        let stop_rx = handle
            .inner
            .run_downlink(path, runtime_config, downlink_config, options, task)
            .await?;

        Ok(EventDownlinkView {
            _type: Default::default(),
            stop_rx,
        })
    }
}

/// An event downlink handle which provides the functionality to await the downlink terminating.
#[derive(Debug, Clone)]
pub struct EventDownlinkView<T> {
    _type: PhantomData<T>,
    stop_rx: promise::Receiver<Result<(), DownlinkRuntimeError>>,
}

impl<T> EventDownlinkView<T> {
    /// Returns a receiver that completes with the result of downlink's internal task.
    pub fn stop_notification(&self) -> promise::Receiver<Result<(), DownlinkRuntimeError>> {
        self.stop_rx.clone()
    }
}
