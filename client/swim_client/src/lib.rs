// Copyright 2015-2021 Swim Inc.
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

use ratchet::NoExtProvider;
use std::collections::BTreeMap;
use std::ops::Deref;

use runtime::{
    start_runtime, ClientConfig, DownlinkRuntimeError, RawHandle, RemotePath, Transport,
};
use std::path::PathBuf;
use std::sync::Arc;
use swim_api::downlink::DownlinkConfig;
use swim_downlink::lifecycle::{
    BasicMapDownlinkLifecycle, BasicValueDownlinkLifecycle, MapDownlinkLifecycle,
    ValueDownlinkLifecycle,
};
use swim_downlink::{
    ChannelError, DownlinkTask, MapDownlinkHandle, MapDownlinkModel, MapKey, MapValue,
    ValueDownlinkModel,
};
use swim_form::Form;
use swim_runtime::downlink::{DownlinkOptions, DownlinkRuntimeConfig};
use swim_runtime::net::dns::Resolver;
use swim_runtime::net::tls::TokioTlsNetworking;
use swim_runtime::ws::ext::RatchetNetworking;
use swim_utilities::trigger;
use swim_utilities::trigger::promise;
use tokio::sync::{mpsc, watch};
pub use url::Url;

pub type DownlinkOperationResult<T> = Result<T, DownlinkRuntimeError>;

#[derive(Debug)]
pub struct SwimClient {
    stop_tx: trigger::Sender,
    handle: ClientHandle,
}

impl SwimClient {
    /// Returns a new client that has been built with the default configuration.
    pub async fn new() -> SwimClient {
        SwimClient::with_config(ClientConfig::default()).await
    }

    /// Returns a new client that has been built with the provided configuration.
    pub async fn with_config(config: ClientConfig) -> SwimClient {
        let ClientConfig {
            websocket,
            remote_buffer_size,
            interpret_frame_data,
            ..
        } = config;

        let websockets = RatchetNetworking {
            config: websocket,
            provider: NoExtProvider,
            subprotocols: Default::default(),
        };
        let networking = TokioTlsNetworking::new::<_, Box<PathBuf>>(
            std::iter::empty(),
            Arc::new(Resolver::new().await),
        );
        let (handle, stop_tx) = start_runtime(
            Transport::new(networking, websockets, remote_buffer_size),
            interpret_frame_data,
        );
        SwimClient {
            stop_tx,
            handle: ClientHandle {
                inner: Arc::new(handle),
            },
        }
    }

    /// Returns a reference to a cloneable handle which may be used to open downlinks.
    pub fn handle(&self) -> &ClientHandle {
        &self.handle
    }

    /// Triggers the runtime to shutdown.
    pub fn shutdown(self) {
        self.stop_tx.trigger();
    }
}

/// A handle to the downlink runtime.
#[derive(Debug, Clone)]
pub struct ClientHandle {
    inner: Arc<RawHandle>,
}

impl ClientHandle {
    /// Returns a value downlink builder initialised with the default options.
    ///
    /// # Arguments
    /// * `path` - The path of the downlink top open.
    /// * `default` - The initial, local, state of the downlink.
    pub fn value_downlink<L, T>(
        &self,
        path: RemotePath,
        default: T,
    ) -> ValueDownlinkBuilder<'_, BasicValueDownlinkLifecycle<T>, T> {
        ValueDownlinkBuilder {
            handle: self,
            lifecycle: BasicValueDownlinkLifecycle::default(),
            path,
            options: DownlinkOptions::SYNC,
            runtime_config: Default::default(),
            downlink_config: Default::default(),
            default,
        }
    }

    /// Returns a map downlink builder initialised with the default options.
    ///
    /// # Arguments
    /// * `path` - The path of the downlink top open.
    pub fn map_downlink<L, K, V>(
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
pub struct ValueDownlinkBuilder<'h, L, T> {
    handle: &'h ClientHandle,
    lifecycle: L,
    path: RemotePath,
    options: DownlinkOptions,
    runtime_config: DownlinkRuntimeConfig,
    downlink_config: DownlinkConfig,
    default: T,
}

impl<'h, L, T> ValueDownlinkBuilder<'h, L, T> {
    /// Sets a new lifecycle that to be used.
    pub fn lifecycle<NL>(self, lifecycle: NL) -> ValueDownlinkBuilder<'h, NL, T> {
        let ValueDownlinkBuilder {
            handle,
            path,
            options,
            runtime_config,
            downlink_config,
            default,
            ..
        } = self;
        ValueDownlinkBuilder {
            handle,
            lifecycle,
            path,
            options,
            runtime_config,
            downlink_config,
            default,
        }
    }

    /// Sets link options for the downlink.
    pub fn options(&mut self, options: DownlinkOptions) -> &mut Self {
        self.options = options;
        self
    }

    /// Sets a new downlink runtime configuration.
    pub fn runtime_config(&mut self, config: DownlinkRuntimeConfig) -> &mut Self {
        self.runtime_config = config;
        self
    }

    /// Sets a new downlink configuration.
    pub fn downlink_config(&mut self, config: DownlinkConfig) -> &mut Self {
        self.downlink_config = config;
        self
    }

    /// Attempts to open the downlink.
    pub async fn open(self) -> Result<ValueDownlinkView<T>, Arc<DownlinkRuntimeError>>
    where
        L: ValueDownlinkLifecycle<T> + Sync + 'static,
        T: Send + Sync + Form + 'static,
        T::Rec: Send,
    {
        let ValueDownlinkBuilder {
            handle,
            lifecycle,
            path,
            default,
            options,
            runtime_config,
            downlink_config,
        } = self;
        let current = Arc::new(default);
        let (set_tx, set_rx) = mpsc::channel::<T>(downlink_config.buffer_size.get());
        let (get_tx, get_rx) = watch::channel::<Arc<T>>(current.clone());

        let task = DownlinkTask::new(ValueDownlinkModel::new(set_rx, get_tx, lifecycle));
        let stop_rx = handle
            .inner
            .run_downlink(path, runtime_config, downlink_config, options, task)
            .await?;

        Ok(ValueDownlinkView {
            current,
            tx: set_tx,
            rx: get_rx,
            stop_rx,
        })
    }
}

/// A view over a value downlink.
#[derive(Debug, Clone)]
pub struct ValueDownlinkView<T> {
    // This is intentionally its own field as to access it in the watch channel's receiver requires
    // accessing its internal mutex.
    current: Arc<T>,
    rx: watch::Receiver<Arc<T>>,
    tx: mpsc::Sender<T>,
    stop_rx: promise::Receiver<Result<(), DownlinkRuntimeError>>,
}

impl<T> ValueDownlinkView<T> {
    /// Sets the state of the value downlink to 'to'.
    pub async fn set(&self, to: T) -> DownlinkOperationResult<()> {
        self.tx.send(to).await?;
        Ok(())
    }

    /// Returns a shared view of the most-recent state of this downlink.
    pub fn get(&mut self) -> DownlinkOperationResult<Arc<T>> {
        if self.rx.has_changed()? {
            let inner = self.rx.borrow();
            self.current = inner.deref().clone();
        }
        Ok(self.current.clone())
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
    pub fn options(&mut self, options: DownlinkOptions) -> &mut Self {
        self.options = options;
        self
    }

    /// Sets a new downlink runtime configuration.
    pub fn runtime_config(&mut self, config: DownlinkRuntimeConfig) -> &mut Self {
        self.runtime_config = config;
        self
    }

    /// Sets a new downlink configuration.
    pub fn downlink_config(&mut self, config: DownlinkConfig) -> &mut Self {
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
        let task = DownlinkTask::new(MapDownlinkModel::new(rx, lifecycle, true));
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
    /// Returns an owned value corresponding to the key.
    pub async fn get(&self, key: K) -> Result<Option<V>, ChannelError> {
        self.inner.get(key).await
    }

    /// Returns a snapshot of the map downlink's current state.
    pub async fn snapshot(&self) -> Result<BTreeMap<K, V>, ChannelError> {
        self.inner.snapshot().await
    }

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
