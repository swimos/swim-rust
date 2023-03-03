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
use runtime::downlink::{DownlinkOperationResult, StatefulDownlinkView};

use runtime::runtime::{start_runtime, DownlinkRuntimeError, RawHandle, RemotePath, Transport};
use runtime::ClientConfig;
use std::sync::Arc;
use swim_api::downlink::DownlinkConfig;
use swim_downlink::lifecycle::{BasicValueDownlinkLifecycle, ValueDownlinkLifecycle};
use swim_downlink::{DownlinkTask, ValueDownlinkModel};
use swim_form::Form;
use swim_runtime::downlink::{DownlinkOptions, DownlinkRuntimeConfig};
use swim_runtime::net::dns::Resolver;
use swim_runtime::net::plain::TokioPlainTextNetworking;
use swim_runtime::ws::ext::RatchetNetworking;
use swim_utilities::trigger;
use swim_utilities::trigger::promise;
use tokio::sync::{mpsc, watch};
pub use url::Url;

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
            ..
        } = config;

        let websockets = RatchetNetworking {
            config: websocket,
            provider: NoExtProvider,
            subprotocols: Default::default(),
        };
        let networking = TokioPlainTextNetworking::new(Arc::new(Resolver::new().await));
        let (handle, stop_tx) =
            start_runtime(Transport::new(networking, websockets, remote_buffer_size));
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
            inner: StatefulDownlinkView {
                current,
                tx: set_tx,
                rx: get_rx,
                stop_rx,
            },
        })
    }
}

/// A view over a value downlink.
#[derive(Debug, Clone)]
pub struct ValueDownlinkView<T> {
    inner: StatefulDownlinkView<T>,
}

impl<T> ValueDownlinkView<T> {
    /// Sets the state of the value downlink to 'to'.
    pub async fn set(&self, to: T) -> DownlinkOperationResult<()> {
        self.inner.set(to).await
    }

    /// Returns a shared view of the most-recent state of this downlink.
    pub fn get(&mut self) -> DownlinkOperationResult<Arc<T>> {
        self.inner.get()
    }

    /// Returns a receiver that completes with the result of downlink's internal task.
    pub fn stop_notification(&self) -> promise::Receiver<Result<(), DownlinkRuntimeError>> {
        self.inner.stop_notification()
    }
}
