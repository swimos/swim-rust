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

use ratchet::{NoExtProvider, WebSocketConfig};
use std::future::Future;
use std::num::NonZeroUsize;

#[cfg(feature = "deflate")]
use ratchet::deflate::DeflateConfig;
use runtime::{
    start_runtime, ClientConfig, DownlinkRuntimeError, RawHandle, RemotePath, Transport,
};
use std::sync::Arc;
use swim_api::downlink::DownlinkConfig;
use swim_downlink::lifecycle::{BasicValueDownlinkLifecycle, ValueDownlinkLifecycle};
use swim_downlink::{DownlinkTask, NotYetSyncedError, ValueDownlinkModel, ValueDownlinkOperation};
use swim_form::Form;
use swim_runtime::downlink::{DownlinkOptions, DownlinkRuntimeConfig};
use swim_runtime::net::dns::Resolver;
use swim_runtime::net::plain::TokioPlainTextNetworking;
use swim_runtime::net::ExternalConnections;
use swim_runtime::ws::ext::RatchetNetworking;
use swim_tls::RustlsNetworking;
#[cfg(feature = "tls")]
use swim_tls::{TlsConfig, TlsError};
use swim_utilities::trigger::promise;
use swim_utilities::{non_zero_usize, trigger};
use tokio::sync::mpsc::error::SendError;
use tokio::sync::oneshot::error::RecvError;
use tokio::sync::{mpsc, oneshot};
pub use url::Url;

type ClientTask = impl Future<Output = ()> + Send;

#[derive(Debug, Default)]
pub struct SwimClientBuilder {
    config: ClientConfig,
}

impl SwimClientBuilder {
    pub fn new(config: ClientConfig) -> SwimClientBuilder {
        SwimClientBuilder { config }
    }

    pub fn set_websocket_config(mut self, to: WebSocketConfig) -> SwimClientBuilder {
        self.config.websocket = to;
        self
    }

    pub fn set_remote_buffer_size(mut self, to: NonZeroUsize) -> SwimClientBuilder {
        self.config.remote_buffer_size = to;
        self
    }

    pub fn set_transport_buffer_size(mut self, to: NonZeroUsize) -> SwimClientBuilder {
        self.config.transport_buffer_size = to;
        self
    }

    #[cfg(feature = "deflate")]
    pub fn set_deflate_config(mut self, to: DeflateConfig) -> SwimClientBuilder {
        self.config.deflate = Some(to);
        self
    }

    pub async fn build(self) -> (SwimClient, ClientTask) {
        let SwimClientBuilder { config } = self;
        open_client(
            config,
            TokioPlainTextNetworking::new(Arc::new(Resolver::new().await)),
        )
    }

    #[cfg(feature = "tls")]
    pub async fn build_tls(self, config: TlsConfig) -> Result<SwimClient, TlsError> {
        let SwimClientBuilder { config } = self;
        open_client(
            config,
            RustlsNetworking::try_from_config(resolver, tls_conf)?,
        )
    }
}

async fn open_client<Net>(config: ClientConfig, networking: Net) -> (SwimClient, ClientTask)
where
    Net: ExternalConnections,
{
    let ClientConfig {
        websocket,
        remote_buffer_size,
        transport_buffer_size,
        registration_buffer_size,
        ..
    } = config;

    let websockets = RatchetNetworking {
        config: websocket,
        provider: NoExtProvider,
        subprotocols: Default::default(),
    };
    let (stop_tx, stop_rx) = trigger::trigger();
    let (handle, task) = start_runtime(
        registration_buffer_size,
        stop_rx,
        Transport::new(networking, websockets, remote_buffer_size),
        transport_buffer_size,
    );
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
    /// Returns a new client that has been built with the default configuration.
    pub async fn new() -> (SwimClient, ClientTask) {
        SwimClient::with_config(ClientConfig::default()).await
    }

    /// Returns a new client that has been built with the provided configuration.
    pub async fn with_config(config: ClientConfig) -> (SwimClient, ClientTask) {
        let ClientConfig {
            websocket,
            remote_buffer_size,
            transport_buffer_size,
            registration_buffer_size,
            ..
        } = config;

        let websockets = RatchetNetworking {
            config: websocket,
            provider: NoExtProvider,
            subprotocols: Default::default(),
        };
        let networking = TokioPlainTextNetworking::new(Arc::new(Resolver::new().await));
        let (stop_tx, stop_rx) = trigger::trigger();
        let (handle, task) = start_runtime(
            registration_buffer_size,
            stop_rx,
            Transport::new(networking, websockets, remote_buffer_size),
            transport_buffer_size,
        );
        let client = SwimClient {
            stop_tx,
            handle: ClientHandle {
                inner: Arc::new(handle),
            },
        };
        (client, task)
    }

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
    pub fn value_downlink<L, T>(
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
    tx: mpsc::Sender<ValueDownlinkOperation<T>>,
    stop_rx: promise::Receiver<Result<(), DownlinkRuntimeError>>,
}

impl<T> ValueDownlinkView<T> {
    /// Gets the most recent value of the downlink.
    pub async fn get(&mut self) -> Result<T, ValueDownlinkOperationError> {
        let (tx, rx) = oneshot::channel();
        self.tx.send(ValueDownlinkOperation::Get(tx)).await?;
        rx.await?.map_err(Into::into)
    }

    /// Sets the value of the downlink to 'to'
    pub async fn set(&self, to: T) -> Result<(), ValueDownlinkOperationError> {
        self.tx.send(ValueDownlinkOperation::Set(to)).await?;
        Ok(())
    }

    /// Returns a receiver that completes with the result of downlink's internal task.
    pub fn stop_notification(&self) -> promise::Receiver<Result<(), DownlinkRuntimeError>> {
        self.stop_rx.clone()
    }
}
