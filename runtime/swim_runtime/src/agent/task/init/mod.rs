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

use std::{convert::identity, num::NonZeroUsize, time::Duration};

use futures::{
    future::{join, Either},
    stream::FuturesUnordered,
    Future, FutureExt, Stream, StreamExt, TryFutureExt,
};
use swim_api::{
    agent::{LaneConfig, StoreConfig, UplinkKind},
    error::{AgentRuntimeError, OpenStoreError, StoreError},
    meta::lane::LaneKind,
    protocol::agent::StoreInitializedCodec,
    store::{StoreDisabled, StoreKind},
};
use swim_model::Text;
use swim_utilities::{
    io::byte_channel::{self, ByteReader, ByteWriter},
    trigger,
};
use tokio::sync::{mpsc, oneshot};
use tokio_stream::wrappers::ReceiverStream;
use tokio_util::codec::FramedRead;
use uuid::Uuid;

use crate::agent::{
    store::{
        no_map_init, no_value_init, AgentItemInitError, AgentPersistence, BoxInitializer,
        StoreInitError,
    },
    AgentExecError, AgentRuntimeRequest, Io, LinkRequest, NodeReporting,
};

use super::{
    ad_hoc::{ad_hoc_commands_task, AdHocTaskConfig, AdHocTaskState, NoReport},
    AdHocChannelRequest, InitialEndpoints, ItemEndpoint, ItemInitTask, LaneEndpoint, LaneResult,
    LaneRuntimeSpec, StoreEndpoint, StoreResult, StoreRuntimeSpec,
};

use tracing::{error, info};

#[cfg(test)]
mod tests;

/// Task for the initialization of an agent. While this task is executing, new items can be
/// registered but they will not be driven and no remote connections will exist attached to
/// the agent.
pub struct AgentInitTask<Store = StoreDisabled> {
    identity: Uuid,
    requests: mpsc::Receiver<AgentRuntimeRequest>,
    link_requests: mpsc::Sender<LinkRequest>,
    init_complete: trigger::Receiver,
    config: InitTaskConfig,
    reporting: Option<NodeReporting>,
    store: Store,
}

pub struct InitTaskConfig {
    pub ad_hoc_queue_size: NonZeroUsize,
    pub item_init_timeout: Duration,
    pub ad_hoc: AdHocTaskConfig,
}

impl AgentInitTask {
    /// #Arguments
    /// * `identity` - Unique ID of the agent.
    /// * `requests` - Channel for requests to open new lanes and downlinks.
    /// * `link_requests` - Channel for request to the runtime to open new external links.
    /// * `init_complete` - Triggered when the initialization phase is complete.
    /// * `config` - Task configuration parameters.
    /// * `reporting` - Reporter for node/lane introspection support.
    pub fn new(
        identity: Uuid,
        requests: mpsc::Receiver<AgentRuntimeRequest>,
        link_requests: mpsc::Sender<LinkRequest>,
        init_complete: trigger::Receiver,
        config: InitTaskConfig,
        reporting: Option<NodeReporting>,
    ) -> Self {
        Self::with_store(
            identity,
            requests,
            link_requests,
            init_complete,
            config,
            reporting,
            StoreDisabled::default(),
        )
    }
}

impl<Store> AgentInitTask<Store>
where
    Store: AgentPersistence + Send + Sync,
{
    /// #Arguments
    /// * `identity` - Unique ID of the agent.
    /// * `requests` - Channel for requests to open new lanes and downlinks.
    /// * `link_requests` - Channel for request to the runtime to open external links.
    /// * `init_complete` - Triggered when the initialization phase is complete.
    /// * `config` - Task configuration parameters.
    /// * `reporting` - Reporter for node/lane introspection support.
    /// * `store` - Store for lane persistence.
    pub fn with_store(
        identity: Uuid,
        requests: mpsc::Receiver<AgentRuntimeRequest>,
        link_requests: mpsc::Sender<LinkRequest>,
        init_complete: trigger::Receiver,
        config: InitTaskConfig,
        reporting: Option<NodeReporting>,
        store: Store,
    ) -> Self {
        AgentInitTask {
            identity,
            requests,
            link_requests,
            init_complete,
            config,
            reporting,
            store,
        }
    }
}

impl<Store: AgentPersistence + Send + Sync> AgentInitTask<Store> {
    pub async fn run(self) -> Result<(InitialEndpoints, Store), AgentExecError> {
        let AgentInitTask {
            identity,
            requests,
            init_complete,
            link_requests,
            store,
            config,
            reporting,
        } = self;
        let InitTaskConfig {
            ad_hoc_queue_size,
            item_init_timeout,
            ad_hoc,
        } = config;
        let initialization = Initialization::new(reporting, item_init_timeout);
        let mut request_stream = ReceiverStream::new(requests);
        let terminated = (&mut request_stream).take_until(init_complete);

        let mut lane_endpoints: Vec<LaneEndpoint<Io>> = vec![];
        let mut store_endpoints: Vec<StoreEndpoint> = vec![];

        let (ad_hoc_tx, ad_hoc_rx) = mpsc::channel(ad_hoc_queue_size.get());

        let ad_hoc_state = AdHocTaskState::new(link_requests.clone());

        let ad_hoc_task =
            ad_hoc_commands_task::<NoReport>(identity, ad_hoc_rx, ad_hoc_state, ad_hoc, None);

        let item_init_task = initialize_items(
            &store,
            terminated,
            &link_requests,
            &ad_hoc_tx,
            &initialization,
            &mut lane_endpoints,
            &mut store_endpoints,
        );

        let (result, ad_hoc_state) = join(item_init_task, ad_hoc_task).await;
        result?;

        let Initialization { reporting, .. } = initialization;
        if lane_endpoints.is_empty() {
            Err(AgentExecError::NoInitialLanes)
        } else {
            Ok((
                InitialEndpoints::new(
                    reporting,
                    request_stream.into_inner(),
                    lane_endpoints,
                    store_endpoints,
                    ad_hoc_state,
                ),
                store,
            ))
        }
    }
}

/// Creates futures that will initialize a lane or a store.
pub struct Initialization {
    reporting: Option<NodeReporting>,
    item_init_timeout: Duration,
}

impl Initialization {
    pub fn new(reporting: Option<NodeReporting>, item_init_timeout: Duration) -> Self {
        Initialization {
            reporting,
            item_init_timeout,
        }
    }

    pub fn add_store<'a, Store>(
        &'a self,
        store: &'a Store,
        name: Text,
        kind: StoreKind,
        config: StoreConfig,
        promise: oneshot::Sender<Result<Io, OpenStoreError>>,
    ) -> Result<
        Option<impl Future<Output = StoreResult<Store::StoreId>> + Send + 'a>,
        AgentItemInitError,
    >
    where
        Store: AgentPersistence + Send + Sync + 'a,
    {
        let Initialization {
            item_init_timeout, ..
        } = self;
        let StoreConfig { buffer_size } = config;

        let log_err = || {
            error!(
                "Agent failed to receive store registration for {} store named '{}'.",
                kind, name
            );
        };

        match store.store_id(name.as_str()) {
            Ok(store_id) => {
                let (in_tx, in_rx) = byte_channel::byte_channel(buffer_size);
                let (out_tx, out_rx) = byte_channel::byte_channel(buffer_size);
                let io = (out_tx, in_rx);
                if promise.send(Ok(io)).is_ok() {
                    let initializer = match kind {
                        StoreKind::Value => store
                            .init_value_store(store_id)
                            .unwrap_or_else(|| no_value_init()),
                        StoreKind::Map => store
                            .init_map_store(store_id)
                            .unwrap_or_else(|| no_map_init()),
                    };
                    let init_task = store_initialization(
                        name.clone(),
                        kind,
                        *item_init_timeout,
                        in_tx,
                        out_rx,
                        initializer,
                    )
                    .map_ok(move |endpoint| (endpoint, store_id))
                    .map_err(move |e| AgentItemInitError::new(name, e));
                    Ok(Some(init_task))
                } else {
                    log_err();
                    Ok(None)
                }
            }
            Err(StoreError::NoStoreAvailable) => {
                if promise
                    .send(Err(OpenStoreError::StoresNotSupported))
                    .is_err()
                {
                    log_err();
                }
                Ok(None)
            }
            Err(err) => Err(AgentItemInitError::new(name, StoreInitError::Store(err))),
        }
    }

    pub fn add_lane<'a, Store>(
        &'a self,
        store: &'a Store,
        name: Text,
        kind: LaneKind,
        config: LaneConfig,
        promise: oneshot::Sender<Result<Io, AgentRuntimeError>>,
    ) -> Option<impl Future<Output = LaneResult<Store::StoreId>> + Send + 'a>
    where
        Store: AgentPersistence + Send + Sync + 'a,
    {
        let Initialization {
            reporting,
            item_init_timeout,
        } = self;
        let uplink_kind = kind.uplink_kind();
        let LaneConfig {
            input_buffer_size,
            output_buffer_size,
            transient,
        } = config;

        let (in_tx, in_rx) = byte_channel::byte_channel(input_buffer_size);
        let (out_tx, out_rx) = byte_channel::byte_channel(output_buffer_size);

        let io = (out_tx, in_rx);
        let name_cpy = name.clone();
        if promise.send(Ok(io)).is_ok() {
            Some(
                async move {
                    let get_store_id = || match store.store_id(name.as_str()) {
                        Ok(id) => Ok(Some(id)),
                        Err(StoreError::NoStoreAvailable) => Ok(None),
                        Err(e) => Err(StoreInitError::Store(e)),
                    };
                    let (maybe_store_id, maybe_initializer) = match uplink_kind {
                        UplinkKind::Value if !transient => {
                            let maybe_store_id = get_store_id()?;
                            let init = maybe_store_id
                                .and_then(|lane_id| store.init_value_store(lane_id))
                                .unwrap_or_else(|| no_value_init());
                            (maybe_store_id, Some(init))
                        }
                        UplinkKind::Map if !transient => {
                            let maybe_store_id = get_store_id()?;
                            let init = maybe_store_id
                                .and_then(|lane_id| store.init_map_store(lane_id))
                                .unwrap_or_else(|| no_map_init());
                            (maybe_store_id, Some(init))
                        }
                        _ => (None, None),
                    };
                    if let Some(initializer) = maybe_initializer {
                        let endpoint = lane_initialization(
                            name.clone(),
                            kind,
                            *item_init_timeout,
                            reporting.as_ref(),
                            in_tx,
                            out_rx,
                            initializer,
                        )
                        .await?;
                        Ok((endpoint, maybe_store_id))
                    } else {
                        let reporter = if let Some(node_reporter) = reporting {
                            node_reporter.register(name.clone(), kind).await
                        } else {
                            None
                        };
                        let endpoint = LaneEndpoint {
                            name,
                            kind: kind.uplink_kind(),
                            transient,
                            io: (in_tx, out_rx),
                            reporter,
                        };
                        Ok((endpoint, None))
                    }
                }
                .map_err(move |e| AgentItemInitError::new(name_cpy, e))
                .boxed(),
            )
        } else {
            error!(
                "Agent failed to receive lane registration for {} lane named '{}'.",
                kind, name
            );
            None
        }
    }
}

async fn wait_for_initialized(reader: &mut ByteReader) -> Result<(), StoreInitError> {
    let mut reader = FramedRead::new(reader, StoreInitializedCodec::default());
    match reader.next().await {
        Some(Ok(_)) => Ok(()),
        _ => Err(StoreInitError::NoAckFromItem),
    }
}

async fn lane_initialization(
    name: Text,
    lane_kind: LaneKind,
    timeout: Duration,
    reporting: Option<&NodeReporting>,
    mut in_tx: ByteWriter,
    mut out_rx: ByteReader,
    initializer: BoxInitializer<'_>,
) -> Result<LaneEndpoint<Io>, StoreInitError> {
    let lane_name = name.clone();
    let kind = lane_kind.uplink_kind();
    let result = tokio::time::timeout(timeout, async move {
        let init = initializer.initialize(&mut in_tx);
        init.await?;
        wait_for_initialized(&mut out_rx).await?;

        let reporter = if let Some(node_reporter) = &reporting {
            node_reporter.register(lane_name.clone(), lane_kind).await
        } else {
            None
        };
        let endpoint = LaneEndpoint {
            name: lane_name,
            transient: false,
            kind,
            io: (in_tx, out_rx),
            reporter,
        };
        Ok(endpoint)
    })
    .await;
    result
        .map_err(move |_| StoreInitError::ItemInitializationTimeout)
        .and_then(identity)
}

async fn store_initialization(
    name: Text,
    kind: StoreKind,
    timeout: Duration,
    mut in_tx: ByteWriter,
    mut out_rx: ByteReader,
    initializer: BoxInitializer<'_>,
) -> Result<StoreEndpoint, StoreInitError> {
    let store_name = name.clone();
    let result = tokio::time::timeout(timeout, async move {
        let init = initializer.initialize(&mut in_tx);
        init.await?;
        wait_for_initialized(&mut out_rx).await?;

        let endpoint = StoreEndpoint::new(store_name, kind, out_rx);
        Ok(endpoint)
    })
    .await;
    result
        .map_err(move |_| StoreInitError::ItemInitializationTimeout)
        .and_then(identity)
}

async fn initialize_items<Store, R>(
    store: &Store,
    mut terminated: R,
    link_requests: &mpsc::Sender<LinkRequest>,
    ad_hoc_tx: &mpsc::Sender<AdHocChannelRequest>,
    initialization: &Initialization,
    lane_endpoints: &mut Vec<LaneEndpoint<Io>>,
    store_endpoints: &mut Vec<StoreEndpoint>,
) -> Result<(), AgentExecError>
where
    Store: AgentPersistence + Send + Sync,
    R: Stream<Item = AgentRuntimeRequest> + Unpin,
{
    let mut initializers: FuturesUnordered<ItemInitTask<'_, Store::StoreId>> =
        FuturesUnordered::new();
    loop {
        let event = tokio::select! {
            Some(item_init_done) = initializers.next(), if !initializers.is_empty() => Either::Left(item_init_done),
            maybe_request = terminated.next() => {
                if let Some(request) = maybe_request {
                    Either::Right(request)
                } else {
                    break Ok(());
                }
            }
        };
        match event {
            Either::Left(endpoint_result) => match endpoint_result? {
                ItemEndpoint::Lane { endpoint: lane, .. } => lane_endpoints.push(lane),
                ItemEndpoint::Store {
                    endpoint: store, ..
                } => store_endpoints.push(store),
            },
            Either::Right(request) => match request {
                AgentRuntimeRequest::AdHoc(req) => {
                    if ad_hoc_tx.send(req).await.is_err() {
                        break Err(AgentExecError::FailedDownlinkRequest);
                    }
                }
                AgentRuntimeRequest::AddLane(LaneRuntimeSpec {
                    name,
                    kind,
                    config,
                    promise,
                }) => {
                    info!("Registering a new {} lane with name '{}'.", kind, name);
                    if let Some(init) =
                        initialization.add_lane(store, name.clone(), kind, config, promise)
                    {
                        initializers.push(init.map_ok(Into::into).boxed());
                    }
                }
                AgentRuntimeRequest::AddStore(StoreRuntimeSpec {
                    name,
                    kind,
                    config,
                    promise,
                }) => {
                    info!("Registering a new {} store with name '{}'.", kind, name);
                    if let Some(init) =
                        initialization.add_store(store, name.clone(), kind, config, promise)?
                    {
                        initializers.push(init.map_ok(Into::into).boxed());
                    }
                }
                AgentRuntimeRequest::OpenDownlink(request) => {
                    if link_requests
                        .send(LinkRequest::Downlink(request))
                        .await
                        .is_err()
                    {
                        break Err(AgentExecError::FailedDownlinkRequest);
                    }
                }
            },
        }
    }?;

    if !initializers.is_empty() {
        while let Some(endpoint_result) = initializers.next().await {
            match endpoint_result? {
                ItemEndpoint::Lane { endpoint: lane, .. } => lane_endpoints.push(lane),
                ItemEndpoint::Store {
                    endpoint: store, ..
                } => store_endpoints.push(store),
            }
        }
    }
    Ok(())
}
