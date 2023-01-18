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

use std::{convert::identity, time::Duration};

use futures::{future::Either, stream::FuturesUnordered, StreamExt};
use swim_api::{
    agent::{LaneConfig, UplinkKind},
    meta::lane::LaneKind,
    protocol::agent::StoreInitializedCodec,
    store::StoreDisabled,
};
use swim_model::Text;
use swim_utilities::{
    io::byte_channel::{self, ByteReader, ByteWriter},
    trigger,
};
use tokio::sync::mpsc;
use tokio_stream::wrappers::ReceiverStream;
use tokio_util::codec::FramedRead;

use crate::agent::{
    store::{no_map_init, no_value_init, AgentPersistence, BoxInitializer, StoreInitError},
    AgentExecError, AgentRuntimeRequest, DownlinkRequest, Io, NodeReporting,
};

use super::{InitialEndpoints, LaneEndpoint};

use tracing::{error, info};

#[cfg(test)]
mod tests;

/// Task for the initialization of an agent. While this task is executing, new lanes can be
/// registered but they will not be driven and no remote connections will exist attached to
/// the agent.
pub struct AgentInitTask<Store = StoreDisabled> {
    requests: mpsc::Receiver<AgentRuntimeRequest>,
    downlink_requests: mpsc::Sender<DownlinkRequest>,
    init_complete: trigger::Receiver,
    lane_init_timeout: Duration,
    reporting: Option<NodeReporting>,
    store: Store,
}

impl AgentInitTask {
    /// #Arguments
    /// * `requests` - Channel for requests to open new lanes and downlinks.
    /// * `init_complete` - Triggered when the initialization phase is complete.
    /// * `lane_init_timeout` - Timeout for initializing lanes from the store.
    /// * `reporting` - Reporter for node/lane introspection support.
    pub fn new(
        requests: mpsc::Receiver<AgentRuntimeRequest>,
        downlink_requests: mpsc::Sender<DownlinkRequest>,
        init_complete: trigger::Receiver,
        lane_init_timeout: Duration,
        reporting: Option<NodeReporting>,
    ) -> Self {
        Self::with_store(
            requests,
            downlink_requests,
            init_complete,
            lane_init_timeout,
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
    /// * `requests` - Channel for requests to open new lanes and downlinks.
    /// * `init_complete` - Triggered when the initialization phase is complete.
    /// * `lane_init_timeout` - Timeout for initializing lanes from the store.
    /// * `reporting` - Reporter for node/lane introspection support.
    /// * `store` - Store for lane persistence.
    pub fn with_store(
        requests: mpsc::Receiver<AgentRuntimeRequest>,
        downlink_requests: mpsc::Sender<DownlinkRequest>,
        init_complete: trigger::Receiver,
        lane_init_timeout: Duration,
        reporting: Option<NodeReporting>,
        store: Store,
    ) -> Self {
        AgentInitTask {
            requests,
            downlink_requests,
            init_complete,
            lane_init_timeout,
            reporting,
            store,
        }
    }
}

impl<Store: AgentPersistence + Send + Sync> AgentInitTask<Store> {
    pub async fn run(self) -> Result<(InitialEndpoints, Store), AgentExecError> {
        let AgentInitTask {
            requests,
            init_complete,
            downlink_requests,
            store,
            lane_init_timeout,
            reporting,
        } = self;

        let mut request_stream = ReceiverStream::new(requests);
        let mut terminated = (&mut request_stream).take_until(init_complete);

        let mut endpoints = vec![];

        let mut initializers = FuturesUnordered::new();

        loop {
            let event = tokio::select! {
                Some(lane_init_done) = initializers.next(), if !initializers.is_empty() => Either::Left(lane_init_done),
                maybe_request = terminated.next() => {
                    if let Some(request) = maybe_request {
                        Either::Right(request)
                    } else {
                        break;
                    }
                }
            };
            match event {
                Either::Left(endpoint_result) => {
                    endpoints.push(endpoint_result?);
                }
                Either::Right(request) => match request {
                    AgentRuntimeRequest::AddLane {
                        name,
                        kind,
                        config,
                        promise,
                    } => {
                        info!("Registering a new {} lane with name '{}'.", kind, name);
                        let uplink_kind = kind.uplink_kind();
                        let LaneConfig {
                            input_buffer_size,
                            output_buffer_size,
                            transient,
                        } = config;

                        let (in_tx, in_rx) = byte_channel::byte_channel(input_buffer_size);
                        let (out_tx, out_rx) = byte_channel::byte_channel(output_buffer_size);

                        let io = (out_tx, in_rx);

                        if promise.send(Ok(io)).is_ok() {
                            let get_lane_id = || {
                                store.lane_id(name.as_str()).map_err(|error| {
                                    AgentExecError::FailedRestoration {
                                        lane_name: name.clone(),
                                        error: StoreInitError::Store(error),
                                    }
                                })
                            };
                            let maybe_initializer = match uplink_kind {
                                UplinkKind::Value if !transient => {
                                    let lane_id = get_lane_id()?;
                                    Some(
                                        store
                                            .init_value_lane(lane_id)
                                            .unwrap_or_else(|| no_value_init()),
                                    )
                                }
                                UplinkKind::Map if !transient => {
                                    let lane_id = get_lane_id()?;
                                    Some(
                                        store
                                            .init_map_lane(lane_id)
                                            .unwrap_or_else(|| no_map_init()),
                                    )
                                }
                                _ => None,
                            };
                            if let Some(initializer) = maybe_initializer {
                                let init_task = lane_initialization(
                                    name.clone(),
                                    kind,
                                    lane_init_timeout,
                                    reporting.as_ref(),
                                    in_tx,
                                    out_rx,
                                    initializer,
                                );
                                initializers.push(init_task);
                            } else {
                                let reporter = if let Some(node_reporter) = &reporting {
                                    node_reporter.register(name.clone(), kind).await
                                } else {
                                    None
                                };
                                endpoints.push(LaneEndpoint {
                                    name,
                                    kind: kind.uplink_kind(),
                                    transient,
                                    io: (in_tx, out_rx),
                                    reporter,
                                });
                            }
                        } else {
                            error!(
                                "Agent failed to receive lane registration for {} lane named '{}'.",
                                kind, name
                            );
                        }
                    }
                    AgentRuntimeRequest::OpenDownlink(request) => {
                        if downlink_requests.send(request).await.is_err() {
                            return Err(AgentExecError::FailedDownlinkRequest);
                        }
                    }
                },
            }
        }
        if !initializers.is_empty() {
            while let Some(endpoint_result) = initializers.next().await {
                endpoints.push(endpoint_result?);
            }
        }
        drop(initializers);
        if endpoints.is_empty() {
            Err(AgentExecError::NoInitialLanes)
        } else {
            Ok((
                InitialEndpoints::new(reporting, request_stream.into_inner(), endpoints),
                store,
            ))
        }
    }
}

async fn wait_for_initialized(reader: &mut ByteReader) -> Result<(), StoreInitError> {
    let mut reader = FramedRead::new(reader, StoreInitializedCodec::default());
    match reader.next().await {
        Some(Ok(_)) => Ok(()),
        _ => Err(StoreInitError::NoAckFromLane),
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
) -> Result<LaneEndpoint<Io>, AgentExecError> {
    let lane_name = name.clone();
    let kind = lane_kind.uplink_kind();
    let result = tokio::time::timeout(timeout, async move {
        let init = initializer.initialize(&mut in_tx);
        let result = init.await;

        if let Err(error) = result {
            Err(AgentExecError::FailedRestoration { lane_name, error })
        } else if let Err(error) = wait_for_initialized(&mut out_rx).await {
            Err(AgentExecError::FailedRestoration { lane_name, error })
        } else {
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
        }
    })
    .await;
    result
        .map_err(move |_| AgentExecError::FailedRestoration {
            lane_name: name,
            error: StoreInitError::LaneInitializationTimeout,
        })
        .and_then(identity)
}
