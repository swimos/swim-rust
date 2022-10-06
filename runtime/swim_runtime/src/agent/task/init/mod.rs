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

use futures::{future::Either, stream::FuturesUnordered, StreamExt};
use swim_api::agent::{LaneConfig, UplinkKind};
use swim_utilities::{
    io::byte_channel::{self, ByteReader},
    trigger,
};
use tokio::sync::mpsc;
use tokio_stream::wrappers::ReceiverStream;
use tokio_util::codec::FramedRead;

use crate::agent::{
    store::{AgentPersistence, StoreDisabled, StoreInitError},
    AgentExecError, AgentRuntimeRequest, DownlinkRequest,
};
use swim_api::protocol::agent::{LaneResponse, ValueLaneResponseDecoder};

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
    store: Store,
}

impl AgentInitTask {
    /// #Arguments
    /// * `requests` - Channel for requests to open new lanes and downlinks.
    /// * `init_complete` - Triggered when the initialization phase is complete.
    pub fn new(
        requests: mpsc::Receiver<AgentRuntimeRequest>,
        downlink_requests: mpsc::Sender<DownlinkRequest>,
        init_complete: trigger::Receiver,
    ) -> Self {
        AgentInitTask {
            requests,
            downlink_requests,
            init_complete,
            store: StoreDisabled::default(),
        }
    }
}

impl<Store: AgentPersistence + Clone + Send + Sync> AgentInitTask<Store> {
    pub async fn run(self) -> Result<InitialEndpoints, AgentExecError> {
        let AgentInitTask {
            requests,
            init_complete,
            downlink_requests,
            store,
            ..
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
                        let LaneConfig {
                            input_buffer_size,
                            output_buffer_size,
                            transient,
                        } = config;

                        let (mut in_tx, in_rx) = byte_channel::byte_channel(input_buffer_size);
                        let (out_tx, mut out_rx) = byte_channel::byte_channel(output_buffer_size);

                        let io = (out_tx, in_rx);
                        if promise.send(Ok(io)).is_ok() {
                            if transient {
                                endpoints.push(LaneEndpoint {
                                    name,
                                    kind,
                                    io: (in_tx, out_rx),
                                });
                            } else {
                                let lane_id = store.lane_id(name.as_str()).map_err(|error| {
                                    AgentExecError::FailedRestoration {
                                        lane_name: name.clone(),
                                        error: StoreInitError::Store(error),
                                    }
                                })?;
                                if let Some(initializer) = match kind {
                                    UplinkKind::Value => store.init_value_lane(lane_id),
                                    UplinkKind::Map => store.init_map_lane(lane_id),
                                } {
                                    let init_task = async move {
                                        let init = initializer.initialize(&mut in_tx);
                                        let result = init.await;

                                        if let Err(error) = result {
                                            Err(AgentExecError::FailedRestoration {
                                                lane_name: name,
                                                error,
                                            })
                                        } else if let Err(error) =
                                            wait_for_initialized(&mut out_rx).await
                                        {
                                            Err(AgentExecError::FailedRestoration {
                                                lane_name: name,
                                                error,
                                            })
                                        } else {
                                            let endpoint = LaneEndpoint {
                                                name,
                                                kind,
                                                io: (in_tx, out_rx),
                                            };
                                            Ok(endpoint)
                                        }
                                    };
                                    initializers.push(init_task);
                                } else {
                                    endpoints.push(LaneEndpoint {
                                        name,
                                        kind,
                                        io: (in_tx, out_rx),
                                    });
                                }
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
        if endpoints.is_empty() {
            Err(AgentExecError::NoInitialLanes)
        } else {
            Ok(InitialEndpoints::new(
                request_stream.into_inner(),
                endpoints,
            ))
        }
    }
}

async fn wait_for_initialized(reader: &mut ByteReader) -> Result<(), StoreInitError> {
    let mut reader = FramedRead::new(reader, ValueLaneResponseDecoder::default());
    match reader.next().await {
        Some(Ok(LaneResponse::Initialized)) => Ok(()),
        _ => Err(StoreInitError::NoAckFromLane),
    }
}
