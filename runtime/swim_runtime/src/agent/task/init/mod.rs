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

use futures::StreamExt;
use swim_api::agent::LaneConfig;
use swim_utilities::{io::byte_channel, trigger};
use tokio::sync::mpsc;
use tokio_stream::wrappers::ReceiverStream;

use crate::agent::{AgentExecError, AgentRuntimeConfig, AgentRuntimeRequest, DownlinkRequest};

use super::{InitialEndpoints, LaneEndpoint};

use tracing::{error, info};

#[cfg(test)]
mod tests;

/// Task for the initialization of an agent. While this task is executing, new lanes can be
/// registered but they will not be driven and no remote connections will exist attached to
/// the agent.
pub struct AgentInitTask {
    requests: mpsc::Receiver<AgentRuntimeRequest>,
    downlink_requests: mpsc::Sender<DownlinkRequest>,
    init_complete: trigger::Receiver,
    config: AgentRuntimeConfig,
}

impl AgentInitTask {
    /// #Arguments
    /// * `requests` - Channel for requests to open new lanes and downlinks.
    /// * `init_complete` - Triggered when the initialization phase is complete.
    /// * `config` - Configuration parameters for the agent runtime.
    pub fn new(
        requests: mpsc::Receiver<AgentRuntimeRequest>,
        downlink_requests: mpsc::Sender<DownlinkRequest>,
        init_complete: trigger::Receiver,
        config: AgentRuntimeConfig,
    ) -> Self {
        AgentInitTask {
            requests,
            downlink_requests,
            init_complete,
            config,
        }
    }

    pub async fn run(self) -> Result<InitialEndpoints, AgentExecError> {
        let AgentInitTask {
            requests,
            init_complete,
            config: agent_config,
            downlink_requests,
        } = self;

        let mut request_stream = ReceiverStream::new(requests);
        let mut terminated = (&mut request_stream).take_until(init_complete);

        let mut endpoints = vec![];

        while let Some(request) = terminated.next().await {
            match request {
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
                        ..
                    } = config.unwrap_or(agent_config.default_lane_config);

                    let (in_tx, in_rx) = byte_channel::byte_channel(input_buffer_size);
                    let (out_tx, out_rx) = byte_channel::byte_channel(output_buffer_size);

                    let io = (out_tx, in_rx);
                    if promise.send(Ok(io)).is_ok() {
                        endpoints.push(LaneEndpoint {
                            name,
                            kind,
                            io: (in_tx, out_rx),
                        });
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
