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

use crate::agent::AgentRuntimeRequest;

use super::{AgentInit, AgentRuntimeConfig, LaneEndpoint};

pub struct AgentInitTask {
    requests: mpsc::Receiver<AgentRuntimeRequest>,
    init_complete: trigger::Receiver,
    config: AgentRuntimeConfig,
}

pub struct NoLanes;

impl AgentInitTask {
    pub fn new(
        requests: mpsc::Receiver<AgentRuntimeRequest>,
        init_complete: trigger::Receiver,
        config: AgentRuntimeConfig,
    ) -> Self {
        AgentInitTask {
            requests,
            init_complete,
            config,
        }
    }

    pub async fn run(self) -> Result<AgentInit, NoLanes> {
        let AgentInitTask {
            requests,
            init_complete,
            config: agent_config,
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
                    let LaneConfig {
                        input_buffer_size,
                        output_buffer_size,
                    } = config.unwrap_or(agent_config.default_lane_config);

                    let (in_tx, in_rx) = byte_channel::byte_channel(input_buffer_size);
                    let (out_tx, out_rx) = byte_channel::byte_channel(output_buffer_size);

                    let io = (in_rx, out_tx);
                    if promise.send(Ok(io)).is_ok() {
                        endpoints.push(LaneEndpoint {
                            name,
                            kind,
                            io: (out_rx, in_tx),
                        });
                    }
                }
                AgentRuntimeRequest::OpenDownlink { .. } => {
                    todo!("Opening downlinks from agents not implemented yet.")
                }
            }
        }
        if endpoints.is_empty() {
            Err(NoLanes)
        } else {
            Ok(AgentInit {
                rx: request_stream.into_inner(),
                endpoints,
            })
        }
    }
}
