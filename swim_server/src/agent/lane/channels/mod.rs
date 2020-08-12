// Copyright 2015-2020 SWIM.AI inc.
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

use crate::agent::lane::channels::update::LaneUpdate;
use crate::agent::lane::channels::uplink::{UplinkAction, UplinkStateMachine};
use crate::routing::{RoutingAddr, ServerRouter};
use futures::future::BoxFuture;
use pin_utils::core_reexport::num::NonZeroUsize;
use tokio::sync::mpsc;

pub mod task;
pub mod update;
pub mod uplink;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AgentExecutionConfig {
    pub max_concurrency: usize,
    pub action_buffer: NonZeroUsize,
    pub update_buffer: NonZeroUsize,
    pub uplink_err_buffer: NonZeroUsize,
    pub max_fatal_uplink_errors: usize,
    pub max_uplink_start_attempts: NonZeroUsize,
}

impl Default for AgentExecutionConfig {
    fn default() -> Self {
        let default_buffer = NonZeroUsize::new(4).unwrap();
        AgentExecutionConfig {
            max_concurrency: 1,
            action_buffer: default_buffer,
            update_buffer: default_buffer,
            uplink_err_buffer: default_buffer,
            max_fatal_uplink_errors: 0,
            max_uplink_start_attempts: default_buffer,
        }
    }
}

pub trait AgentExecutionContext {
    type Router: ServerRouter + 'static;

    fn router_handle(&self) -> Self::Router;

    fn spawner(&self) -> mpsc::Sender<BoxFuture<'static, ()>>;
}

pub trait LaneMessageHandler: Send + Sync {
    type Event: Send;
    type Uplink: UplinkStateMachine<Self::Event> + Send + Sync + 'static;
    type Update: LaneUpdate;

    fn make_uplink(&self, addr: RoutingAddr) -> Self::Uplink;

    fn make_update(&self) -> Self::Update;
}

pub type OutputMessage<Handler> = <<Handler as LaneMessageHandler>::Uplink as UplinkStateMachine<
    <Handler as LaneMessageHandler>::Event,
>>::Msg;

pub type InputMessage<Handler> = <<Handler as LaneMessageHandler>::Update as LaneUpdate>::Msg;

#[derive(Debug)]
pub struct TaggedAction(RoutingAddr, UplinkAction);
