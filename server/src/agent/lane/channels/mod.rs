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

use crate::routing::ServerRouter;
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
    pub max_fatal_uplink_errors: usize,
}

pub trait AgentExecutionContext {
    type Router: ServerRouter;

    fn router_handle(&self) -> Self::Router;

    fn configuration(&self) -> &AgentExecutionConfig;

    fn spawner(&self) -> mpsc::Sender<BoxFuture<'static, ()>>;
}
