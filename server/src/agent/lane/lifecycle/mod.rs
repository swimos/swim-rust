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

use crate::agent::lane::model::action::ActionLane;
use crate::agent::lane::strategy::{Buffered, Dropping, Queue};
use crate::agent::lane::LaneModel;
use crate::agent::AgentContext;
use futures::future::{ready, BoxFuture, FutureExt};

#[allow(unused)]
pub trait LaneLifecycle<Model: LaneModel, Agent>: Default + Send + Sync + 'static {
    type WatchStrategy;

    fn create_strategy(&self) -> Self::WatchStrategy;

    fn on_start<'a, C: AgentContext<Agent>>(
        &'a self,
        model: &'a Model,
        context: &'a C,
    ) -> BoxFuture<'a, ()>;

    fn on_event<'a, C>(
        &'a self,
        event: &'a Model::Event,
        model: &'a Model,
        context: &'a C,
    ) -> BoxFuture<'a, ()>
    where
        C: AgentContext<Agent> + Send + Sync + 'static;
}

pub trait ActionLaneLifecycle<Command, Response, Agent>:
    LaneLifecycle<ActionLane<Command, Response>, Agent>
{
    fn on_command<C: AgentContext<Agent>>(&self, command: &Command, context: &C) -> Response;
}

impl<Model: LaneModel, Agent> LaneLifecycle<Model, Agent> for Queue {
    type WatchStrategy = Self;

    fn create_strategy(&self) -> Self::WatchStrategy {
        self.clone()
    }

    fn on_start<'a, C: AgentContext<Agent>>(
        &'a self,
        _model: &'a Model,
        _context: &'a C,
    ) -> BoxFuture<'a, ()> {
        ready(()).boxed()
    }

    fn on_event<'a, C: AgentContext<Agent>>(
        &'a self,
        _event: &'a Model::Event,
        _model: &'a Model,
        _context: &'a C,
    ) -> BoxFuture<'a, ()> {
        ready(()).boxed()
    }
}

impl<Model: LaneModel, Agent> LaneLifecycle<Model, Agent> for Dropping {
    type WatchStrategy = Self;

    fn create_strategy(&self) -> Self::WatchStrategy {
        self.clone()
    }

    fn on_start<'a, C: AgentContext<Agent>>(
        &'a self,
        _model: &'a Model,
        _context: &'a C,
    ) -> BoxFuture<'a, ()> {
        ready(()).boxed()
    }

    fn on_event<'a, C: AgentContext<Agent>>(
        &'a self,
        _event: &'a Model::Event,
        _model: &'a Model,
        _context: &'a C,
    ) -> BoxFuture<'a, ()> {
        ready(()).boxed()
    }
}

impl<Model: LaneModel, Agent> LaneLifecycle<Model, Agent> for Buffered {
    type WatchStrategy = Self;

    fn create_strategy(&self) -> Self::WatchStrategy {
        self.clone()
    }

    fn on_start<'a, C: AgentContext<Agent>>(
        &'a self,
        _model: &'a Model,
        _context: &'a C,
    ) -> BoxFuture<'a, ()> {
        ready(()).boxed()
    }

    fn on_event<'a, C: AgentContext<Agent>>(
        &'a self,
        _event: &'a Model::Event,
        _model: &'a Model,
        _context: &'a C,
    ) -> BoxFuture<'a, ()> {
        ready(()).boxed()
    }
}
