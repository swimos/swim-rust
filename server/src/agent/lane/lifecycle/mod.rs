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
use futures::future::{ready, Ready};
use std::future::Future;

pub trait StatefulLaneLifecycleBase: Default + Send + Sync + 'static {
    type WatchStrategy;

    fn create_strategy(&self) -> Self::WatchStrategy;
}

#[allow(unused)]
pub trait StatefulLaneLifecycle<'a, Model: LaneModel, Agent>: StatefulLaneLifecycleBase {
    type StartFuture: Future<Output = ()> + Send + 'a;
    type EventFuture: Future<Output = ()> + Send + 'a;

    fn on_start<C: AgentContext<Agent>>(
        &'a self,
        model: &'a Model,
        context: &'a C,
    ) -> Self::StartFuture;

    fn on_event<C>(
        &'a self,
        event: &'a Model::Event,
        model: &'a Model,
        context: &'a C,
    ) -> Self::EventFuture
    where
        C: AgentContext<Agent> + Send + Sync + 'static;
}

pub trait ActionLaneLifecycle<'a, Command, Response, Agent>:
    Default + Send + Sync + 'static
{
    type ResponseFuture: Future<Output = Response> + Send + 'a;

    fn on_command<C: AgentContext<Agent>>(
        &'a self,
        command: &'a Command,
        model: &'a ActionLane<Command, Response>,
        context: &'a C,
    ) -> Self::ResponseFuture;
}

impl StatefulLaneLifecycleBase for Queue {
    type WatchStrategy = Self;

    fn create_strategy(&self) -> Self::WatchStrategy {
        self.clone()
    }
}

impl<'a, Model: LaneModel, Agent> StatefulLaneLifecycle<'a, Model, Agent> for Queue {
    type StartFuture = Ready<()>;
    type EventFuture = Ready<()>;

    fn on_start<C: AgentContext<Agent>>(
        &'a self,
        _model: &'a Model,
        _context: &'a C,
    ) -> Self::StartFuture {
        ready(())
    }

    fn on_event<C: AgentContext<Agent>>(
        &'a self,
        _event: &'a Model::Event,
        _model: &'a Model,
        _context: &'a C,
    ) -> Self::EventFuture {
        ready(())
    }
}

impl StatefulLaneLifecycleBase for Dropping {
    type WatchStrategy = Self;

    fn create_strategy(&self) -> Self::WatchStrategy {
        self.clone()
    }
}

impl<'a, Model: LaneModel, Agent> StatefulLaneLifecycle<'a, Model, Agent> for Dropping {
    type StartFuture = Ready<()>;
    type EventFuture = Ready<()>;

    fn on_start<C: AgentContext<Agent>>(
        &'a self,
        _model: &'a Model,
        _context: &'a C,
    ) -> Self::StartFuture {
        ready(())
    }

    fn on_event<C: AgentContext<Agent>>(
        &'a self,
        _event: &'a Model::Event,
        _model: &'a Model,
        _context: &'a C,
    ) -> Self::EventFuture {
        ready(())
    }
}

impl StatefulLaneLifecycleBase for Buffered {
    type WatchStrategy = Self;

    fn create_strategy(&self) -> Self::WatchStrategy {
        self.clone()
    }
}

impl<'a, Model: LaneModel, Agent> StatefulLaneLifecycle<'a, Model, Agent> for Buffered {
    type StartFuture = Ready<()>;
    type EventFuture = Ready<()>;

    fn on_start<C: AgentContext<Agent>>(
        &'a self,
        _model: &'a Model,
        _context: &'a C,
    ) -> Self::StartFuture {
        ready(())
    }

    fn on_event<C: AgentContext<Agent>>(
        &'a self,
        _event: &'a Model::Event,
        _model: &'a Model,
        _context: &'a C,
    ) -> Self::EventFuture {
        ready(())
    }
}
