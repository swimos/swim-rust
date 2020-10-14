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
use crate::agent::lane::model::demand::DemandLane;
use crate::agent::lane::model::demand_map::DemandMapLane;
use crate::agent::lane::strategy::{Buffered, Dropping, Queue};
use crate::agent::lane::LaneModel;
use crate::agent::AgentContext;
use futures::future::{ready, Ready};
use std::fmt::Debug;
use std::future::Future;
use swim_common::form::Form;

#[cfg(test)]
mod tests;

/// Base trait for all lane lifecycles for lanes that maintain an internal state.
pub trait StatefulLaneLifecycleBase: Send + Sync + 'static {
    type WatchStrategy;

    /// Create the watch strategy that will receive events indicating the changes to the
    /// underlying state. The constraints on this type will depend on the particular type of lane.
    fn create_strategy(&self) -> Self::WatchStrategy;
}

/// Life cycle events to add behaviour to a lane that maintains an internal state.
/// #Type Parameters
///
/// * `Model` - The type of the model of the lane.
/// * `Agent` - The type of the agent to which the lane belongs.
pub trait StatefulLaneLifecycle<'a, Model: LaneModel, Agent>: StatefulLaneLifecycleBase {
    type StartFuture: Future<Output = ()> + Send + 'a;
    type EventFuture: Future<Output = ()> + Send + 'a;

    /// Called after the agent containing the lane has started.
    ///
    /// #Arguments
    ///
    /// * `model` - The model of the lane.
    /// * `context` - Context of the agent that owns the lane.
    fn on_start<C>(&'a self, model: &'a Model, context: &'a C) -> Self::StartFuture
    where
        C: AgentContext<Agent> + Send + Sync + 'a;

    /// Called each type an event is received by the lane's watch strategy (at most once each time
    /// the state changes, depending on the strategy).
    ///
    /// #Arguments
    ///
    /// * `event` - The description of the state change.
    /// * `model` - The model of the lane.
    /// * `context` - Context of the agent that owns the lane.
    fn on_event<C>(
        &'a self,
        event: &'a Model::Event,
        model: &'a Model,
        context: &'a C,
    ) -> Self::EventFuture
    where
        C: AgentContext<Agent> + Send + Sync + 'static;
}

/// Trait for the life cycle of a lane that does not have any internal state and only processes
/// commands.
///
/// #Type Parameters
///
/// * `Command` - The type of commands that the lane can handle.
/// * `Response` - The type of messages that will be received by a subscriber to the lane.
/// * `Agent` - The type of the agent to which the lane belongs.
pub trait ActionLaneLifecycle<'a, Command, Response, Agent>: Send + Sync + 'static {
    type ResponseFuture: Future<Output = Response> + Send + 'a;

    /// Called each type a command is applied to the lane. The returned response will be sent
    /// to any subscribers to the lane.
    ///
    /// #Arguments
    ///
    /// * `command` - The command object.
    /// * `model` - The model of the lane.
    /// * `context` - Context of the agent that owns the lane.
    fn on_command<C>(
        &'a self,
        command: Command,
        model: &'a ActionLane<Command, Response>,
        context: &'a C,
    ) -> Self::ResponseFuture
    where
        C: AgentContext<Agent> + Send + Sync + 'static;
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

pub trait DemandLaneLifecycle<'a, Value, Agent>: Send + Sync + 'static {
    type OnCueFuture: Future<Output = Option<Value>> + Send + 'a;

    fn on_cue<C>(&'a self, model: &'a DemandLane<Value>, context: &'a C) -> Self::OnCueFuture
    where
        C: AgentContext<Agent> + Send + Sync + 'static;
}

pub trait DemandMapLaneLifecycle<'a, Key, Value, Agent>: Send + Sync + 'static
where
    Key: Debug + Form + Send + Sync + 'static,
    Value: Debug + Form + Send + Sync + 'static,
{
    type OnSyncFuture: Future<Output = Vec<Key>> + Send + 'a;
    type OnCueFuture: Future<Output = Option<Value>> + Send + 'a;

    fn on_sync<C>(
        &'a self,
        model: &'a DemandMapLane<Key, Value>,
        context: &'a C,
    ) -> Self::OnSyncFuture
    where
        C: AgentContext<Agent> + Send + Sync + 'static;

    fn on_cue<C>(
        &'a self,
        model: &'a DemandMapLane<Key, Value>,
        context: &'a C,
        key: Key,
    ) -> Self::OnCueFuture
    where
        C: AgentContext<Agent> + Send + Sync + 'static;
}
