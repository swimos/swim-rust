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

use crate::agent::lane::model::action::ActionLane;
use crate::agent::lane::model::command::CommandLane;
use crate::agent::lane::model::demand::DemandLane;
use crate::agent::lane::model::demand_map::DemandMapLane;
use crate::agent::lane::LaneModel;
use crate::agent::AgentContext;
use futures::future::{ready, Ready};
use std::fmt::Debug;
use std::future::Future;
use swim_form::Form;

#[cfg(test)]
mod tests;

#[derive(Debug, PartialEq, Eq)]
pub struct DefaultLifecycle;

/// Life cycle events to add behaviour to a lane that maintains an internal state.
/// #Type Parameters
///
/// * `Model` - The type of the model of the lane.
/// * `Agent` - The type of the agent to which the lane belongs.
pub trait StatefulLaneLifecycle<'a, Model: LaneModel, Agent>: Send + Sync + 'static {
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
        &'a mut self,
        event: &'a Model::Event,
        model: &'a Model,
        context: &'a C,
    ) -> Self::EventFuture
    where
        C: AgentContext<Agent> + Send + Sync + 'static;
}

/// Trait for the life cycle of a lane that does not have any internal state and only processes
/// commands and return responses to the original sender.
///
/// #Type Parameters
///
/// * `Command` - The type of commands that the lane can handle.
/// * `Response` - The type of messages that will be received by a subscriber to the lane.
/// * `Agent` - The type of the agent to which the lane belongs.
pub trait ActionLaneLifecycle<'a, Command, Response, Agent>: Send + Sync + 'static {
    type ResponseFuture: Future<Output = Response> + Send + 'a;

    /// Called each type a command is applied to the lane. The returned response will be sent
    /// to the original sender.
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

/// Trait for the life cycle of a lane that does not have any internal state and only processes
/// commands and returns responses to all subscribers.
///
/// #Type Parameters
///
/// * `Command` - The type of commands that the lane can handle.
/// * `Agent` - The type of the agent to which the lane belongs.
pub trait CommandLaneLifecycle<'a, Command, Agent>: Send + Sync + 'static {
    type ResponseFuture: Future<Output = ()> + Send + 'a;

    /// Called each type a command is applied to the lane. The generated event will be sent
    /// to all subscribers of the lane.
    ///
    /// #Arguments
    ///
    /// * `command` - The command object.
    /// * `model` - The model of the lane.
    /// * `context` - Context of the agent that owns the lane.
    fn on_command<C>(
        &'a self,
        command: &'a Command,
        model: &'a CommandLane<Command>,
        context: &'a C,
    ) -> Self::ResponseFuture
    where
        C: AgentContext<Agent> + Send + Sync + 'static;
}

/// Trait for the lifecycle of a lane that has access to the configuration of
/// a swim agent and defines how the lifecycle is created.
///
/// # Type Parameters
///
/// * `Config` - Swim agent config.

pub trait LaneLifecycle<Config> {
    /// Called when a task for a swim agent is created using the lifecycle.
    /// This method defines how to create the lifecycle and has access to the swim
    /// agent config file.
    ///
    /// # Arguments
    ///
    /// * `config` - Swim agent config.

    fn create(config: &Config) -> Self;
}

pub trait DemandLaneLifecycle<'a, Event, Agent>: Send + Sync + 'static {
    type OnCueFuture: Future<Output = Option<Event>> + Send + 'a;

    fn on_cue<C>(&'a self, model: &'a DemandLane<Event>, context: &'a C) -> Self::OnCueFuture
    where
        C: AgentContext<Agent> + Send + Sync + 'static;
}

/// Trait for the lifecycle of a lane that does not have any internal state and will fetch all
/// values by demand. Upon a sync request, the keys to sync are returned by `OnSyncFuture`. For all
/// of these keys, `on_cue` is invoked and any `Some(Value)` returned are synced. Any cue requests
/// made to the lane, `on_cue` is invoked and any `Some(Value)` returned are propagated.
///
/// # Type Parameters
///
/// * `Key`: The type of keys in the map.
/// * `Value`: The type of the values in the map.
/// * `Agent` - The type of the agent to which the lane belongs.
pub trait DemandMapLaneLifecycle<'a, Key, Value, Agent>: Send + Sync + 'static
where
    Key: Debug + Form + Send + Sync + 'static,
    Value: Debug + Form + Send + Sync + 'static,
{
    type OnSyncFuture: Future<Output = Vec<Key>> + Send + 'a;
    type OnCueFuture: Future<Output = Option<Value>> + Send + 'a;
    type OnRemoveFuture: Future<Output = ()> + Send + 'a;

    /// Invoked after a sync request has been made to the lane.
    ///
    /// # Arguments
    ///
    /// * `model` - The model of the lane.
    /// * `context` - Context of the agent that owns the lane.
    fn on_sync<C>(
        &'a self,
        model: &'a DemandMapLane<Key, Value>,
        context: &'a C,
    ) -> Self::OnSyncFuture
    where
        C: AgentContext<Agent> + Send + Sync + 'static;

    /// Invoked after a key has been cued. If `Some(Value)` is returned, then this value will be
    /// propagated to all uplinks.
    ///
    /// # Arguments:
    ///
    /// * `model` - The model of the lane.
    /// * `context` - Context of the agent that owns the lane.
    /// * `key` - The key of the value.
    fn on_cue<C>(
        &'a self,
        model: &'a DemandMapLane<Key, Value>,
        context: &'a C,
        key: Key,
    ) -> Self::OnCueFuture
    where
        C: AgentContext<Agent> + Send + Sync + 'static;

    /// Invoked after a key has been removed.
    ///
    /// # Arguments:
    ///
    /// * `model` - The model of the lane.
    /// * `context` - Context of the agent that owns the lane.
    /// * `key` - The key of the value.
    fn on_remove<C>(
        &'a mut self,
        _model: &'a DemandMapLane<Key, Value>,
        _context: &'a C,
        _key: Key,
    ) -> Self::OnRemoveFuture
    where
        C: AgentContext<Agent> + Send + Sync + 'static;
}

impl<'a, Model: LaneModel, Agent> StatefulLaneLifecycle<'a, Model, Agent> for DefaultLifecycle {
    type StartFuture = Ready<()>;
    type EventFuture = Ready<()>;

    fn on_start<C: AgentContext<Agent>>(
        &'a self,
        _model: &'a Model,
        _context: &'a C,
    ) -> Self::StartFuture {
        ready(())
    }

    fn on_event<C>(
        &'a mut self,
        _event: &'a <Model as LaneModel>::Event,
        _model: &'a Model,
        _context: &'a C,
    ) -> Self::EventFuture
    where
        C: AgentContext<Agent> + Send + Sync + 'static,
    {
        ready(())
    }
}
