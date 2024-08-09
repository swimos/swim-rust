// Copyright 2015-2024 Swim Inc.
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

#[doc(hidden)]
pub mod command;
#[doc(hidden)]
pub mod demand;
#[doc(hidden)]
pub mod demand_map;
#[doc(hidden)]
pub mod http;
mod join;
#[doc(hidden)]
pub mod map;
mod queues;
#[doc(hidden)]
pub mod supply;
#[cfg(test)]
mod tests;
#[doc(hidden)]
pub mod value;
#[doc(hidden)]
pub use join::map as join_map;
#[doc(hidden)]
pub use join::value as join_value;
pub use join::LinkClosedResponse;
use swimos_api::error::LaneSpawnError;

use crate::event_handler::ActionContext;
use crate::event_handler::EventHandler;
use crate::event_handler::EventHandlerError;
use crate::event_handler::HandlerAction;
use crate::event_handler::StepResult;
use crate::AgentMetadata;
use crate::{agent_model::WriteResult, item::AgentItem};
use bytes::BytesMut;
use swimos_api::agent::WarpLaneKind;

#[doc(inline)]
pub use self::{
    command::CommandLane,
    demand::DemandLane,
    demand_map::DemandMapLane,
    http::{HttpLane, SimpleHttpLane},
    join::JoinLaneKind,
    join_map::JoinMapLane,
    join_value::JoinValueLane,
    map::MapLane,
    supply::SupplyLane,
    value::ValueLane,
};

#[doc(hidden)]
pub use {
    map::{MapLaneSelectClear, MapLaneSelectRemove, MapLaneSelectUpdate},
    value::ValueLaneSelectSet,
};

/// Wrapper to allow projection function pointers to be exposed as event handler transforms
/// for different types of lanes.
pub struct ProjTransform<C, L> {
    projection: fn(&C) -> &L,
}

impl<C, L> ProjTransform<C, L> {
    pub fn new(projection: fn(&C) -> &L) -> Self {
        ProjTransform { projection }
    }
}

/// Base trait for all agent items that model lanes.
pub trait LaneItem: AgentItem {
    /// If the state of the lane has changed, write an event into the buffer.
    fn write_to_buffer(&self, buffer: &mut BytesMut) -> WriteResult;
}

/// A selector borrows a (potentially absent) named component from its type.
pub trait Selector {
    /// The type of the component.
    type Target: ?Sized;

    /// Borrow the component, if it exists.
    fn select(&self) -> Option<&Self::Target>;

    /// The name of the component.
    fn name(&self) -> &str;
}

/// A projection function that binds a [`Selector`] to a component of a context type `C`.
pub trait SelectorFn<C> {
    /// The type of the component chosen by the [`Selector`].
    type Target: ?Sized;

    /// Bind the selector.
    ///
    /// #Arguments
    /// * `context` - The context form the the [`Selector`] will attempt to select its component.
    fn selector(self, context: &C) -> impl Selector<Target = Self::Target> + '_;
}

/// An [event handler](crate::event_handler::EventHandler) that attempts to open a new lane for the
/// agent (note that the [agent specification](crate::AgentSpec) must support this.)
pub struct OpenLane<OnDone> {
    name: String,
    kind: WarpLaneKind,
    on_done: Option<OnDone>,
}

impl<OnDone> OpenLane<OnDone> {
    /// # Arguments
    /// * `name` - The name of the new lane.
    /// * `kind` - The kind of the new lane.
    /// * `on_done` - A callback tht produces an event handler that will be executed after the request completes.
    pub(crate) fn new(name: String, kind: WarpLaneKind, on_done: OnDone) -> Self {
        OpenLane {
            name,
            kind,
            on_done: Some(on_done),
        }
    }
}

impl<Context, OnDone, H> HandlerAction<Context> for OpenLane<OnDone>
where
    OnDone: FnOnce(Result<(), LaneSpawnError>) -> H + Send + 'static,
    H: EventHandler<Context> + Send + 'static,
{
    type Completion = ();

    fn step(
        &mut self,
        action_context: &mut ActionContext<Context>,
        _meta: AgentMetadata,
        _context: &Context,
    ) -> StepResult<Self::Completion> {
        let OpenLane {
            name,
            kind,
            on_done,
        } = self;
        if let Some(on_done) = on_done.take() {
            match action_context.open_lane(name, *kind, on_done) {
                Ok(_) => StepResult::done(()),
                Err(err) => StepResult::Fail(EventHandlerError::FailedRegistration(err)),
            }
        } else {
            StepResult::after_done()
        }
    }
}
