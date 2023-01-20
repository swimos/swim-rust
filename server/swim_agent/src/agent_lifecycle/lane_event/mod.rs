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

use std::fmt::Debug;

use swim_api::handlers::NoHandler;

use crate::event_handler::{EventHandler, UnitHandler};

use super::utility::HandlerContext;

mod command;
mod map;
#[cfg(test)]
mod tests;
mod value;

pub use command::{
    CommandBranch, CommandLeaf, CommandLifecycleHandler, CommandLifecycleHandlerShared,
};
pub use map::{
    MapBranch, MapLeaf, MapLifecycleHandler, MapLifecycleHandlerShared, MapStoreBranch,
    MapStoreLeaf,
};
pub use value::{
    ValueBranch, ValueLeaf, ValueLifecycleHandler, ValueLifecycleHandlerShared, ValueStoreBranch,
    ValueStoreLeaf,
};

/// Trait to implement all event handlers for all of the lanes of an agent. Implementations of
/// this trait will typically consist of a type level tree (implementations of [`HTree`]) of handlers
/// for each lane.
pub trait LaneEvent<Context> {
    type LaneEventHandler<'a>: EventHandler<Context> + 'a
    where
        Self: 'a;

    /// Create the handler for a lane, if it exists. It is the responsibility of the lanes to keep track
    /// of which what events need to be triggered. If the lane does not exist or no event is pending, no
    /// handler will be returned.
    /// #Arguments
    /// * `context` - The context of the agent (allowing access to the lanes).
    /// * `lane_name` - The name of the lane.
    fn lane_event<'a>(
        &'a self,
        context: &Context,
        lane_name: &str,
    ) -> Option<Self::LaneEventHandler<'a>>;
}

/// Trait to implement all event handlers for all of the lanes of an agent. Implementations of
/// this trait will typically consist of a type level tree (implementations of [`HTree`]) of handlers
/// for each lane. Each of the event handlers has access to a single shared state.
pub trait LaneEventShared<Context, Shared> {
    type LaneEventHandler<'a>: EventHandler<Context> + 'a
    where
        Self: 'a,
        Shared: 'a;

    /// Create the handler for a lane, if it exists. It is the responsibility of the lanes to keep track
    /// of which what events need to be triggered. If the lane does not exist or no event is pending, no
    /// handler will be returned.
    /// #Arguments
    /// * `shared` - The shared state.
    /// * `handler_context` - Utility for constructing event handlers.
    /// * `context` - The context of the agent (allowing access to the lanes).
    /// * `lane_name` - The name of the lane.
    fn lane_event<'a>(
        &'a self,
        shared: &'a Shared,
        handler_context: HandlerContext<Context>,
        context: &Context,
        lane_name: &str,
    ) -> Option<Self::LaneEventHandler<'a>>;
}

impl<Context> LaneEvent<Context> for NoHandler {
    type LaneEventHandler<'a> = UnitHandler
    where
        Self: 'a;

    fn lane_event<'a>(
        &'a self,
        _context: &Context,
        _lane_name: &str,
    ) -> Option<Self::LaneEventHandler<'a>> {
        None
    }
}

impl<Context, Shared> LaneEventShared<Context, Shared> for NoHandler {
    type LaneEventHandler<'a> = UnitHandler
    where
        Self: 'a,
        Shared: 'a;

    fn lane_event<'a>(
        &'a self,
        _shared: &'a Shared,
        _handler_context: HandlerContext<Context>,
        _context: &Context,
        _lane_name: &str,
    ) -> Option<Self::LaneEventHandler<'a>> {
        None
    }
}

/// Trait for type level, binary trees of lane event handlers.
pub trait HTree {
    /// The label of the tree node (or none for an empty leaf).
    fn label(&self) -> Option<&'static str>;
}

///An empty leaf node in an [`HTree`].
#[derive(Debug, Default, Clone, Copy)]
pub struct HLeaf;

impl HTree for HLeaf {
    fn label(&self) -> Option<&'static str> {
        None
    }
}

impl<Context> LaneEvent<Context> for HLeaf {
    type LaneEventHandler<'a> = UnitHandler
    where
        Self: 'a;

    fn lane_event<'a>(
        &'a self,
        _context: &Context,
        _lane_name: &str,
    ) -> Option<Self::LaneEventHandler<'a>> {
        None
    }
}

impl<Context, Shared> LaneEventShared<Context, Shared> for HLeaf {
    type LaneEventHandler<'a> = UnitHandler
    where
        Self: 'a,
        Shared: 'a;

    fn lane_event<'a>(
        &'a self,
        _shared: &'a Shared,
        _handler_context: HandlerContext<Context>,
        _context: &Context,
        _lane_name: &str,
    ) -> Option<Self::LaneEventHandler<'a>> {
        None
    }
}
