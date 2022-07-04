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
mod value;

pub use command::{
    CommandBranch, CommandLeaf, CommandLifecycleHandler, CommandLifecycleHandlerShared,
};
pub use map::{MapBranch, MapLeaf, MapLifecycleHandler, MapLifecycleHandlerShared};
pub use value::{ValueBranch, ValueLeaf, ValueLifecycleHandler, ValueLifecycleHandlerShared};

pub trait LaneEvent<'a, Context> {
    type LaneEventHandler: EventHandler<Context, Completion = ()> + Send + 'a;

    fn lane_event(&'a self, context: &Context, lane_name: &str) -> Option<Self::LaneEventHandler>;
}

pub trait LaneEventShared<'a, Context, Shared> {
    type LaneEventHandler: EventHandler<Context, Completion = ()> + Send + 'a;

    fn lane_event(
        &'a self,
        shared: &'a Shared,
        handler_context: HandlerContext<Context>,
        context: &Context,
        lane_name: &str,
    ) -> Option<Self::LaneEventHandler>;
}

impl<'a, Context> LaneEvent<'a, Context> for NoHandler {
    type LaneEventHandler = UnitHandler;

    fn lane_event(
        &'a self,
        _context: &Context,
        _lane_name: &str,
    ) -> Option<Self::LaneEventHandler> {
        None
    }
}

impl<'a, Context, Shared> LaneEventShared<'a, Context, Shared> for NoHandler {
    type LaneEventHandler = UnitHandler;

    fn lane_event(
        &'a self,
        _shared: &'a Shared,
        _handler_context: HandlerContext<Context>,
        _context: &Context,
        _lane_name: &str,
    ) -> Option<Self::LaneEventHandler> {
        None
    }
}

pub trait HTree {
    fn label(&self) -> Option<&'static str>;
}

#[derive(Debug, Default, Clone, Copy)]
struct HLeaf;

impl HTree for HLeaf {
    fn label(&self) -> Option<&'static str> {
        None
    }
}

impl<'a, Context> LaneEvent<'a, Context> for HLeaf {
    type LaneEventHandler = UnitHandler;

    fn lane_event(
        &'a self,
        _context: &Context,
        _lane_name: &str,
    ) -> Option<Self::LaneEventHandler> {
        None
    }
}

impl<'a, Context, Shared> LaneEventShared<'a, Context, Shared> for HLeaf {
    type LaneEventHandler = UnitHandler;

    fn lane_event(
        &'a self,
        _shared: &'a Shared,
        _handler_context: HandlerContext<Context>,
        _context: &Context,
        _lane_name: &str,
    ) -> Option<Self::LaneEventHandler> {
        None
    }
}
