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

/// Trait to implement all event handlers for all of the items(lanes and stores) of an agent.
/// Implementations of this trait will typically consist of a type level tree (implementations of
/// [`HTree`]) of handlers for each item.
pub trait ItemEvent<Context> {
    type ItemEventHandler<'a>: EventHandler<Context> + 'a
    where
        Self: 'a;

    /// Create the handler for an item, if it exists. It is the responsibility of the items to keep track
    /// of which what events need to be triggered. If the item does not exist or no event is pending, no
    /// handler will be returned.
    /// #Arguments
    /// * `context` - The context of the agent (allowing access to the items).
    /// * `item_name` - The name of the item.
    fn item_event<'a>(
        &'a self,
        context: &Context,
        item_name: &str,
    ) -> Option<Self::ItemEventHandler<'a>>;
}

/// Trait to implement all event handlers for all of the items of an agent. Implementations of
/// this trait will typically consist of a type level tree (implementations of [`HTree`]) of handlers
/// for each item. Each of the event handlers has access to a single shared state.
pub trait ItemEventShared<Context, Shared> {
    type ItemEventHandler<'a>: EventHandler<Context> + 'a
    where
        Self: 'a,
        Shared: 'a;

    /// Create the handler for an item, if it exists. It is the responsibility of the items to keep track
    /// of which what events need to be triggered. If the item does not exist or no event is pending, no
    /// handler will be returned.
    /// #Arguments
    /// * `shared` - The shared state.
    /// * `handler_context` - Utility for constructing event handlers.
    /// * `context` - The context of the agent (allowing access to the items).
    /// * `item_name` - The name of the item.
    fn item_event<'a>(
        &'a self,
        shared: &'a Shared,
        handler_context: HandlerContext<Context>,
        context: &Context,
        item_name: &str,
    ) -> Option<Self::ItemEventHandler<'a>>;
}

impl<Context> ItemEvent<Context> for NoHandler {
    type ItemEventHandler<'a> = UnitHandler
    where
        Self: 'a;

    fn item_event<'a>(
        &'a self,
        _context: &Context,
        _item_name: &str,
    ) -> Option<Self::ItemEventHandler<'a>> {
        None
    }
}

impl<Context, Shared> ItemEventShared<Context, Shared> for NoHandler {
    type ItemEventHandler<'a> = UnitHandler
    where
        Self: 'a,
        Shared: 'a;

    fn item_event<'a>(
        &'a self,
        _shared: &'a Shared,
        _handler_context: HandlerContext<Context>,
        _context: &Context,
        _item_name: &str,
    ) -> Option<Self::ItemEventHandler<'a>> {
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

impl<Context> ItemEvent<Context> for HLeaf {
    type ItemEventHandler<'a> = UnitHandler
    where
        Self: 'a;

    fn item_event<'a>(
        &'a self,
        _context: &Context,
        _item_name: &str,
    ) -> Option<Self::ItemEventHandler<'a>> {
        None
    }
}

impl<Context, Shared> ItemEventShared<Context, Shared> for HLeaf {
    type ItemEventHandler<'a> = UnitHandler
    where
        Self: 'a,
        Shared: 'a;

    fn item_event<'a>(
        &'a self,
        _shared: &'a Shared,
        _handler_context: HandlerContext<Context>,
        _context: &Context,
        _item_name: &str,
    ) -> Option<Self::ItemEventHandler<'a>> {
        None
    }
}
