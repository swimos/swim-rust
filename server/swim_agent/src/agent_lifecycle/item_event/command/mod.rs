// Copyright 2015-2023 Swim Inc.
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

use futures::future::Either;
use std::{cmp::Ordering, fmt::Debug};

use crate::{
    agent_lifecycle::utility::HandlerContext,
    lanes::command::{
        lifecycle::{
            on_command::{OnCommand, OnCommandShared},
            CommandLaneLifecycle, CommandLaneLifecycleShared,
        },
        CommandLane,
    },
};

use super::{HLeaf, HTree, ItemEvent, ItemEventShared};

#[cfg(test)]
mod tests;

pub type CommandLeaf<Context, T, LC> = CommandBranch<Context, T, LC, HLeaf, HLeaf>;

pub type CommandLifecycleHandler<'a, Context, T, LC> =
    <LC as OnCommand<T, Context>>::OnCommandHandler<'a>;

pub type CommandLifecycleHandlerShared<'a, Context, Shared, T, LC> =
    <LC as OnCommandShared<T, Context, Shared>>::OnCommandHandler<'a>;

type CommandBranchHandler<'a, Context, T, LC, L, R> = Either<
    <L as ItemEvent<Context>>::ItemEventHandler<'a>,
    Either<
        CommandLifecycleHandler<'a, Context, T, LC>,
        <R as ItemEvent<Context>>::ItemEventHandler<'a>,
    >,
>;

type CommandBranchHandlerShared<'a, Context, Shared, T, LC, L, R> = Either<
    <L as ItemEventShared<Context, Shared>>::ItemEventHandler<'a>,
    Either<
        CommandLifecycleHandlerShared<'a, Context, Shared, T, LC>,
        <R as ItemEventShared<Context, Shared>>::ItemEventHandler<'a>,
    >,
>;

/// Command lane lifecycle as a branch node of an [`HTree`].
pub struct CommandBranch<Context, T, LC, L, R> {
    label: &'static str,
    projection: fn(&Context) -> &CommandLane<T>,
    lifecycle: LC,
    left: L,
    right: R,
}

impl<Context, T, LC: Clone, L: Clone, R: Clone> Clone for CommandBranch<Context, T, LC, L, R> {
    fn clone(&self) -> Self {
        Self {
            label: self.label,
            projection: self.projection,
            lifecycle: self.lifecycle.clone(),
            left: self.left.clone(),
            right: self.right.clone(),
        }
    }
}

impl<Context, T, LC, L, R> Debug for CommandBranch<Context, T, LC, L, R>
where
    LC: Debug,
    L: Debug,
    R: Debug,
{
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("CommandBranch")
            .field("label", &self.label)
            .field("projection", &"...")
            .field("lifecycle", &self.lifecycle)
            .field("left", &self.left)
            .field("right", &self.right)
            .finish()
    }
}

impl<Context, T, LC> CommandLeaf<Context, T, LC> {
    pub fn leaf(
        label: &'static str,
        projection: fn(&Context) -> &CommandLane<T>,
        lifecycle: LC,
    ) -> Self {
        CommandBranch::new(label, projection, lifecycle, HLeaf, HLeaf)
    }
}

impl<Context, T, LC, L, R> CommandBranch<Context, T, LC, L, R>
where
    L: HTree,
    R: HTree,
{
    pub fn new(
        label: &'static str,
        projection: fn(&Context) -> &CommandLane<T>,
        lifecycle: LC,
        left: L,
        right: R,
    ) -> Self {
        if let Some(left_label) = left.label() {
            assert!(left_label < label);
        }
        if let Some(right_label) = right.label() {
            assert!(label < right_label);
        }
        CommandBranch {
            label,
            projection,
            lifecycle,
            left,
            right,
        }
    }
}

impl<Context, T, LC, L: HTree, R: HTree> HTree for CommandBranch<Context, T, LC, L, R> {
    fn label(&self) -> Option<&'static str> {
        Some(self.label)
    }
}

impl<Context, T, LC, L, R> ItemEvent<Context> for CommandBranch<Context, T, LC, L, R>
where
    LC: CommandLaneLifecycle<T, Context>,
    L: HTree + ItemEvent<Context>,
    R: HTree + ItemEvent<Context>,
{
    type ItemEventHandler<'a> = CommandBranchHandler<'a, Context, T, LC, L, R>
    where
        Self: 'a;

    fn item_event<'a>(
        &'a self,
        context: &Context,
        item_name: &str,
    ) -> Option<Self::ItemEventHandler<'a>> {
        let CommandBranch {
            label,
            projection,
            lifecycle,
            left,
            right,
        } = self;
        match item_name.cmp(*label) {
            Ordering::Less => left.item_event(context, item_name).map(Either::Left),
            Ordering::Equal => {
                let lane = projection(context);
                lane.with_prev(|prev| prev.as_ref().map(|value| lifecycle.on_command(value)))
                    .map(|h| Either::Right(Either::Left(h)))
            }
            Ordering::Greater => right
                .item_event(context, item_name)
                .map(|r| Either::Right(Either::Right(r))),
        }
    }
}

impl<Context, Shared, T, LC, L, R> ItemEventShared<Context, Shared>
    for CommandBranch<Context, T, LC, L, R>
where
    LC: CommandLaneLifecycleShared<T, Context, Shared>,
    L: HTree + ItemEventShared<Context, Shared>,
    R: HTree + ItemEventShared<Context, Shared>,
{
    type ItemEventHandler<'a> = CommandBranchHandlerShared<'a, Context, Shared, T, LC, L, R>
    where
        Self: 'a,
        Shared: 'a;

    fn item_event<'a>(
        &'a self,
        shared: &'a Shared,
        handler_context: HandlerContext<Context>,
        context: &Context,
        item_name: &str,
    ) -> Option<Self::ItemEventHandler<'a>> {
        let CommandBranch {
            label,
            projection,
            lifecycle,
            left,
            right,
        } = self;
        match item_name.cmp(*label) {
            Ordering::Less => left
                .item_event(shared, handler_context, context, item_name)
                .map(Either::Left),
            Ordering::Equal => {
                let lane = projection(context);
                lane.with_prev(|prev| {
                    prev.as_ref()
                        .map(|value| lifecycle.on_command(shared, handler_context, value))
                })
                .map(|h| Either::Right(Either::Left(h)))
            }
            Ordering::Greater => right
                .item_event(shared, handler_context, context, item_name)
                .map(|r| Either::Right(Either::Right(r))),
        }
    }
}
