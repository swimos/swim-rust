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

use super::{HLeaf, HTree, LaneEvent, LaneEventShared};

#[cfg(test)]
mod tests;

pub type CommandLeaf<Context, T, LC> = CommandBranch<Context, T, LC, HLeaf, HLeaf>;

pub type CommandLifecycleHandler<'a, Context, T, LC> =
    <LC as OnCommand<T, Context>>::OnCommandHandler<'a>;

pub type CommandLifecycleHandlerShared<'a, Context, Shared, T, LC> =
    <LC as OnCommandShared<T, Context, Shared>>::OnCommandHandler<'a>;

type CommandBranchHandler<'a, Context, T, LC, L, R> = Either<
    <L as LaneEvent<'a, Context>>::LaneEventHandler,
    Either<
        CommandLifecycleHandler<'a, Context, T, LC>,
        <R as LaneEvent<'a, Context>>::LaneEventHandler,
    >,
>;

type CommandBranchHandlerShared<'a, Context, Shared, T, LC, L, R> = Either<
    <L as LaneEventShared<'a, Context, Shared>>::LaneEventHandler,
    Either<
        CommandLifecycleHandlerShared<'a, Context, Shared, T, LC>,
        <R as LaneEventShared<'a, Context, Shared>>::LaneEventHandler,
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

impl<'a, Context, T, LC, L, R> LaneEvent<'a, Context> for CommandBranch<Context, T, LC, L, R>
where
    LC: CommandLaneLifecycle<T, Context> + 'a,
    L: HTree + LaneEvent<'a, Context>,
    R: HTree + LaneEvent<'a, Context>,
{
    type LaneEventHandler = CommandBranchHandler<'a, Context, T, LC, L, R>;

    fn lane_event(&'a self, context: &Context, lane_name: &str) -> Option<Self::LaneEventHandler> {
        let CommandBranch {
            label,
            projection,
            lifecycle,
            left,
            right,
        } = self;
        match lane_name.cmp(*label) {
            Ordering::Less => left.lane_event(context, lane_name).map(Either::Left),
            Ordering::Equal => {
                let lane = projection(context);
                lane.with_prev(|prev| prev.as_ref().map(|value| lifecycle.on_command(value)))
                    .map(|h| Either::Right(Either::Left(h)))
            }
            Ordering::Greater => right
                .lane_event(context, lane_name)
                .map(|r| Either::Right(Either::Right(r))),
        }
    }
}

impl<'a, Context, Shared, T, LC, L, R> LaneEventShared<'a, Context, Shared>
    for CommandBranch<Context, T, LC, L, R>
where
    Shared: 'a,
    LC: CommandLaneLifecycleShared<T, Context, Shared> + 'a,
    L: HTree + LaneEventShared<'a, Context, Shared>,
    R: HTree + LaneEventShared<'a, Context, Shared>,
{
    type LaneEventHandler = CommandBranchHandlerShared<'a, Context, Shared, T, LC, L, R>;

    fn lane_event(
        &'a self,
        shared: &'a Shared,
        handler_context: HandlerContext<Context>,
        context: &Context,
        lane_name: &str,
    ) -> Option<Self::LaneEventHandler> {
        let CommandBranch {
            label,
            projection,
            lifecycle,
            left,
            right,
        } = self;
        match lane_name.cmp(*label) {
            Ordering::Less => left
                .lane_event(shared, handler_context, context, lane_name)
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
                .lane_event(shared, handler_context, context, lane_name)
                .map(|r| Either::Right(Either::Right(r))),
        }
    }
}
