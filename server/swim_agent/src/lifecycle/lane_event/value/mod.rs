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

use std::cmp::Ordering;

use futures::future::Either;

use crate::lifecycle::utility::HandlerContext;
use crate::{
    event_handler::{EventHandler, FollowedBy},
    lanes::value::{
        lifecycle::{
            on_event::{OnEvent, OnEventShared},
            on_set::{OnSet, OnSetShared},
            ValueLaneLifecycle, ValueLaneLifecycleShared,
        },
        ValueLane,
    },
};

use super::{HLeaf, HTree, LaneEvent, LaneEventShared};

#[cfg(test)]
mod tests;

pub type ValueLeaf<Context, T, LC> = ValueBranch<Context, T, LC, HLeaf, HLeaf>;

/// Command lane lifecycle as a branch node of an [`HTree`].
pub struct ValueBranch<Context, T, LC, L, R> {
    label: &'static str,
    projection: fn(&Context) -> &ValueLane<T>,
    lifecycle: LC,
    left: L,
    right: R,
}

impl<Context, T, LC, L, R> Debug for ValueBranch<Context, T, LC, L, R>
where
    LC: Debug,
    L: Debug,
    R: Debug,
{
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ValueBranch")
            .field("label", &self.label)
            .field("projection", &"...")
            .field("lifecycle", &self.lifecycle)
            .field("left", &self.left)
            .field("right", &self.right)
            .finish()
    }
}

impl<Context, T, LC> ValueLeaf<Context, T, LC> {
    pub fn leaf(
        label: &'static str,
        projection: fn(&Context) -> &ValueLane<T>,
        lifecycle: LC,
    ) -> Self {
        Self::new(label, projection, lifecycle, HLeaf, HLeaf)
    }
}

impl<Context, T, LC, L, R> ValueBranch<Context, T, LC, L, R>
where
    L: HTree,
    R: HTree,
{
    pub fn new(
        label: &'static str,
        projection: fn(&Context) -> &ValueLane<T>,
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
        ValueBranch {
            label,
            projection,
            lifecycle,
            left,
            right,
        }
    }
}

impl<Context, T, LC, L: HTree, R: HTree> HTree for ValueBranch<Context, T, LC, L, R> {
    fn label(&self) -> Option<&'static str> {
        Some(self.label)
    }
}

pub type ValueLifecycleHandler<'a, Context, T, LC> = FollowedBy<
    <LC as OnEvent<'a, T, Context>>::OnEventHandler,
    <LC as OnSet<'a, T, Context>>::OnSetHandler,
>;

pub type ValueLifecycleHandlerShared<'a, Context, Shared, T, LC> = FollowedBy<
    <LC as OnEventShared<'a, T, Context, Shared>>::OnEventHandler,
    <LC as OnSetShared<'a, T, Context, Shared>>::OnSetHandler,
>;

type ValueBranchHandler<'a, Context, T, LC, L, R> = Either<
    <L as LaneEvent<'a, Context>>::LaneEventHandler,
    Either<
        ValueLifecycleHandler<'a, Context, T, LC>,
        <R as LaneEvent<'a, Context>>::LaneEventHandler,
    >,
>;

type ValueBranchHandlerShared<'a, Context, Shared, T, LC, L, R> = Either<
    <L as LaneEventShared<'a, Context, Shared>>::LaneEventHandler,
    Either<
        ValueLifecycleHandlerShared<'a, Context, Shared, T, LC>,
        <R as LaneEventShared<'a, Context, Shared>>::LaneEventHandler,
    >,
>;

impl<'a, Context, T, LC, L, R> LaneEvent<'a, Context> for ValueBranch<Context, T, LC, L, R>
where
    LC: ValueLaneLifecycle<T, Context>,
    L: HTree + LaneEvent<'a, Context>,
    R: HTree + LaneEvent<'a, Context>,
{
    type LaneEventHandler = ValueBranchHandler<'a, Context, T, LC, L, R>;

    fn lane_event(&'a self, context: &Context, lane_name: &str) -> Option<Self::LaneEventHandler> {
        let ValueBranch {
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
                let handler = lane.read_with_prev(|prev, new_value| {
                    let event_handler = lifecycle.on_event(new_value);
                    let set_handler = lifecycle.on_set(prev, new_value);
                    event_handler.followed_by(set_handler)
                });
                Some(Either::Right(Either::Left(handler)))
            }
            Ordering::Greater => right
                .lane_event(context, lane_name)
                .map(|r| Either::Right(Either::Right(r))),
        }
    }
}

impl<'a, Context, Shared, T, LC, L, R> LaneEventShared<'a, Context, Shared>
    for ValueBranch<Context, T, LC, L, R>
where
    LC: ValueLaneLifecycleShared<T, Context, Shared>,
    L: HTree + LaneEventShared<'a, Context, Shared>,
    R: HTree + LaneEventShared<'a, Context, Shared>,
{
    type LaneEventHandler = ValueBranchHandlerShared<'a, Context, Shared, T, LC, L, R>;

    fn lane_event(
        &'a self,
        shared: &'a Shared,
        handler_context: HandlerContext<Context>,
        context: &Context,
        lane_name: &str,
    ) -> Option<Self::LaneEventHandler> {
        let ValueBranch {
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
                let handler = lane.read_with_prev(|prev, new_value| {
                    let event_handler = lifecycle.on_event(shared, handler_context, new_value);
                    let set_handler = lifecycle.on_set(shared, handler_context, new_value, prev);
                    event_handler.followed_by(set_handler)
                });
                Some(Either::Right(Either::Left(handler)))
            }
            Ordering::Greater => right
                .lane_event(shared, handler_context, context, lane_name)
                .map(|r| Either::Right(Either::Right(r))),
        }
    }
}
