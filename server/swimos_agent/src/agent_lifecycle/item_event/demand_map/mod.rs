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

use futures::future::Either;

use crate::{
    agent_lifecycle::HandlerContext,
    lanes::{
        demand_map::{
            demand_map_handler, demand_map_handler_shared,
            lifecycle::{
                keys::{Keys, KeysShared},
                on_cue_key::{OnCueKey, OnCueKeyShared},
                DemandMapLaneLifecycle, DemandMapLaneLifecycleShared,
            },
            DemandMap,
        },
        DemandMapLane,
    },
};
use std::{cmp::Ordering, fmt::Debug, hash::Hash};

use super::{HLeaf, HTree, ItemEvent, ItemEventShared};

#[cfg(test)]
mod tests;

pub type DemandMapLeaf<Context, K, V, LC> = DemandMapBranch<Context, K, V, LC, HLeaf, HLeaf>;

pub type DemandMapLifecycleHandler<'a, Context, K, V, LC> = DemandMap<
    Context,
    K,
    V,
    <LC as Keys<K, Context>>::KeysHandler<'a>,
    <LC as OnCueKey<K, V, Context>>::OnCueKeyHandler<'a>,
>;

pub type DemandMapLifecycleHandlerShared<'a, Context, Shared, K, V, LC> = DemandMap<
    Context,
    K,
    V,
    <LC as KeysShared<K, Context, Shared>>::KeysHandler<'a>,
    <LC as OnCueKeyShared<K, V, Context, Shared>>::OnCueKeyHandler<'a>,
>;

type DemandMapBranchHandler<'a, Context, K, V, LC, L, R> = Either<
    <L as ItemEvent<Context>>::ItemEventHandler<'a>,
    Either<
        DemandMapLifecycleHandler<'a, Context, K, V, LC>,
        <R as ItemEvent<Context>>::ItemEventHandler<'a>,
    >,
>;

type DemandMapBranchHandlerShared<'a, Context, Shared, K, V, LC, L, R> = Either<
    <L as ItemEventShared<Context, Shared>>::ItemEventHandler<'a>,
    Either<
        DemandMapLifecycleHandlerShared<'a, Context, Shared, K, V, LC>,
        <R as ItemEventShared<Context, Shared>>::ItemEventHandler<'a>,
    >,
>;

/// Demand lane lifecycle as a branch node of an [`HTree`].
pub struct DemandMapBranch<Context, K, V, LC, L, R> {
    label: &'static str,
    projection: fn(&Context) -> &DemandMapLane<K, V>,
    lifecycle: LC,
    left: L,
    right: R,
}

impl<Context, K, V, LC: Clone, L: Clone, R: Clone> Clone
    for DemandMapBranch<Context, K, V, LC, L, R>
{
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

impl<Context, K, V, LC, L, R> Debug for DemandMapBranch<Context, K, V, LC, L, R>
where
    LC: Debug,
    L: Debug,
    R: Debug,
{
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("DemandMapBranch")
            .field("label", &self.label)
            .field("projection", &"...")
            .field("lifecycle", &self.lifecycle)
            .field("left", &self.left)
            .field("right", &self.right)
            .finish()
    }
}

impl<Context, K, V, LC> DemandMapLeaf<Context, K, V, LC> {
    pub fn leaf(
        label: &'static str,
        projection: fn(&Context) -> &DemandMapLane<K, V>,
        lifecycle: LC,
    ) -> Self {
        DemandMapBranch::new(label, projection, lifecycle, HLeaf, HLeaf)
    }
}

impl<Context, K, V, LC, L, R> DemandMapBranch<Context, K, V, LC, L, R>
where
    L: HTree,
    R: HTree,
{
    pub fn new(
        label: &'static str,
        projection: fn(&Context) -> &DemandMapLane<K, V>,
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
        DemandMapBranch {
            label,
            projection,
            lifecycle,
            left,
            right,
        }
    }
}

impl<Context, K, V, LC, L: HTree, R: HTree> HTree for DemandMapBranch<Context, K, V, LC, L, R> {
    fn label(&self) -> Option<&'static str> {
        Some(self.label)
    }
}

impl<Context, K, V, LC, L, R> ItemEvent<Context> for DemandMapBranch<Context, K, V, LC, L, R>
where
    K: Clone + Eq + Hash,
    LC: DemandMapLaneLifecycle<K, V, Context>,
    L: HTree + ItemEvent<Context>,
    R: HTree + ItemEvent<Context>,
{
    type ItemEventHandler<'a> = DemandMapBranchHandler<'a, Context, K, V, LC, L, R>
    where
        Self: 'a;

    fn item_event<'a>(
        &'a self,
        context: &Context,
        item_name: &str,
    ) -> Option<Self::ItemEventHandler<'a>> {
        let DemandMapBranch {
            label,
            projection,
            lifecycle,
            left,
            right,
        } = self;
        match item_name.cmp(*label) {
            Ordering::Less => left.item_event(context, item_name).map(Either::Left),
            Ordering::Equal => {
                let handler = demand_map_handler(context, *projection, lifecycle);
                Some(Either::Right(Either::Left(handler)))
            }
            Ordering::Greater => right
                .item_event(context, item_name)
                .map(|r| Either::Right(Either::Right(r))),
        }
    }
}

impl<Context, Shared, K, V, LC, L, R> ItemEventShared<Context, Shared>
    for DemandMapBranch<Context, K, V, LC, L, R>
where
    K: Clone + Eq + Hash,
    LC: DemandMapLaneLifecycleShared<K, V, Context, Shared>,
    L: HTree + ItemEventShared<Context, Shared>,
    R: HTree + ItemEventShared<Context, Shared>,
{
    type ItemEventHandler<'a> = DemandMapBranchHandlerShared<'a, Context, Shared, K, V, LC, L, R>
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
        let DemandMapBranch {
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
                let handler = demand_map_handler_shared(context, shared, *projection, lifecycle);
                Some(Either::Right(Either::Left(handler)))
            }
            Ordering::Greater => right
                .item_event(shared, handler_context, context, item_name)
                .map(|r| Either::Right(Either::Right(r))),
        }
    }
}
