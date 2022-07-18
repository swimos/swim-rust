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
use std::hash::Hash;
use std::{cmp::Ordering, collections::HashMap};

use frunk::{Coprod, Coproduct};
use futures::future::Either;

use crate::lanes::map::lifecycle::MapLaneLifecycleShared;
use crate::lanes::map::{
    lifecycle::{
        on_clear::{OnClear, OnClearShared},
        on_remove::{OnRemove, OnRemoveShared},
        on_update::{OnUpdate, OnUpdateShared},
        MapLaneLifecycle,
    },
    MapLane, MapLaneEvent,
};
use crate::lifecycle::utility::HandlerContext;

use super::{HLeaf, HTree, LaneEvent, LaneEventShared};

#[cfg(test)]
mod tests;

pub type MapLeaf<Context, K, V, LC> = MapBranch<Context, K, V, LC, HLeaf, HLeaf>;

pub type MapLifecycleHandler<'a, Context, K, V, LC> = Coprod!(
    <LC as OnUpdate<'a, K, V, Context>>::OnUpdateHandler,
    <LC as OnRemove<'a, K, V, Context>>::OnRemoveHandler,
    <LC as OnClear<'a, K, V, Context>>::OnClearHandler,
);

pub type MapLifecycleHandlerShared<'a, Context, Shared, K, V, LC> = Coprod!(
    <LC as OnUpdateShared<'a, K, V, Context, Shared>>::OnUpdateHandler,
    <LC as OnRemoveShared<'a, K, V, Context, Shared>>::OnRemoveHandler,
    <LC as OnClearShared<'a, K, V, Context, Shared>>::OnClearHandler,
);

type MapBranchHandler<'a, Context, K, V, LC, L, R> = Either<
    <L as LaneEvent<'a, Context>>::LaneEventHandler,
    Either<
        MapLifecycleHandler<'a, Context, K, V, LC>,
        <R as LaneEvent<'a, Context>>::LaneEventHandler,
    >,
>;

type MapBranchHandlerShared<'a, Context, Shared, K, V, LC, L, R> = Either<
    <L as LaneEventShared<'a, Context, Shared>>::LaneEventHandler,
    Either<
        MapLifecycleHandlerShared<'a, Context, Shared, K, V, LC>,
        <R as LaneEventShared<'a, Context, Shared>>::LaneEventHandler,
    >,
>;

fn map_handler<'a, Context, K, V, LC>(
    event: MapLaneEvent<K, V>,
    lifecycle: &'a LC,
    map: &HashMap<K, V>,
) -> MapLifecycleHandler<'a, Context, K, V, LC>
where
    LC: MapLaneLifecycle<K, V, Context>,
{
    match event {
        MapLaneEvent::Update(k, v) => Coproduct::Inl(lifecycle.on_update(map, k, v)),
        MapLaneEvent::Remove(k, v) => {
            Coproduct::Inr(Coproduct::Inl(lifecycle.on_remove(map, k, v)))
        }
        MapLaneEvent::Clear(before) => {
            Coproduct::Inr(Coproduct::Inr(Coproduct::Inl(lifecycle.on_clear(before))))
        }
    }
}

fn map_handler_shared<'a, Context, Shared, K, V, LC>(
    shared: &'a Shared,
    handler_context: HandlerContext<Context>,
    event: MapLaneEvent<K, V>,
    lifecycle: &'a LC,
    map: &HashMap<K, V>,
) -> MapLifecycleHandlerShared<'a, Context, Shared, K, V, LC>
where
    LC: MapLaneLifecycleShared<K, V, Context, Shared>,
{
    match event {
        MapLaneEvent::Update(k, v) => {
            Coproduct::Inl(lifecycle.on_update(shared, handler_context, map, k, v))
        }
        MapLaneEvent::Remove(k, v) => Coproduct::Inr(Coproduct::Inl(lifecycle.on_remove(
            shared,
            handler_context,
            map,
            k,
            v,
        ))),
        MapLaneEvent::Clear(before) => Coproduct::Inr(Coproduct::Inr(Coproduct::Inl(
            lifecycle.on_clear(shared, handler_context, before),
        ))),
    }
}

/// Map lane lifecycle as a branch node of an [`HTree`].
pub struct MapBranch<Context, K, V, LC, L, R> {
    label: &'static str,
    projection: fn(&Context) -> &MapLane<K, V>,
    lifecycle: LC,
    left: L,
    right: R,
}

impl<Context, K, V, LC, L: HTree, R: HTree> HTree for MapBranch<Context, K, V, LC, L, R> {
    fn label(&self) -> Option<&'static str> {
        Some(self.label)
    }
}

impl<Context, K, V, LC, L, R> Debug for MapBranch<Context, K, V, LC, L, R>
where
    LC: Debug,
    L: Debug,
    R: Debug,
{
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("MapBranch")
            .field("label", &self.label)
            .field("projection", &"...")
            .field("lifecycle", &self.lifecycle)
            .field("left", &self.left)
            .field("right", &self.right)
            .finish()
    }
}

impl<Context, K, V, LC> MapLeaf<Context, K, V, LC> {
    pub fn leaf(
        label: &'static str,
        projection: fn(&Context) -> &MapLane<K, V>,
        lifecycle: LC,
    ) -> Self {
        MapBranch::new(label, projection, lifecycle, HLeaf, HLeaf)
    }
}

impl<Context, K, V, LC, L, R> MapBranch<Context, K, V, LC, L, R>
where
    L: HTree,
    R: HTree,
{
    pub fn new(
        label: &'static str,
        projection: fn(&Context) -> &MapLane<K, V>,
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
        MapBranch {
            label,
            projection,
            lifecycle,
            left,
            right,
        }
    }
}

impl<'a, Context, K, V, LC, L, R> LaneEvent<'a, Context> for MapBranch<Context, K, V, LC, L, R>
where
    K: Clone + Eq + Hash,
    LC: MapLaneLifecycle<K, V, Context>,
    L: HTree + LaneEvent<'a, Context>,
    R: HTree + LaneEvent<'a, Context>,
{
    type LaneEventHandler = MapBranchHandler<'a, Context, K, V, LC, L, R>;

    fn lane_event(&'a self, context: &Context, lane_name: &str) -> Option<Self::LaneEventHandler> {
        let MapBranch {
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
                let handler =
                    lane.read_with_prev(|prev, map| prev.map(|ev| map_handler(ev, lifecycle, map)));
                handler.map(|h| Either::Right(Either::Left(h)))
            }
            Ordering::Greater => right
                .lane_event(context, lane_name)
                .map(|r| Either::Right(Either::Right(r))),
        }
    }
}

impl<'a, Context, Shared, K, V, LC, L, R> LaneEventShared<'a, Context, Shared>
    for MapBranch<Context, K, V, LC, L, R>
where
    K: Clone + Eq + Hash,
    LC: MapLaneLifecycleShared<K, V, Context, Shared>,
    L: HTree + LaneEventShared<'a, Context, Shared>,
    R: HTree + LaneEventShared<'a, Context, Shared>,
{
    type LaneEventHandler = MapBranchHandlerShared<'a, Context, Shared, K, V, LC, L, R>;

    fn lane_event(
        &'a self,
        shared: &'a Shared,
        handler_context: HandlerContext<Context>,
        context: &Context,
        lane_name: &str,
    ) -> Option<Self::LaneEventHandler> {
        let MapBranch {
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
                let handler = lane.read_with_prev(|prev, map| {
                    prev.map(|ev| map_handler_shared(shared, handler_context, ev, lifecycle, map))
                });
                handler.map(|h| Either::Right(Either::Left(h)))
            }
            Ordering::Greater => right
                .lane_event(shared, handler_context, context, lane_name)
                .map(|r| Either::Right(Either::Right(r))),
        }
    }
}
