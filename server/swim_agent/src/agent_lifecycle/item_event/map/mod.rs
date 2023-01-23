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
use std::marker::PhantomData;
use std::{cmp::Ordering, collections::HashMap};

use frunk::{Coprod, Coproduct};
use futures::future::Either;

use crate::agent_lifecycle::utility::HandlerContext;
use crate::item::MapItem;
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
use crate::stores::MapStore;

use super::{HLeaf, HTree, ItemEvent, ItemEventShared};

#[cfg(test)]
mod tests;

pub type MapLeaf<Context, K, V, LC> = MapBranch<Context, K, V, LC, HLeaf, HLeaf>;
pub type MapBranch<Context, K, V, LC, L, R> = MapLikeBranch<Context, K, V, MapLane<K, V>, LC, L, R>;

pub type MapStoreLeaf<Context, K, V, LC> = MapStoreBranch<Context, K, V, LC, HLeaf, HLeaf>;
pub type MapStoreBranch<Context, K, V, LC, L, R> =
    MapLikeBranch<Context, K, V, MapStore<K, V>, LC, L, R>;

pub type MapLifecycleHandler<'a, Context, K, V, LC> = Coprod!(
    <LC as OnUpdate<K, V, Context>>::OnUpdateHandler<'a>,
    <LC as OnRemove<K, V, Context>>::OnRemoveHandler<'a>,
    <LC as OnClear<K, V, Context>>::OnClearHandler<'a>,
);

pub type MapLifecycleHandlerShared<'a, Context, Shared, K, V, LC> = Coprod!(
    <LC as OnUpdateShared<K, V, Context, Shared>>::OnUpdateHandler<'a>,
    <LC as OnRemoveShared<K, V, Context, Shared>>::OnRemoveHandler<'a>,
    <LC as OnClearShared<K, V, Context, Shared>>::OnClearHandler<'a>,
);

type MapBranchHandler<'a, Context, K, V, LC, L, R> = Either<
    <L as ItemEvent<Context>>::ItemEventHandler<'a>,
    Either<
        MapLifecycleHandler<'a, Context, K, V, LC>,
        <R as ItemEvent<Context>>::ItemEventHandler<'a>,
    >,
>;

type MapBranchHandlerShared<'a, Context, Shared, K, V, LC, L, R> = Either<
    <L as ItemEventShared<Context, Shared>>::ItemEventHandler<'a>,
    Either<
        MapLifecycleHandlerShared<'a, Context, Shared, K, V, LC>,
        <R as ItemEventShared<Context, Shared>>::ItemEventHandler<'a>,
    >,
>;

fn map_handler<'a, Context, K, V, LC>(
    event: MapLaneEvent<K, V>,
    lifecycle: &'a LC,
    map: &HashMap<K, V>,
) -> MapLifecycleHandler<'a, Context, K, V, LC>
where
    K: Hash + Eq,
    LC: MapLaneLifecycle<K, V, Context>,
{
    match event {
        MapLaneEvent::Update(k, old) => {
            let new = &map[&k];
            Coproduct::Inl(lifecycle.on_update(map, k, old, new))
        }
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
    K: Hash + Eq,
    LC: MapLaneLifecycleShared<K, V, Context, Shared>,
{
    match event {
        MapLaneEvent::Update(k, old) => {
            let new = &map[&k];
            Coproduct::Inl(lifecycle.on_update(shared, handler_context, map, k, old, new))
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

type KeyValue<K, V> = fn((K, V)) -> (K, V);
/// Map lane lifecycle as a branch node of an [`HTree`].
pub struct MapLikeBranch<Context, K, V, Item, LC, L, R> {
    _type: PhantomData<KeyValue<K, V>>,
    label: &'static str,
    projection: fn(&Context) -> &Item,
    lifecycle: LC,
    left: L,
    right: R,
}

impl<Context, K, V, Item, LC: Clone, L: Clone, R: Clone> Clone
    for MapLikeBranch<Context, K, V, Item, LC, L, R>
{
    fn clone(&self) -> Self {
        Self {
            _type: PhantomData,
            label: self.label,
            projection: self.projection,
            lifecycle: self.lifecycle.clone(),
            left: self.left.clone(),
            right: self.right.clone(),
        }
    }
}

impl<Context, K, V, Item, LC, L: HTree, R: HTree> HTree
    for MapLikeBranch<Context, K, V, Item, LC, L, R>
{
    fn label(&self) -> Option<&'static str> {
        Some(self.label)
    }
}

impl<Context, K, V, Item, LC, L, R> Debug for MapLikeBranch<Context, K, V, Item, LC, L, R>
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

impl<Context, K, V, Item, LC> MapLikeBranch<Context, K, V, Item, LC, HLeaf, HLeaf> {
    pub fn leaf(label: &'static str, projection: fn(&Context) -> &Item, lifecycle: LC) -> Self {
        MapLikeBranch::new(label, projection, lifecycle, HLeaf, HLeaf)
    }
}

impl<Context, K, V, Item, LC, L, R> MapLikeBranch<Context, K, V, Item, LC, L, R>
where
    L: HTree,
    R: HTree,
{
    pub fn new(
        label: &'static str,
        projection: fn(&Context) -> &Item,
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
        MapLikeBranch {
            _type: PhantomData,
            label,
            projection,
            lifecycle,
            left,
            right,
        }
    }
}

impl<Context, K, V, Item, LC, L, R> ItemEvent<Context>
    for MapLikeBranch<Context, K, V, Item, LC, L, R>
where
    K: Clone + Eq + Hash,
    Item: MapItem<K, V>,
    LC: MapLaneLifecycle<K, V, Context>,
    L: HTree + ItemEvent<Context>,
    R: HTree + ItemEvent<Context>,
{
    type ItemEventHandler<'a> = MapBranchHandler<'a, Context, K, V, LC, L, R>
    where
        Self: 'a;

    fn item_event<'a>(
        &'a self,
        context: &Context,
        item_name: &str,
    ) -> Option<Self::ItemEventHandler<'a>> {
        let MapLikeBranch {
            label,
            projection,
            lifecycle,
            left,
            right,
            ..
        } = self;
        match item_name.cmp(*label) {
            Ordering::Less => left.item_event(context, item_name).map(Either::Left),
            Ordering::Equal => {
                let lane = projection(context);
                let handler =
                    lane.read_with_prev(|prev, map| prev.map(|ev| map_handler(ev, lifecycle, map)));
                handler.map(|h| Either::Right(Either::Left(h)))
            }
            Ordering::Greater => right
                .item_event(context, item_name)
                .map(|r| Either::Right(Either::Right(r))),
        }
    }
}

impl<Context, Shared, K, V, Item, LC, L, R> ItemEventShared<Context, Shared>
    for MapLikeBranch<Context, K, V, Item, LC, L, R>
where
    K: Clone + Eq + Hash,
    Item: MapItem<K, V>,
    LC: MapLaneLifecycleShared<K, V, Context, Shared>,
    L: HTree + ItemEventShared<Context, Shared>,
    R: HTree + ItemEventShared<Context, Shared>,
{
    type ItemEventHandler<'a> = MapBranchHandlerShared<'a, Context, Shared, K, V, LC, L, R>
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
        let MapLikeBranch {
            label,
            projection,
            lifecycle,
            left,
            right,
            ..
        } = self;
        match item_name.cmp(*label) {
            Ordering::Less => left
                .item_event(shared, handler_context, context, item_name)
                .map(Either::Left),
            Ordering::Equal => {
                let lane = projection(context);
                let handler = lane.read_with_prev(|prev, map| {
                    prev.map(|ev| map_handler_shared(shared, handler_context, ev, lifecycle, map))
                });
                handler.map(|h| Either::Right(Either::Left(h)))
            }
            Ordering::Greater => right
                .item_event(shared, handler_context, context, item_name)
                .map(|r| Either::Right(Either::Right(r))),
        }
    }
}
