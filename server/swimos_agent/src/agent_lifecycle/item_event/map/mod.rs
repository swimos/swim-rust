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

use std::cmp::Ordering;
use std::fmt::Debug;
use std::hash::Hash;
use std::marker::PhantomData;

use frunk::{Coprod, Coproduct};
use futures::future::Either;

use crate::agent_lifecycle::HandlerContext;
use crate::item::MapItem;
use crate::lanes::map::lifecycle::MapLaneLifecycleShared;
use crate::lanes::map::{
    lifecycle::{
        on_clear::{OnClear, OnClearShared},
        on_remove::{OnRemove, OnRemoveShared},
        on_update::{OnUpdate, OnUpdateShared},
        MapLaneLifecycle,
    },
    MapLaneEvent,
};
use crate::map_storage::MapOps;

use super::{HLeaf, HTree, ItemEvent, ItemEventShared};

#[cfg(test)]
mod tests;

pub type MapLifecycleHandler<'a, Context, K, V, M, LC> = Coprod!(
    <LC as OnUpdate<K, V, M, Context>>::OnUpdateHandler<'a>,
    <LC as OnRemove<K, V, M, Context>>::OnRemoveHandler<'a>,
    <LC as OnClear<M, Context>>::OnClearHandler<'a>,
);

pub type MapLifecycleHandlerShared<'a, Context, Shared, K, V, M, LC> = Coprod!(
    <LC as OnUpdateShared<K, V, M, Context, Shared>>::OnUpdateHandler<'a>,
    <LC as OnRemoveShared<K, V, M, Context, Shared>>::OnRemoveHandler<'a>,
    <LC as OnClearShared<M, Context, Shared>>::OnClearHandler<'a>,
);

type MapBranchHandler<'a, Context, K, V, M, LC, L, R> = Either<
    <L as ItemEvent<Context>>::ItemEventHandler<'a>,
    Either<
        MapLifecycleHandler<'a, Context, K, V, M, LC>,
        <R as ItemEvent<Context>>::ItemEventHandler<'a>,
    >,
>;

type MapBranchHandlerShared<'a, Context, Shared, K, V, M, LC, L, R> = Either<
    <L as ItemEventShared<Context, Shared>>::ItemEventHandler<'a>,
    Either<
        MapLifecycleHandlerShared<'a, Context, Shared, K, V, M, LC>,
        <R as ItemEventShared<Context, Shared>>::ItemEventHandler<'a>,
    >,
>;

fn map_handler<'a, Context, K, V, M, LC>(
    event: MapLaneEvent<K, V, M>,
    lifecycle: &'a LC,
    map: &M,
) -> MapLifecycleHandler<'a, Context, K, V, M, LC>
where
    K: Hash + Eq,
    LC: MapLaneLifecycle<K, V, M, Context>,
    M: MapOps<K, V>,
{
    match event {
        MapLaneEvent::Update(k, old) => {
            let new = map.get(&k).expect("Entry missing.");
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

fn map_handler_shared<'a, Context, Shared, K, V, M, LC>(
    shared: &'a Shared,
    handler_context: HandlerContext<Context>,
    event: MapLaneEvent<K, V, M>,
    lifecycle: &'a LC,
    map: &M,
) -> MapLifecycleHandlerShared<'a, Context, Shared, K, V, M, LC>
where
    K: Hash + Eq,
    LC: MapLaneLifecycleShared<K, V, M, Context, Shared>,
    M: MapOps<K, V>,
{
    match event {
        MapLaneEvent::Update(k, old) => {
            let new = map.get(&k).expect("Entry missing.");
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

type MapKeyValue<M, K, V> = fn((&M, K, V)) -> (K, V);
/// Map lane lifecycle as a branch node of an [`HTree`].
pub struct MapLikeBranch<Context, K, V, M, Item, LC, L, R> {
    _type: PhantomData<MapKeyValue<M, K, V>>,
    label: &'static str,
    projection: fn(&Context) -> &Item,
    lifecycle: LC,
    left: L,
    right: R,
}

impl<Context, K, V, M, Item, LC: Clone, L: Clone, R: Clone> Clone
    for MapLikeBranch<Context, K, V, M, Item, LC, L, R>
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

impl<Context, K, V, M, Item, LC, L: HTree, R: HTree> HTree
    for MapLikeBranch<Context, K, V, M, Item, LC, L, R>
{
    fn label(&self) -> Option<&'static str> {
        Some(self.label)
    }
}

impl<Context, K, V, M, Item, LC, L, R> Debug for MapLikeBranch<Context, K, V, M, Item, LC, L, R>
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

impl<Context, K, V, M, Item, LC> MapLikeBranch<Context, K, V, M, Item, LC, HLeaf, HLeaf> {
    pub fn leaf(label: &'static str, projection: fn(&Context) -> &Item, lifecycle: LC) -> Self {
        MapLikeBranch::new(label, projection, lifecycle, HLeaf, HLeaf)
    }
}

impl<Context, K, V, M, Item, LC, L, R> MapLikeBranch<Context, K, V, M, Item, LC, L, R>
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

impl<Context, K, V, M, Item, LC, L, R> ItemEvent<Context>
    for MapLikeBranch<Context, K, V, M, Item, LC, L, R>
where
    K: Clone + Eq + Hash,
    Item: MapItem<K, V, M>,
    LC: MapLaneLifecycle<K, V, M, Context>,
    L: HTree + ItemEvent<Context>,
    R: HTree + ItemEvent<Context>,
    M: MapOps<K, V>,
{
    type ItemEventHandler<'a> = MapBranchHandler<'a, Context, K, V, M, LC, L, R>
    where
        Self: 'a;

    fn item_event<'a>(
        &'a self,
        context: &Context,
        item_name: &'a str,
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

impl<Context, Shared, K, V, M, Item, LC, L, R> ItemEventShared<Context, Shared>
    for MapLikeBranch<Context, K, V, M, Item, LC, L, R>
where
    K: Clone + Eq + Hash,
    Item: MapItem<K, V, M>,
    LC: MapLaneLifecycleShared<K, V, M, Context, Shared>,
    L: HTree + ItemEventShared<Context, Shared>,
    R: HTree + ItemEventShared<Context, Shared>,
    M: MapOps<K, V>,
{
    type ItemEventHandler<'a> = MapBranchHandlerShared<'a, Context, Shared, K, V, M, LC, L, R>
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
