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

use std::{fmt::Debug, marker::PhantomData};

use std::cmp::Ordering;

use futures::future::Either;

use crate::{
    agent_lifecycle::utility::HandlerContext,
    event_handler::{FollowedBy, HandlerActionExt},
    item::ValueItem,
    lanes::value::{
        lifecycle::{
            on_event::{OnEvent, OnEventShared},
            on_set::{OnSet, OnSetShared},
            ValueLaneLifecycle, ValueLaneLifecycleShared,
        },
        ValueLane,
    },
    stores::ValueStore,
};

use super::{HLeaf, HTree, ItemEvent, ItemEventShared};

#[cfg(test)]
mod tests;

pub type ValueLeaf<Context, T, LC> = ValueBranch<Context, T, LC, HLeaf, HLeaf>;
pub type ValueStoreLeaf<Context, T, LC> = ValueStoreBranch<Context, T, LC, HLeaf, HLeaf>;

/// Value item lifecycle as a branch node of an [`HTree`].
pub struct ValueLikeBranch<Context, T, Item, LC, L, R> {
    _type: PhantomData<fn(T) -> T>,
    label: &'static str,
    projection: fn(&Context) -> &Item,
    lifecycle: LC,
    left: L,
    right: R,
}

pub type ValueBranch<Context, T, LC, L, R> = ValueLikeBranch<Context, T, ValueLane<T>, LC, L, R>;
pub type ValueStoreBranch<Context, T, LC, L, R> =
    ValueLikeBranch<Context, T, ValueStore<T>, LC, L, R>;

impl<Context, T, Item, LC: Clone, L: Clone, R: Clone> Clone
    for ValueLikeBranch<Context, T, Item, LC, L, R>
{
    fn clone(&self) -> Self {
        Self {
            _type: Default::default(),
            label: self.label,
            projection: self.projection,
            lifecycle: self.lifecycle.clone(),
            left: self.left.clone(),
            right: self.right.clone(),
        }
    }
}

impl<Context, T, Item, LC, L, R> Debug for ValueLikeBranch<Context, T, Item, LC, L, R>
where
    LC: Debug,
    L: Debug,
    R: Debug,
{
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ValueLikeBranch")
            .field("label", &self.label)
            .field("projection", &"...")
            .field("lifecycle", &self.lifecycle)
            .field("left", &self.left)
            .field("right", &self.right)
            .finish()
    }
}

impl<Context, T, Item, LC> ValueLikeBranch<Context, T, Item, LC, HLeaf, HLeaf> {
    pub fn leaf(label: &'static str, projection: fn(&Context) -> &Item, lifecycle: LC) -> Self {
        Self::new(label, projection, lifecycle, HLeaf, HLeaf)
    }
}

impl<Context, T, Item, LC, L, R> ValueLikeBranch<Context, T, Item, LC, L, R>
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
        ValueLikeBranch {
            _type: PhantomData,
            label,
            projection,
            lifecycle,
            left,
            right,
        }
    }
}

impl<Context, T, Item, LC, L: HTree, R: HTree> HTree
    for ValueLikeBranch<Context, T, Item, LC, L, R>
{
    fn label(&self) -> Option<&'static str> {
        Some(self.label)
    }
}

pub type ValueLifecycleHandler<'a, Context, T, LC> = FollowedBy<
    <LC as OnEvent<T, Context>>::OnEventHandler<'a>,
    <LC as OnSet<T, Context>>::OnSetHandler<'a>,
>;

pub type ValueLifecycleHandlerShared<'a, Context, Shared, T, LC> = FollowedBy<
    <LC as OnEventShared<T, Context, Shared>>::OnEventHandler<'a>,
    <LC as OnSetShared<T, Context, Shared>>::OnSetHandler<'a>,
>;

type ValueBranchHandler<'a, Context, T, LC, L, R> = Either<
    <L as ItemEvent<Context>>::ItemEventHandler<'a>,
    Either<
        ValueLifecycleHandler<'a, Context, T, LC>,
        <R as ItemEvent<Context>>::ItemEventHandler<'a>,
    >,
>;

type ValueBranchHandlerShared<'a, Context, Shared, T, LC, L, R> = Either<
    <L as ItemEventShared<Context, Shared>>::ItemEventHandler<'a>,
    Either<
        ValueLifecycleHandlerShared<'a, Context, Shared, T, LC>,
        <R as ItemEventShared<Context, Shared>>::ItemEventHandler<'a>,
    >,
>;

impl<Context, T, Item, LC, L, R> ItemEvent<Context> for ValueLikeBranch<Context, T, Item, LC, L, R>
where
    Item: ValueItem<T>,
    LC: ValueLaneLifecycle<T, Context>,
    L: HTree + ItemEvent<Context>,
    R: HTree + ItemEvent<Context>,
{
    type ItemEventHandler<'a> = ValueBranchHandler<'a, Context, T, LC, L, R>
    where
        Self: 'a;

    fn item_event<'a>(
        &'a self,
        context: &Context,
        item_name: &str,
    ) -> Option<Self::ItemEventHandler<'a>> {
        let ValueLikeBranch {
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
                let handler = lane.read_with_prev(|prev, new_value| {
                    let event_handler = lifecycle.on_event(new_value);
                    let set_handler = lifecycle.on_set(prev, new_value);
                    event_handler.followed_by(set_handler)
                });
                Some(Either::Right(Either::Left(handler)))
            }
            Ordering::Greater => right
                .item_event(context, item_name)
                .map(|r| Either::Right(Either::Right(r))),
        }
    }
}

impl<Context, Shared, T, Item, LC, L, R> ItemEventShared<Context, Shared>
    for ValueLikeBranch<Context, T, Item, LC, L, R>
where
    Item: ValueItem<T>,
    LC: ValueLaneLifecycleShared<T, Context, Shared>,
    L: HTree + ItemEventShared<Context, Shared>,
    R: HTree + ItemEventShared<Context, Shared>,
{
    type ItemEventHandler<'a> = ValueBranchHandlerShared<'a, Context, Shared, T, LC, L, R>
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
        let ValueLikeBranch {
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
                let handler = lane.read_with_prev(|prev, new_value| {
                    let event_handler = lifecycle.on_event(shared, handler_context, new_value);
                    let set_handler = lifecycle.on_set(shared, handler_context, new_value, prev);
                    event_handler.followed_by(set_handler)
                });
                Some(Either::Right(Either::Left(handler)))
            }
            Ordering::Greater => right
                .item_event(shared, handler_context, context, item_name)
                .map(|r| Either::Right(Either::Right(r))),
        }
    }
}
