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

use std::{cmp::Ordering, fmt::Debug};

use futures::future::Either;

use crate::{
    agent_lifecycle::utility::HandlerContext,
    lanes::http::{
        lifecycle::{
            HttpLaneLifecycle, HttpLaneLifecycleShared, HttpLifecycleHandler,
            HttpLifecycleHandlerShared,
        },
        HttpLane, HttpLaneCodec,
    },
};

use super::{HLeaf, HTree, ItemEvent, ItemEventShared};

#[cfg(test)]
mod tests;

pub type HttpLeaf<Context, Get, Post, Put, Codec, LC> =
    HttpBranch<Context, Get, Post, Put, Codec, LC, HLeaf, HLeaf>;

type HttpBranchHandler<'a, Context, Get, Post, Put, Codec, LC, L, R> = Either<
    <L as ItemEvent<Context>>::ItemEventHandler<'a>,
    Either<
        HttpLifecycleHandler<'a, Context, Get, Post, Put, Codec, LC>,
        <R as ItemEvent<Context>>::ItemEventHandler<'a>,
    >,
>;

type HttpBranchHandlerShared<'a, Context, Shared, Get, Post, Put, Codec, LC, L, R> = Either<
    <L as ItemEventShared<Context, Shared>>::ItemEventHandler<'a>,
    Either<
        HttpLifecycleHandlerShared<'a, Context, Shared, Get, Post, Put, Codec, LC>,
        <R as ItemEventShared<Context, Shared>>::ItemEventHandler<'a>,
    >,
>;

/// HTTP lane lifecycle as a branch node of an [`HTree`].
pub struct HttpBranch<Context, Get, Post, Put, Codec, LC, L, R> {
    label: &'static str,
    projection: fn(&Context) -> &HttpLane<Get, Post, Put, Codec>,
    lifecycle: LC,
    left: L,
    right: R,
}

impl<Context, Get, Post, Put, Codec, LC, L, R> Clone
    for HttpBranch<Context, Get, Post, Put, Codec, LC, L, R>
where
    LC: Clone,
    L: Clone,
    R: Clone,
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

impl<Context, Get, Post, Put, Codec, LC, L, R> HTree
    for HttpBranch<Context, Get, Post, Put, Codec, LC, L, R>
{
    fn label(&self) -> Option<&'static str> {
        Some(self.label)
    }
}

impl<Context, Get, Post, Put, Codec, LC, L, R> Debug
    for HttpBranch<Context, Get, Post, Put, Codec, LC, L, R>
where
    LC: Debug,
    L: Debug,
    R: Debug,
{
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("HttpBranch")
            .field("label", &self.label)
            .field("projection", &"...")
            .field("lifecycle", &self.lifecycle)
            .field("left", &self.left)
            .field("right", &self.right)
            .finish()
    }
}

impl<Context, Get, Post, Put, Codec, LC>
    HttpBranch<Context, Get, Post, Put, Codec, LC, HLeaf, HLeaf>
{
    pub fn leaf(
        label: &'static str,
        projection: fn(&Context) -> &HttpLane<Get, Post, Put, Codec>,
        lifecycle: LC,
    ) -> Self {
        HttpBranch::new(label, projection, lifecycle, HLeaf, HLeaf)
    }
}

impl<Context, Get, Post, Put, Codec, LC, L, R> HttpBranch<Context, Get, Post, Put, Codec, LC, L, R>
where
    L: HTree,
    R: HTree,
{
    pub fn new(
        label: &'static str,
        projection: fn(&Context) -> &HttpLane<Get, Post, Put, Codec>,
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
        HttpBranch {
            label,
            projection,
            lifecycle,
            left,
            right,
        }
    }
}

impl<Context, Get, Post, Put, Codec, LC, L, R> ItemEvent<Context>
    for HttpBranch<Context, Get, Post, Put, Codec, LC, L, R>
where
    Codec: HttpLaneCodec<Get>,
    LC: HttpLaneLifecycle<Get, Post, Put, Context>,
    L: HTree + ItemEvent<Context>,
    R: HTree + ItemEvent<Context>,
{
    type ItemEventHandler<'a> = HttpBranchHandler<'a, Context, Get, Post, Put, Codec, LC, L, R>
    where
        Self: 'a;

    fn item_event<'a>(
        &'a self,
        context: &Context,
        item_name: &str,
    ) -> Option<Self::ItemEventHandler<'a>> {
        let HttpBranch {
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
                lane.take_request().map(|request| {
                    Either::Right(Either::Left(HttpLifecycleHandler::new(
                        request,
                        lane.codec(),
                        lifecycle,
                    )))
                })
            }
            Ordering::Greater => right
                .item_event(context, item_name)
                .map(|r| Either::Right(Either::Right(r))),
        }
    }
}

impl<Context, Shared, Get, Post, Put, Codec, LC, L, R> ItemEventShared<Context, Shared>
    for HttpBranch<Context, Get, Post, Put, Codec, LC, L, R>
where
    LC: HttpLaneLifecycleShared<Get, Post, Put, Context, Shared>,
    Codec: HttpLaneCodec<Get>,
    L: HTree + ItemEventShared<Context, Shared>,
    R: HTree + ItemEventShared<Context, Shared>,
{
    type ItemEventHandler<'a> = HttpBranchHandlerShared<'a, Context, Shared, Get, Post, Put, Codec, LC, L, R>
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
        let HttpBranch {
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
                lane.take_request().map(|request| {
                    Either::Right(Either::Left(HttpLifecycleHandlerShared::new(
                        request,
                        shared,
                        lane.codec(),
                        lifecycle,
                    )))
                })
            }
            Ordering::Greater => right
                .item_event(shared, handler_context, context, item_name)
                .map(|r| Either::Right(Either::Right(r))),
        }
    }
}
