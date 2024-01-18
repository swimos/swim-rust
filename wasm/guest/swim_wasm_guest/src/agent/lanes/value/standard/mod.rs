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

use std::marker::PhantomData;
use std::mem::replace;

use wasm_ir::LaneKindRepr;

use crate::agent::{AgentItem, AgentSpecBuilder, ItemRoute, LaneItem, MutableValueLikeItem};
use crate::prelude::lanes::handlers::{HandlerContext, HandlerEffect};
use crate::prelude::lanes::value::standard::handlers::{
    UpdateValueHandler, ValueLaneHandler, ValueLaneUpdate, ValueLaneUpdateThen,
};
use crate::prelude::lanes::{ItemProjection, NoLifecycle};

mod handlers;

#[derive(Debug)]
pub struct ValueLane<T> {
    id: u64,
    pending: Option<T>,
    old: Option<T>,
    current: T,
}

impl<T> ValueLane<T> {
    fn new(id: u64, current: T) -> ValueLane<T> {
        ValueLane {
            id,
            pending: None,
            old: None,
            current,
        }
    }

    fn previous(&self) -> Option<&T> {
        self.old.as_ref()
    }

    fn get(&self) -> &T {
        &self.current
    }

    fn set(&mut self, to: T) {
        self.old = Some(replace(&mut self.current, to));
    }

    pub fn builder(
        spec: &mut AgentSpecBuilder,
        uri: &str,
        transient: bool,
    ) -> ValueLaneBuilder<T, NoLifecycle<T>> {
        ValueLaneBuilder {
            handlers: NoLifecycle::default(),
            id: spec.push::<ValueLane<T>>(uri, transient),
            _ty: Default::default(),
        }
    }
}

impl<T> AgentItem for ValueLane<T> {
    fn id(&self) -> u64 {
        self.id
    }
}

impl<T> LaneItem for ValueLane<T> {
    fn kind() -> LaneKindRepr {
        LaneKindRepr::Value
    }
}

impl<S> MutableValueLikeItem<S> for ValueLane<S>
where
    S: Send + 'static,
{
    type UpdateHandler<C> = UpdateValueHandler<C, S>
        where
            C: 'static;

    fn update_handler<C: 'static>(
        projection: ItemProjection<C, Self>,
        value: S,
    ) -> Self::UpdateHandler<C> {
        UpdateValueHandler::new(projection, value)
    }
}

pub struct ValueLaneBuilder<T, H> {
    handlers: H,
    id: u64,
    _ty: PhantomData<T>,
}

impl<T, H> ValueLaneBuilder<T, H> {
    pub fn on_update<A, F>(
        self,
        projection: ItemProjection<A, ValueLane<T>>,
        on_update: F,
    ) -> (
        ValueLane<T>,
        ItemRoute<ValueLaneHandler<A, ValueLaneUpdate<A, T, H, F>, T>>,
    )
    where
        F: Fn(Option<&T>, &T),
        T: Default,
    {
        let ValueLaneBuilder { handlers, id, _ty } = self;
        let lane = ValueLane::new(id, T::default());
        let handler = ValueLaneHandler::new(
            projection,
            ValueLaneUpdate::new(projection, handlers, on_update),
        );
        (lane, ItemRoute::new(id, handler))
    }

    pub fn on_update_then<A, F, N>(
        self,
        projection: ItemProjection<A, ValueLane<T>>,
        on_update: F,
    ) -> (
        ValueLane<T>,
        ItemRoute<ValueLaneHandler<A, ValueLaneUpdateThen<A, T, H, F>, T>>,
    )
    where
        F: Fn(&HandlerContext<A>, Option<&T>, &T) -> N,
        N: HandlerEffect<A>,
        T: Default,
    {
        let ValueLaneBuilder { _ty, handlers, id } = self;
        let lane = ValueLane::new(id, T::default());
        let handler = ValueLaneHandler::new(
            projection,
            ValueLaneUpdateThen::new(projection, handlers, on_update),
        );
        (lane, ItemRoute::new(id, handler))
    }

    pub fn build<A>(
        self,
        projection: ItemProjection<A, ValueLane<T>>,
    ) -> (ValueLane<T>, ItemRoute<ValueLaneHandler<A, H, T>>)
    where
        T: Default,
    {
        let ValueLaneBuilder { _ty, handlers, id } = self;
        let lane = ValueLane::new(id, T::default());
        let handler = ValueLaneHandler::new(projection, handlers);
        (lane, ItemRoute::new(id, handler))
    }
}
