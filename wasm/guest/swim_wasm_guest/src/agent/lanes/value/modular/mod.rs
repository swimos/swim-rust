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

use bytes::BytesMut;
use either::Either;
use tokio_util::codec::Encoder;
use uuid::Uuid;

use handlers::lifecycle::{ModularValueLaneUpdate, ValueLaneUpdateThen};
use handlers::transform::{FilterHandler, FilterMapHandler, MapHandler};
use swim_form::structural::read::recognizer::RecognizerReadable;
use swim_form::structural::write::StructuralWritable;
use swim_protocol::agent::{LaneResponse, ValueLaneResponseEncoder};
use wasm_ir::requests::{GuestLaneResponses, IdentifiedLaneResponseEncoder, LaneSyncProcedure};
use wasm_ir::wpc::EnvAccess;
use wasm_ir::LaneKindRepr;

use crate::agent::lanes::value::propagate_event;
use crate::agent::lanes::INFALLIBLE_SER;
use crate::agent::{
    AgentContext, AgentItem, AgentSpecBuilder, ItemRoute, LaneItem, MutableValueLikeItem,
};
use crate::prelude::lanes::handlers::{Done, HandlerContext, HandlerEffect, HandlerError};
use crate::prelude::lanes::value::modular::handlers::operational::TryUpdateValueHandler;
use crate::prelude::lanes::value::{value_decode, ValueLaneModel};
use crate::prelude::lanes::{ItemProjection, LaneLifecycle, NoLifecycle};

#[cfg(test)]
mod tests;

pub mod handlers;

#[derive(Debug)]
pub struct ModularValueLane<I, S> {
    id: u64,
    pending: Option<S>,
    old: Option<S>,
    current: S,
    _ty: PhantomData<fn(I) -> S>,
}

impl<I, S> ModularValueLane<I, S> {
    fn new(id: u64, initial: S) -> ModularValueLane<I, S> {
        ModularValueLane {
            id,
            pending: None,
            old: None,
            current: initial,
            _ty: PhantomData::default(),
        }
    }

    fn get(&self) -> &S {
        &self.current
    }

    fn previous(&self) -> Option<&S> {
        self.old.as_ref()
    }

    fn try_set(&mut self, to: S) {
        self.pending = Some(to);
    }

    fn set(&mut self, to: S) {
        self.current = to;
    }
}

impl<S> ModularValueLane<S, S> {
    pub fn builder(
        spec: &mut AgentSpecBuilder,
        uri: &str,
        transient: bool,
    ) -> ModularValueLaneBuilder<S, S, NoLifecycle<S>> {
        let id = spec.push::<ModularValueLane<S, S>>(uri, transient);
        ModularValueLaneBuilder::new(NoLifecycle::default(), id)
    }
}

impl<I, S> AgentItem for ModularValueLane<I, S> {
    fn id(&self) -> u64 {
        self.id
    }
}

impl<I, S> LaneItem for ModularValueLane<I, S> {
    fn kind() -> LaneKindRepr {
        LaneKindRepr::Value
    }
}

pub struct ModularValueLaneBuilder<I, S, H> {
    handlers: H,
    id: u64,
    _ty: PhantomData<fn(I) -> S>,
}

impl<I> ModularValueLaneBuilder<I, I, NoLifecycle<I>> {
    fn new(handlers: NoLifecycle<I>, id: u64) -> ModularValueLaneBuilder<I, I, NoLifecycle<I>> {
        ModularValueLaneBuilder {
            handlers,
            id,
            _ty: Default::default(),
        }
    }
}

impl<I, S, H> ModularValueLaneBuilder<I, S, H> {
    pub fn filter<F>(self, filter: F) -> ModularValueLaneBuilder<I, S, FilterHandler<H, F>>
    where
        F: Fn(&S) -> bool + 'static,
    {
        let ModularValueLaneBuilder { _ty, handlers, id } = self;
        ModularValueLaneBuilder {
            _ty,
            handlers: FilterHandler::new(handlers, filter),
            id,
        }
    }

    pub fn map<F, Y>(self, map: F) -> ModularValueLaneBuilder<I, Y, MapHandler<H, F, Y>>
    where
        F: Fn(S) -> Y,
    {
        let ModularValueLaneBuilder { _ty, handlers, id } = self;
        ModularValueLaneBuilder {
            _ty: PhantomData::default(),
            handlers: MapHandler::new(handlers, map),
            id,
        }
    }

    pub fn filter_map<F, Y>(
        self,
        func: F,
    ) -> ModularValueLaneBuilder<I, Y, FilterMapHandler<H, F, Y>>
    where
        F: Fn(S) -> Option<Y>,
    {
        let ModularValueLaneBuilder { _ty, handlers, id } = self;
        ModularValueLaneBuilder {
            _ty: PhantomData::default(),
            handlers: FilterMapHandler::new(handlers, func),
            id,
        }
    }

    pub fn on_update<A, F>(
        self,
        projection: ItemProjection<A, ModularValueLane<I, S>>,
        on_update: F,
    ) -> (
        ModularValueLane<I, S>,
        ItemRoute<ModularValueLaneHandler<I, A, ModularValueLaneUpdate<I, A, S, H, F>, S>>,
    )
    where
        F: Fn(Option<&S>, &S),
        S: Default,
    {
        let ModularValueLaneBuilder { _ty, handlers, id } = self;
        let lane = ModularValueLane::new(id, S::default());
        let handler = ModularValueLaneHandler::new(
            projection,
            ModularValueLaneUpdate::new(projection, handlers, on_update),
        );
        (lane, ItemRoute::new(id, handler))
    }

    pub fn on_update_then<A, F, N>(
        self,
        projection: ItemProjection<A, ModularValueLane<I, S>>,
        on_update: F,
    ) -> (
        ModularValueLane<I, S>,
        ItemRoute<ModularValueLaneHandler<I, A, ValueLaneUpdateThen<I, A, S, H, F>, S>>,
    )
    where
        F: Fn(&HandlerContext<A>, Option<&S>, &S) -> N,
        N: HandlerEffect<A>,
        S: Default,
    {
        let ModularValueLaneBuilder { _ty, handlers, id } = self;
        let lane = ModularValueLane::new(id, S::default());
        let handler = ModularValueLaneHandler::new(
            projection,
            ValueLaneUpdateThen::new(projection, handlers, on_update),
        );
        (lane, ItemRoute::new(id, handler))
    }

    pub fn build<A>(
        self,
        projection: ItemProjection<A, ModularValueLane<I, S>>,
    ) -> (
        ModularValueLane<I, S>,
        ItemRoute<ModularValueLaneHandler<I, A, H, S>>,
    )
    where
        S: Default,
    {
        let ModularValueLaneBuilder { _ty, handlers, id } = self;
        let lane = ModularValueLane::new(id, S::default());
        let handler = ModularValueLaneHandler::new(projection, handlers);
        (lane, ItemRoute::new(id, handler))
    }
}

#[derive(Debug)]
pub struct ModularValueLaneHandler<I, A, H, S> {
    projection: ItemProjection<A, ModularValueLane<I, S>>,
    handler: H,
}

impl<I, A, M, S> ModularValueLaneHandler<I, A, M, S> {
    pub fn new(
        projection: ItemProjection<A, ModularValueLane<I, S>>,
        handler: M,
    ) -> ModularValueLaneHandler<I, A, M, S> {
        ModularValueLaneHandler {
            projection,
            handler,
        }
    }
}

impl<I, S> MutableValueLikeItem<S> for ModularValueLane<I, S>
where
    S: Send + 'static,
    I: 'static,
{
    type UpdateHandler<C> = TryUpdateValueHandler<I, C, S>
        where
            C: 'static;

    fn update_handler<C: 'static>(
        projection: ItemProjection<C, Self>,
        value: S,
    ) -> Self::UpdateHandler<C> {
        TryUpdateValueHandler::new(projection, value)
    }
}

impl<A, I, Hand, T, N> ValueLaneModel<A> for ModularValueLaneHandler<I, A, Hand, T>
where
    Hand: LaneLifecycle<A, Input = T, Output = (T, N)>,
    N: HandlerEffect<A>,
    T: RecognizerReadable + StructuralWritable,
{
    type EventEffect = Either<N, Done>;

    fn init(&mut self, agent: &mut A, data: BytesMut) -> Result<(), HandlerError> {
        let ModularValueLaneHandler { projection, .. } = self;

        let val = value_decode(data)?;
        let lane = projection(agent);

        lane.set(val);
        Ok(())
    }

    fn sync<H>(&self, context: &mut AgentContext<A, H>, agent: &mut A, remote: Uuid)
    where
        H: EnvAccess,
    {
        let ModularValueLaneHandler { projection, .. } = self;

        let lane = projection(agent);
        let value_response = LaneResponse::sync_event(remote, lane.get());

        let mut buffer = BytesMut::new();
        let mut encoder =
            IdentifiedLaneResponseEncoder::new(lane.id(), ValueLaneResponseEncoder::default());

        encoder
            .encode(value_response, &mut buffer)
            .expect(INFALLIBLE_SER);

        let synced_response = LaneResponse::<&T>::synced(remote);
        encoder
            .encode(synced_response, &mut buffer)
            .expect(INFALLIBLE_SER);

        context.satisfy::<LaneSyncProcedure, _>(GuestLaneResponses { responses: buffer });
    }

    fn event<H>(
        &mut self,
        context: &mut AgentContext<A, H>,
        agent: &mut A,
    ) -> Result<Self::EventEffect, HandlerError>
    where
        H: EnvAccess,
    {
        let ModularValueLaneHandler {
            projection,
            handler,
        } = self;

        let pending = {
            let lane = projection(agent);
            match lane.pending.take() {
                Some(pending) => pending,
                None => return Ok(Either::Right(Done)),
            }
        };

        match handler.run(context, agent, pending) {
            Ok(Some((val, next))) => {
                let lane = projection(agent);
                lane.set(val);
                propagate_event(lane.id(), lane.get(), context);
                Ok(Either::Left(next))
            }
            Ok(None) => Ok(Either::Right(Done)),
            Err(e) => Err(e),
        }
    }

    fn command<H>(
        &mut self,
        context: &mut AgentContext<A, H>,
        data: BytesMut,
        agent: &mut A,
    ) -> Result<Self::EventEffect, HandlerError>
    where
        H: EnvAccess,
    {
        let ModularValueLaneHandler {
            projection,
            handler,
        } = self;

        let val = value_decode(data)?;
        match handler.run(context, agent, val) {
            Ok(Some((val, next))) => {
                let lane = projection(agent);
                lane.set(val);
                propagate_event(lane.id(), lane.get(), context);
                Ok(Either::Left(next))
            }
            Ok(None) => Ok(Either::Right(Done)),
            Err(e) => Err(e),
        }
    }
}
