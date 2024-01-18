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

use bytes::BytesMut;
use either::Either;
use tokio_util::codec::Encoder;
use uuid::Uuid;

use swim_form::structural::read::recognizer::RecognizerReadable;
use swim_form::structural::write::StructuralWritable;
use swim_protocol::agent::{LaneResponse, ValueLaneResponseEncoder};
use wasm_ir::requests::{GuestLaneResponses, IdentifiedLaneResponseEncoder, LaneSyncProcedure};
use wasm_ir::wpc::EnvAccess;

use crate::agent::lanes::value::propagate_event;
use crate::agent::lanes::INFALLIBLE_SER;
use crate::agent::{AgentContext, AgentItem};
use crate::prelude::lanes::handlers::{
    Done, EventHandlerError, HandlerContext, HandlerEffect, HandlerError, Modification, StepResult,
};
use crate::prelude::lanes::value::{value_decode, ValueLane, ValueLaneModel};
use crate::prelude::lanes::{ItemProjection, LaneLifecycle};

#[derive(Debug)]
pub struct ValueLaneHandler<A, H, T> {
    projection: ItemProjection<A, ValueLane<T>>,
    handler: H,
}

impl<A, M, T> ValueLaneHandler<A, M, T> {
    pub fn new(
        projection: ItemProjection<A, ValueLane<T>>,
        handler: M,
    ) -> ValueLaneHandler<A, M, T> {
        ValueLaneHandler {
            projection,
            handler,
        }
    }
}

impl<A, Hand, T, N> ValueLaneModel<A> for ValueLaneHandler<A, Hand, T>
where
    Hand: LaneLifecycle<A, Input = T, Output = (T, N)>,
    N: HandlerEffect<A>,
    T: RecognizerReadable + StructuralWritable,
{
    type EventEffect = Either<N, Done>;

    fn init(&mut self, agent: &mut A, data: BytesMut) -> Result<(), HandlerError> {
        let ValueLaneHandler { projection, .. } = self;

        let val = value_decode(data)?;
        let lane = projection(agent);

        lane.set(val);
        Ok(())
    }

    fn sync<H>(&self, context: &mut AgentContext<A, H>, agent: &mut A, remote: Uuid)
    where
        H: EnvAccess,
    {
        let ValueLaneHandler { projection, .. } = self;

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
        let ValueLaneHandler {
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
        let ValueLaneHandler {
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

pub struct UpdateValueHandler<A, T> {
    projection: ItemProjection<A, ValueLane<T>>,
    to: Option<T>,
}

impl<A, T> UpdateValueHandler<A, T> {
    pub fn new(projection: ItemProjection<A, ValueLane<T>>, to: T) -> UpdateValueHandler<A, T> {
        UpdateValueHandler {
            projection,
            to: Some(to),
        }
    }
}

impl<A, T> HandlerEffect<A> for UpdateValueHandler<A, T> {
    fn step<H>(&mut self, _context: &AgentContext<A, H>, agent: &mut A) -> StepResult
    where
        H: EnvAccess,
    {
        let UpdateValueHandler { projection, to } = self;
        match to.take() {
            Some(val) => {
                let lane = projection(agent);
                lane.set(val);
                StepResult::Complete {
                    modified_item: Some(Modification::of(lane.id())),
                }
            }
            None => StepResult::Fail(EventHandlerError::SteppedAfterComplete),
        }
    }
}

pub struct ValueLaneUpdate<A, T, H, F> {
    projection: ItemProjection<A, ValueLane<T>>,
    handler_chain: H,
    on_update: F,
}

impl<A, T, H, F> ValueLaneUpdate<A, T, H, F> {
    pub fn new(
        projection: ItemProjection<A, ValueLane<T>>,
        handler_chain: H,
        on_update: F,
    ) -> ValueLaneUpdate<A, T, H, F> {
        ValueLaneUpdate {
            projection,
            handler_chain,
            on_update,
        }
    }
}

impl<A, F, H> LaneLifecycle<A> for ValueLaneUpdate<A, H::Output, H, F>
where
    H: LaneLifecycle<A>,
    F: Fn(Option<&H::Output>, &H::Output),
{
    type Input = H::Input;
    type Output = (H::Output, Done);

    fn run<E>(
        &mut self,
        context: &mut AgentContext<A, E>,
        agent: &mut A,
        input: Self::Input,
    ) -> Result<Option<Self::Output>, HandlerError>
    where
        E: EnvAccess,
    {
        let ValueLaneUpdate {
            projection,
            handler_chain,
            on_update,
        } = self;
        match handler_chain.run(context, agent, input) {
            Ok(Some(o)) => {
                let lane = projection(agent);
                let previous = lane.previous();
                on_update(previous, &o);
                Ok(Some((o, Done)))
            }
            Ok(None) => Ok(None),
            Err(e) => Err(e),
        }
    }
}

pub struct ValueLaneUpdateThen<A, T, H, F> {
    projection: ItemProjection<A, ValueLane<T>>,
    handler_chain: H,
    on_update: F,
}

impl<A, T, H, F> ValueLaneUpdateThen<A, T, H, F> {
    pub fn new(
        projection: ItemProjection<A, ValueLane<T>>,
        handler_chain: H,
        on_update: F,
    ) -> ValueLaneUpdateThen<A, T, H, F> {
        ValueLaneUpdateThen {
            projection,
            handler_chain,
            on_update,
        }
    }
}

impl<A, F, H, Next> LaneLifecycle<A> for ValueLaneUpdateThen<A, H::Output, H, F>
where
    H: LaneLifecycle<A>,
    F: Fn(&HandlerContext<A>, Option<&H::Output>, &H::Output) -> Next,
    Next: HandlerEffect<A>,
{
    type Input = H::Input;
    type Output = (H::Output, Next);

    fn run<E>(
        &mut self,
        context: &mut AgentContext<A, E>,
        agent: &mut A,
        input: Self::Input,
    ) -> Result<Option<Self::Output>, HandlerError>
    where
        E: EnvAccess,
    {
        let ValueLaneUpdateThen {
            projection,
            handler_chain,
            on_update,
        } = self;
        match handler_chain.run(context, agent, input) {
            Ok(Some(o)) => {
                let lane = projection(agent);
                let previous = lane.previous();
                let next = on_update(&context.handler(), previous, &o);
                Ok(Some((o, next)))
            }
            Ok(None) => Ok(None),
            Err(e) => Err(e),
        }
    }
}
