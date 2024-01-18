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
use uuid::Uuid;

use wasm_ir::wpc::EnvAccess;

use crate::agent::AgentContext;
use crate::prelude::lanes::handlers::{Done, HandlerEffect, HandlerError};
use crate::prelude::lanes::value::ValueLaneModel;

#[derive(Debug, thiserror::Error)]
pub enum RouterError {
    #[error("No such route: '{0}'")]
    NoSuchRoute(u64),
    #[error("{0}")]
    Handler(#[from] HandlerError),
}

pub trait EventRouter<A> {
    type EventHandler: HandlerEffect<A>;

    fn or<T>(self, left: T) -> EitherRoute<T, Self>
    where
        Self: Sized,
    {
        EitherRoute { left, right: self }
    }

    fn resolves(&self, route: u64) -> bool;

    fn init_value(
        &mut self,
        _agent: &mut A,
        _route: u64,
        _buffer: BytesMut,
    ) -> Result<(), RouterError> {
        Ok(())
    }

    fn init_map(
        &mut self,
        _agent: &mut A,
        _route: u64,
        _buffer: BytesMut,
    ) -> Result<(), RouterError> {
        Ok(())
    }

    fn sync<E>(
        &mut self,
        context: &mut AgentContext<A, E>,
        agent: &mut A,
        route: u64,
        remote: Uuid,
    ) -> Result<(), RouterError>
    where
        E: EnvAccess;

    fn event<E>(
        &mut self,
        context: &mut AgentContext<A, E>,
        agent: &mut A,
        route: u64,
    ) -> Result<Self::EventHandler, RouterError>
    where
        E: EnvAccess;

    fn value_command<E>(
        &mut self,
        context: &mut AgentContext<A, E>,
        agent: &mut A,
        route: u64,
        request: BytesMut,
    ) -> Result<Self::EventHandler, RouterError>
    where
        E: EnvAccess;

    fn map_command<E>(
        &mut self,
        context: &mut AgentContext<A, E>,
        agent: &mut A,
        route: u64,
        bytes: BytesMut,
    ) -> Result<Self::EventHandler, RouterError>
    where
        E: EnvAccess;
}

pub struct EitherRoute<L, R> {
    left: L,
    right: R,
}

impl<A, L, R> EventRouter<A> for EitherRoute<L, R>
where
    L: EventRouter<A>,
    R: EventRouter<A>,
{
    type EventHandler = Either<L::EventHandler, R::EventHandler>;

    fn resolves(&self, route: u64) -> bool {
        self.left.resolves(route) || self.right.resolves(route)
    }

    fn sync<E>(
        &mut self,
        context: &mut AgentContext<A, E>,
        agent: &mut A,
        route: u64,
        remote: Uuid,
    ) -> Result<(), RouterError>
    where
        E: EnvAccess,
    {
        let EitherRoute { left, right } = self;
        if left.resolves(route) {
            left.sync(context, agent, route, remote)
        } else if right.resolves(route) {
            right.sync(context, agent, route, remote)
        } else {
            Err(RouterError::NoSuchRoute(route))
        }
    }

    fn event<H>(
        &mut self,
        context: &mut AgentContext<A, H>,
        agent: &mut A,
        route: u64,
    ) -> Result<Self::EventHandler, RouterError>
    where
        H: EnvAccess,
    {
        let EitherRoute { left, right } = self;
        if left.resolves(route) {
            let next = left.event(context, agent, route)?;
            Ok(Either::Left(next))
        } else if right.resolves(route) {
            let next = right.event(context, agent, route)?;
            Ok(Either::Right(next))
        } else {
            Err(RouterError::NoSuchRoute(route))
        }
    }

    fn value_command<H>(
        &mut self,
        context: &mut AgentContext<A, H>,
        agent: &mut A,
        route: u64,
        bytes: BytesMut,
    ) -> Result<Self::EventHandler, RouterError>
    where
        H: EnvAccess,
    {
        let EitherRoute { left, right } = self;
        if left.resolves(route) {
            let next = left.value_command(context, agent, route, bytes)?;
            Ok(Either::Left(next))
        } else if right.resolves(route) {
            let next = right.value_command(context, agent, route, bytes)?;
            Ok(Either::Right(next))
        } else {
            Err(RouterError::NoSuchRoute(route))
        }
    }

    fn map_command<E>(
        &mut self,
        context: &mut AgentContext<A, E>,
        agent: &mut A,
        route: u64,
        bytes: BytesMut,
    ) -> Result<Self::EventHandler, RouterError>
    where
        E: EnvAccess,
    {
        let EitherRoute { left, right } = self;
        if left.resolves(route) {
            let next = left.map_command(context, agent, route, bytes)?;
            Ok(Either::Left(next))
        } else if right.resolves(route) {
            let next = right.map_command(context, agent, route, bytes)?;
            Ok(Either::Right(next))
        } else {
            Err(RouterError::NoSuchRoute(route))
        }
    }
}

pub struct ItemRoute<M> {
    id: u64,
    item: M,
}

impl<H> ItemRoute<H> {
    pub fn new(id: u64, item: H) -> ItemRoute<H> {
        ItemRoute { id, item }
    }
}

impl<L, A> EventRouter<A> for ItemRoute<L>
where
    L: ValueLaneModel<A>,
{
    type EventHandler = Either<L::EventEffect, Done>;

    fn resolves(&self, route: u64) -> bool {
        self.id.eq(&route)
    }

    fn init_value(
        &mut self,
        agent: &mut A,
        route: u64,
        buffer: BytesMut,
    ) -> Result<(), RouterError> {
        let ItemRoute { id, item } = self;
        if *id == route {
            Ok(item.init(agent, buffer)?)
        } else {
            Err(RouterError::NoSuchRoute(route))
        }
    }

    fn sync<E>(
        &mut self,
        context: &mut AgentContext<A, E>,
        agent: &mut A,
        route: u64,
        remote: Uuid,
    ) -> Result<(), RouterError>
    where
        E: EnvAccess,
    {
        let ItemRoute { id, item } = self;
        if *id == route {
            Ok(item.sync(context, agent, remote))
        } else {
            Err(RouterError::NoSuchRoute(route))
        }
    }

    fn event<E>(
        &mut self,
        context: &mut AgentContext<A, E>,
        agent: &mut A,
        route: u64,
    ) -> Result<Self::EventHandler, RouterError>
    where
        E: EnvAccess,
    {
        let ItemRoute { id, item } = self;
        if *id == route {
            Ok(Either::Left(item.event(context, agent)?))
        } else {
            Err(RouterError::NoSuchRoute(route))
        }
    }

    fn value_command<E>(
        &mut self,
        context: &mut AgentContext<A, E>,
        agent: &mut A,
        route: u64,
        request: BytesMut,
    ) -> Result<Self::EventHandler, RouterError>
    where
        E: EnvAccess,
    {
        let ItemRoute { id, item } = self;
        if *id == route {
            Ok(Either::Left(item.command(context, request, agent)?))
        } else {
            Err(RouterError::NoSuchRoute(route))
        }
    }

    fn map_command<E>(
        &mut self,
        _context: &mut AgentContext<A, E>,
        _agent: &mut A,
        _route: u64,
        _bytes: BytesMut,
    ) -> Result<Self::EventHandler, RouterError>
    where
        E: EnvAccess,
    {
        Ok(Either::Right(Done))
    }
}
