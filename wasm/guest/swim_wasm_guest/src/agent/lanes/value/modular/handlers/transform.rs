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

use wasm_ir::wpc::EnvAccess;

use crate::agent::AgentContext;
use crate::prelude::lanes::handlers::HandlerError;
use crate::prelude::lanes::LaneLifecycle;

pub struct FilterHandler<H, T> {
    handler_chain: H,
    filter: T,
}

impl<H, T> FilterHandler<H, T> {
    pub fn new(handler_chain: H, filter: T) -> FilterHandler<H, T> {
        FilterHandler {
            handler_chain,
            filter,
        }
    }
}

impl<A, H, F> LaneLifecycle<A> for FilterHandler<H, F>
where
    H: LaneLifecycle<A>,
    F: Fn(&H::Output) -> bool + 'static,
{
    type Input = H::Input;
    type Output = H::Output;

    fn run<E>(
        &mut self,
        context: &mut AgentContext<A, E>,
        agent: &mut A,
        input: Self::Input,
    ) -> Result<Option<Self::Output>, HandlerError>
    where
        E: EnvAccess,
    {
        let FilterHandler {
            handler_chain,
            filter,
        } = self;
        match handler_chain.run(context, agent, input) {
            Ok(Some(o)) => {
                if filter(&o) {
                    Ok(Some(o))
                } else {
                    Ok(None)
                }
            }
            Ok(None) => Ok(None),
            Err(e) => Err(e),
        }
    }
}

pub struct MapHandler<H, T, Y> {
    handler_chain: H,
    map: T,
    _ty: PhantomData<Y>,
}

impl<H, T, Y> MapHandler<H, T, Y> {
    pub fn new(handler_chain: H, map: T) -> MapHandler<H, T, Y> {
        MapHandler {
            handler_chain,
            map,
            _ty: PhantomData::default(),
        }
    }
}

impl<A, H, F, Y> LaneLifecycle<A> for MapHandler<H, F, Y>
where
    H: LaneLifecycle<A>,
    F: Fn(H::Output) -> Y,
{
    type Input = H::Input;
    type Output = Y;

    fn run<E>(
        &mut self,
        context: &mut AgentContext<A, E>,
        agent: &mut A,
        input: Self::Input,
    ) -> Result<Option<Self::Output>, HandlerError>
    where
        E: EnvAccess,
    {
        let MapHandler {
            handler_chain,
            map,
            _ty,
        } = self;
        match handler_chain.run(context, agent, input) {
            Ok(Some(o)) => Ok(Some(map(o))),
            Ok(None) => Ok(None),
            Err(e) => Err(e),
        }
    }
}

pub struct FilterMapHandler<H, F, Y> {
    handler_chain: H,
    func: F,
    _ty: PhantomData<Y>,
}

impl<H, F, Y> FilterMapHandler<H, F, Y> {
    pub fn new(handler_chain: H, func: F) -> FilterMapHandler<H, F, Y> {
        FilterMapHandler {
            handler_chain,
            func,
            _ty: PhantomData::default(),
        }
    }
}

impl<A, H, F, Y> LaneLifecycle<A> for FilterMapHandler<H, F, Y>
where
    H: LaneLifecycle<A>,
    F: Fn(H::Output) -> Option<Y>,
{
    type Input = H::Input;
    type Output = Y;

    fn run<E>(
        &mut self,
        context: &mut AgentContext<A, E>,
        agent: &mut A,
        input: Self::Input,
    ) -> Result<Option<Self::Output>, HandlerError>
    where
        E: EnvAccess,
    {
        let FilterMapHandler {
            handler_chain,
            func,
            _ty,
        } = self;
        match handler_chain.run(context, agent, input) {
            Ok(Some(o)) => Ok(func(o)),
            Ok(None) => Ok(None),
            Err(e) => Err(e),
        }
    }
}
