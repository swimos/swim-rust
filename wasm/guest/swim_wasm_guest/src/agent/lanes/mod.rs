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

use wasm_ir::wpc::EnvAccess;

use crate::agent::AgentContext;
use crate::prelude::lanes::handlers::{HandlerEffect, HandlerError};

pub mod handlers;
pub mod value;

const INFALLIBLE_SER: &str = "Serializing to recon should be infallible.";

pub type ItemProjection<A, I> = fn(&mut A) -> &mut I;

pub trait LaneLifecycle<A> {
    type Input;
    type Output;

    fn run<H>(
        &mut self,
        context: &mut AgentContext<A, H>,
        agent: &mut A,
        input: Self::Input,
    ) -> Result<Option<Self::Output>, HandlerError>
    where
        H: EnvAccess;
}

#[derive(Debug)]
pub struct NoLifecycle<T>(PhantomData<T>);

impl<T> Default for NoLifecycle<T> {
    fn default() -> Self {
        NoLifecycle(PhantomData::default())
    }
}

impl<A, T> LaneLifecycle<A> for NoLifecycle<T> {
    type Input = T;
    type Output = T;

    fn run<H>(
        &mut self,
        _context: &mut AgentContext<A, H>,
        _agent: &mut A,
        input: Self::Input,
    ) -> Result<Option<Self::Output>, HandlerError>
    where
        H: EnvAccess,
    {
        Ok(Some(input))
    }
}

pub trait ItemHandler<A> {
    type Effect: HandlerEffect<A>;

    fn event<H>(
        &mut self,
        context: &mut AgentContext<A, H>,
        agent: &mut A,
    ) -> Result<Self::Effect, HandlerError>
    where
        H: EnvAccess;

    fn request<H>(
        &mut self,
        context: &mut AgentContext<A, H>,
        agent: &mut A,
        request: BytesMut,
    ) -> Result<Self::Effect, HandlerError>
    where
        H: EnvAccess;
}
