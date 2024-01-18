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
use tokio_util::codec::Encoder;
use uuid::Uuid;

pub use standard::*;
use swim_form::structural::read::recognizer::RecognizerReadable;
use swim_form::structural::read::ReadError;
use swim_form::structural::write::StructuralWritable;
use swim_protocol::agent::{LaneResponse, ValueLaneResponseEncoder};
use swim_recon::parser::{parse_recognize, ParseError, Span};
use wasm_ir::requests::{
    DispatchLaneResponsesProcedure, GuestLaneResponses, IdentifiedLaneResponseEncoder,
};
use wasm_ir::wpc::EnvAccess;

use crate::agent::lanes::INFALLIBLE_SER;
use crate::agent::AgentContext;
use crate::prelude::lanes::handlers::{HandlerEffect, HandlerError, HandlerErrorKind};

pub mod modular;
mod standard;

fn value_decode<T>(input: BytesMut) -> Result<T, HandlerError>
where
    T: RecognizerReadable,
{
    std::str::from_utf8(input.as_ref())
        .map_err(|e| ParseError::Structure(ReadError::Message(e.to_string().into())))
        .and_then(|str| parse_recognize::<T>(Span::new(str), false))
        .map_err(|e| HandlerError::new("Decoder", HandlerErrorKind::Parse(e)))
}

pub trait ValueLaneModel<A> {
    type EventEffect: HandlerEffect<A>;

    fn init(&mut self, agent: &mut A, with: BytesMut) -> Result<(), HandlerError>;

    fn sync<H>(&self, context: &mut AgentContext<A, H>, agent: &mut A, remote: Uuid)
    where
        H: EnvAccess;

    fn event<H>(
        &mut self,
        context: &mut AgentContext<A, H>,
        agent: &mut A,
    ) -> Result<Self::EventEffect, HandlerError>
    where
        H: EnvAccess;

    fn command<H>(
        &mut self,
        context: &mut AgentContext<A, H>,
        data: BytesMut,
        agent: &mut A,
    ) -> Result<Self::EventEffect, HandlerError>
    where
        H: EnvAccess;
}

fn propagate_event<H, A, T>(id: u64, val: &T, ctx: &mut AgentContext<A, H>)
where
    H: EnvAccess,
    T: StructuralWritable,
{
    let mut buffer = BytesMut::new();
    let mut encoder = IdentifiedLaneResponseEncoder::new(id, ValueLaneResponseEncoder::default());

    let synced_response = LaneResponse::<&T>::event(val);
    encoder
        .encode(synced_response, &mut buffer)
        .expect(INFALLIBLE_SER);

    ctx.dispatch::<DispatchLaneResponsesProcedure, _>(GuestLaneResponses { responses: buffer });
}
