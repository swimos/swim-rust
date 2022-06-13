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

use bytes::{BytesMut, Bytes};
use frunk::{HNil, Coprod, Coproduct, HCons};
use swim_form::structural::read::recognizer::Recognizer;
use swim_model::Text;
use swim_recon::parser::RecognizerDecoder;
use uuid::Uuid;

use crate::{lifecycle::on_command::OnReceive, event_handler::{ConstHandler, EventHandler}, lanes::{ValueLaneCommand, ValueLane, ValueLaneSync}};

pub struct LaneDispatcher<Lanes> {
    buffer: BytesMut,
    lanes: Lanes,
}

pub trait OnCommandWithBuffer<'a, Context>: Send {
    type OnCmdBufHandler: EventHandler<Context, Completion = ()> + Send + 'a;

    fn on_command_with(&'a mut self, buffer: &'a mut BytesMut, lane: Text, body: Bytes) -> Self::OnCmdBufHandler;
}

pub trait OnSyncRequest<'a, Context>: Send {
    type OnSyncReqHandler: EventHandler<Context, Completion = ()> + Send + 'a;

    fn on_sync_req(&'a mut self, lane: Text, id: Uuid) -> Self::OnSyncReqHandler;
}


impl<'a, Context> OnCommandWithBuffer<'a, Context> for HNil {
    type OnCmdBufHandler = Coprod!(ConstHandler<()>);

    fn on_command_with(&'a mut self, _buffer: &'a mut BytesMut, _lane: Text, _body: bytes::Bytes) -> Self::OnCmdBufHandler {
        Coproduct::Inl(ConstHandler::default())
    }
}

impl<'a, Context> OnSyncRequest<'a, Context> for HNil {
    type OnSyncReqHandler = Coprod!(ConstHandler<()>);

    fn on_sync_req(&'a mut self, _lane: Text, _id: Uuid) -> Self::OnSyncReqHandler {
        Coproduct::Inl(ConstHandler::default())
    }
}

pub struct ValueLaneCommandTarget<C, T, R> {
    name: Text,
    projection: for<'b> fn(&'b C) -> &'b ValueLane<T>,
    decoder: RecognizerDecoder<R>,
}

impl<'a, Context, T, R, Tail> OnCommandWithBuffer<'a, Context> for HCons<ValueLaneCommandTarget<Context, T, R>, Tail>
where
    T: 'static, 
    Context: 'a, 
    Tail: OnCommandWithBuffer<'a, Context>,
    R: Recognizer<Target = T> + Send + 'static, {
    type OnCmdBufHandler = Coproduct<ValueLaneCommand<'a, Context, T, R>, Tail::OnCmdBufHandler>;

    fn on_command_with(&'a mut self, buffer: &'a mut BytesMut, lane: Text, body: bytes::Bytes) -> Self::OnCmdBufHandler {
        let HCons { head, tail } = self;
        let ValueLaneCommandTarget { name, projection, decoder } = head;
        if lane == name {
            let cmd = ValueLaneCommand::new(*projection, decoder, buffer);
            Coproduct::Inl(cmd)
        } else {
            Coproduct::Inr(tail.on_command_with(buffer, lane, body))
        }
    }
}

impl<'a, Context, T, R, Tail> OnSyncRequest<'a, Context> for HCons<ValueLaneCommandTarget<Context, T, R>, Tail>
where
    T: 'static, 
    Context: 'a, 
    Tail: OnSyncRequest<'a, Context>,
    R: Send + 'static, {
       
    type OnSyncReqHandler = Coproduct<ValueLaneSync<Context, T>, Tail::OnSyncReqHandler>;

    fn on_sync_req(&'a mut self, lane: Text, id: Uuid) -> Self::OnSyncReqHandler {
        let HCons { head, tail } = self;
        let ValueLaneCommandTarget { name, projection, .. } = head;
        if lane == name {
            let cmd = ValueLaneSync::new(*projection, id);
            Coproduct::Inl(cmd)
        } else {
            Coproduct::Inr(tail.on_sync_req(lane, id))
        }
    }
}

impl<'a, Context, Lanes> OnReceive<'a, Context> for LaneDispatcher<Lanes>
where
    Lanes: OnCommandWithBuffer<'a, Context> + OnSyncRequest<'a, Context>,
{
    type OnCommandHandler = Lanes::OnCmdBufHandler;

    fn on_command(&'a mut self, lane: Text, body: Bytes) -> Self::OnCommandHandler {
        let LaneDispatcher { buffer, lanes } = self;
        lanes.on_command_with(buffer, lane, body)
    }

    type OnSyncHandler = Lanes::OnSyncReqHandler;

    fn on_sync(&'a mut self, lane: Text, id: Uuid) -> Self::OnSyncHandler {
        let LaneDispatcher { lanes, .. } = self;
        lanes.on_sync_req(lane, id)
    }
}

