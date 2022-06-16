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

pub mod lifecycle;

use std::{cell::{RefCell, Cell}, collections::VecDeque};

use bytes::BytesMut;
use swim_api::protocol::agent::{ValueLaneResponseEncoder, ValueLaneResponse};
use swim_form::structural::{read::{recognizer::Recognizer, ReadError}, write::StructuralWritable};
use swim_recon::parser::{RecognizerDecoder, AsyncParseError, ParseError};
use tokio_util::codec::{Decoder, Encoder};
use uuid::Uuid;

use crate::{event_handler::{EventHandler, StepResult, EventHandlerError}, model::WriteResult, meta::AgentMetadata};

#[derive(Debug)]
pub struct ValueLane<T> {
    id: u64,
    content: RefCell<T>,
    dirty: Cell<bool>,
    sync_queue: RefCell<VecDeque<Uuid>>,
}

impl<T> ValueLane<T> {

    pub fn new(id: u64, init: T) -> Self {
        ValueLane { 
            id, 
            content: RefCell::new(init), 
            dirty: Cell::new(false),
            sync_queue: Default::default(),
        }
    }

    pub fn read<F, R>(&self, f: F) -> R
    where
        F: FnOnce(&T) -> R,
    {
        let ValueLane { content, ..} = self;
        let value = content.borrow();
        f(&*value)
    }

    pub fn write(&self, value: T) {
        let ValueLane { content, dirty, .. } = self;
        content.replace(value);
        dirty.replace(true);
    }

    pub fn sync(&self, id: Uuid) {
        let ValueLane { sync_queue, .. } = self;
        sync_queue.borrow_mut().push_back(id);
    }

}

const INFALLIBLE_SER: &str = "Serializing to recon should be infallible.";

impl<T: StructuralWritable> ValueLane<T> {

    pub fn write_to_buffer(&self, buffer: &mut BytesMut) -> WriteResult {
        let ValueLane { content, dirty, sync_queue, .. } = self;
        let mut encoder = ValueLaneResponseEncoder;
        let mut sync = sync_queue.borrow_mut();
        if let Some(id) = sync.pop_front() {
            let value_guard = content.borrow();
            let response = ValueLaneResponse::synced(id, &*value_guard);
            encoder.encode(response, buffer).expect(INFALLIBLE_SER);
            if dirty.get() || !sync.is_empty() {
                WriteResult::LaneStillDirty
            } else {
                WriteResult::LaneNowClean
            }
        } else if dirty.get() {
            let value_guard = content.borrow();
            let response = ValueLaneResponse::event(&*value_guard);
            encoder.encode(response, buffer).expect(INFALLIBLE_SER);
            dirty.set(false);
            WriteResult::LaneNowClean
        } else {
            WriteResult::NoData
        }
    }

}

pub struct ValueLaneSet<C, T> {
    projection: for<'a> fn(&'a C) -> &'a ValueLane<T>,
    value: Option<T>,
}

pub struct ValueLaneSync<C, T> {
    projection: for<'b> fn(&'b C) -> &'b ValueLane<T>,
    id: Option<Uuid>,
}


impl<C, T> ValueLaneSet<C, T> {

    fn new(projection: for<'a> fn(&'a C) -> &'a ValueLane<T>,
    value: T) -> Self {
        ValueLaneSet { projection, value: Some(value) }
    }

}

impl<C, T> ValueLaneSync<C, T> {

    pub fn new(projection: for<'a> fn(&'a C) -> &'a ValueLane<T>, id: Uuid) -> Self {
        ValueLaneSync { projection, id: Some(id) }
    }

}

impl<C, T> EventHandler<C> for ValueLaneSet<C, T> {
    type Completion = ();

    fn step(&mut self, _meta: AgentMetadata, context: &C) -> StepResult<Self::Completion> {
        let ValueLaneSet { projection, value } = self;
        if let Some(value) = value.take() {
            let lane = projection(context);
            lane.write(value);
            StepResult::Complete { modified_lane: Some(lane.id), result: () }
        } else {
            StepResult::Fail(EventHandlerError::SteppedAfterComplete)
        }
    }
}

impl<C, T> EventHandler<C> for ValueLaneSync<C, T> {
    type Completion = ();

    fn step(&mut self, _meta: AgentMetadata, context: &C) -> StepResult<Self::Completion> {
        let ValueLaneSync { projection, id } = self;
        if let Some(id) = id.take() {
            let lane = projection(context);
            lane.sync(id);
            StepResult::Complete { modified_lane: Some(lane.id), result: () }
        } else {
            StepResult::Fail(EventHandlerError::SteppedAfterComplete)
        }
    }
}

//TODO Add terminator.
pub struct ValueLaneCommand<'a, C, T, R> {
    projection: for<'b> fn(&'b C) -> &'b ValueLane<T>,
    decoder: &'a mut RecognizerDecoder<R>,
    buffer: &'a mut BytesMut,
}

impl<'a, C, T, R> ValueLaneCommand<'a, C, T, R> {

    pub fn new(projection: for<'b> fn(&'b C) -> &'b ValueLane<T>,
    decoder: &'a mut RecognizerDecoder<R>,
    buffer: &'a mut BytesMut) -> Self {
        ValueLaneCommand { projection, decoder, buffer }
    }

}

impl<'a, C, T, R> EventHandler<C> for ValueLaneCommand<'a, C, T, R>
where
    R: Recognizer<Target = T>,
{
    type Completion = ();

    fn step(&mut self, meta: AgentMetadata, context: &C) -> StepResult<Self::Completion> {
        let ValueLaneCommand { projection, buffer, decoder } = self;
        match decoder.decode_eof(buffer) {
            Ok(Some(value)) => {
                let mut setter = ValueLaneSet::new(*projection, value);
                setter.step(meta, context)
            },
            Err(e) => StepResult::Fail(EventHandlerError::BadCommand(e)),
            _ => StepResult::Fail(EventHandlerError::BadCommand(AsyncParseError::Parser(ParseError::Structure(ReadError::IncompleteRecord)))),
        }
    }
}
