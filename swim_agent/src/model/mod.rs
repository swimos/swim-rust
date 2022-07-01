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

use std::{
    collections::{HashMap, HashSet},
    marker::PhantomData,
    pin::Pin,
    task::{Context, Poll},
};

use bytes::{Bytes, BytesMut};
use futures::FutureExt;
use futures::SinkExt;
use futures::{
    future::{BoxFuture, Either},
    ready,
    stream::{FuturesUnordered, SelectAll},
    Stream, StreamExt,
};
use swim_api::{
    agent::{Agent, AgentConfig, AgentContext, AgentInitResult, UplinkKind},
    error::{AgentInitError, AgentTaskError, FrameIoError},
    protocol::{
        agent::{LaneRequest, LaneRequestDecoder},
        map::{MapMessage, MapMessageDecoder, RawMapOperationDecoder},
        WithLengthBytesCodec,
    },
};
use swim_model::Text;
use swim_utilities::{
    io::byte_channel::{ByteReader, ByteWriter},
    routing::uri::RelativeUri,
};
use tokio_util::codec::{BytesCodec, FramedRead, FramedWrite};
use uuid::Uuid;

use crate::{
    event_handler::{EventHandler, EventHandlerError, StepResult},
    lifecycle::AgentLifecycle,
    meta::AgentMetadata,
};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum WriteResult {
    NoData,
    Done,
    DataStillAvailable,
}

pub trait AgentLaneModel: Sized {
    type ValCommandHandler: EventHandler<Self, Completion = ()> + Send + 'static;
    type MapCommandHandler: EventHandler<Self, Completion = ()> + Send + 'static;
    type OnSyncHandler: EventHandler<Self, Completion = ()> + Send + 'static;

    fn make_instance() -> Self;

    fn value_like_lanes(&self) -> HashSet<&str>;
    fn map_like_lanes(&self) -> HashSet<&str>;
    fn lane_ids(&self) -> HashMap<u64, Text>;

    fn on_value_command(&self, lane: &str, body: Bytes) -> Option<Self::ValCommandHandler>;
    fn on_map_command(
        &self,
        lane: &str,
        body: MapMessage<Bytes, Bytes>,
    ) -> Option<Self::MapCommandHandler>;
    fn on_sync(&self, lane: &str, id: Uuid) -> Option<Self::OnSyncHandler>;

    fn write_event(&self, lane: &str, buffer: &mut BytesMut) -> Option<WriteResult>;
}

#[derive(Debug, Clone)]
pub struct AgentModel<LaneModel, Lifecycle> {
    _model_type: PhantomData<fn() -> LaneModel>,
    lifecycle: Lifecycle,
}

impl<LaneModel, Lifecycle> Agent for AgentModel<LaneModel, Lifecycle>
where
    LaneModel: AgentLaneModel + Clone + Send + 'static,
    Lifecycle: AgentLifecycle<LaneModel> + Clone + Send + Sync + 'static,
{
    fn run<'a>(
        &self,
        route: RelativeUri,
        config: AgentConfig,
        context: &'a dyn AgentContext,
    ) -> BoxFuture<'a, AgentInitResult<'a>> {
        self.clone()
            .initialize_agent(route, config, context)
            .boxed()
    }
}

type ValueLaneReader = FramedRead<ByteReader, LaneRequestDecoder<WithLengthBytesCodec>>;
type MapLaneReader =
    FramedRead<ByteReader, LaneRequestDecoder<MapMessageDecoder<RawMapOperationDecoder>>>;

struct LaneWriter {
    id: u64,
    writer: FramedWrite<ByteWriter, BytesCodec>,
    buffer: BytesMut,
}

struct LaneReader {
    id: u64,
    inner: Either<ValueLaneReader, MapLaneReader>,
}

impl LaneReader {
    fn value(id: u64, reader: ByteReader) -> Self {
        LaneReader {
            id,
            inner: Either::Left(FramedRead::new(reader, Default::default())),
        }
    }

    fn map(id: u64, reader: ByteReader) -> Self {
        LaneReader {
            id,
            inner: Either::Right(FramedRead::new(reader, Default::default())),
        }
    }
}

impl LaneWriter {
    fn new(id: u64, tx: ByteWriter) -> Self {
        LaneWriter {
            id,
            writer: FramedWrite::new(tx, BytesCodec::default()),
            buffer: Default::default(),
        }
    }

    async fn write(mut self) -> (Self, Result<(), std::io::Error>) {
        let LaneWriter { writer, buffer, .. } = &mut self;
        let data = buffer.split().freeze();
        let result = writer.send(data).await;
        (self, result)
    }
}

impl Stream for LaneReader {
    type Item = (
        u64,
        Result<Either<LaneRequest<Bytes>, LaneRequest<MapMessage<Bytes, Bytes>>>, FrameIoError>,
    );

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let LaneReader { id, inner } = self.get_mut();
        match inner {
            Either::Left(reader) => {
                let result = ready!(reader.poll_next_unpin(cx));
                Poll::Ready(result.map(|r| (*id, r.map(Either::Left))))
            }
            Either::Right(reader) => {
                let result = ready!(reader.poll_next_unpin(cx));
                Poll::Ready(result.map(|r| (*id, r.map(Either::Right))))
            }
        }
    }
}

enum TaskEvent {
    WriteComplete {
        writer: LaneWriter,
        result: Result<(), std::io::Error>,
    },
    ValueRequest {
        id: u64,
        request: LaneRequest<Bytes>,
    },
    MapRequest {
        id: u64,
        request: LaneRequest<MapMessage<Bytes, Bytes>>,
    },
    RequestError {
        id: u64,
        error: FrameIoError,
    },
}

impl<LaneModel, Lifecycle> AgentModel<LaneModel, Lifecycle>
where
    LaneModel: AgentLaneModel + Send + 'static,
    Lifecycle: AgentLifecycle<LaneModel> + 'static,
{
    async fn initialize_agent<'a>(
        self,
        route: RelativeUri,
        config: AgentConfig,
        context: &'a dyn AgentContext,
    ) -> AgentInitResult<'a>
    where
        LaneModel: AgentLaneModel,
        Lifecycle: AgentLifecycle<LaneModel>,
    {
        let AgentModel { lifecycle, .. } = &self;

        let lane_model = LaneModel::make_instance();
        let meta = AgentMetadata::new(&route, &config);

        let mut value_lane_io = HashMap::new();
        let mut map_lane_io = HashMap::new();

        let val_lane_names = lane_model.value_like_lanes();
        let map_lane_names = lane_model.map_like_lanes();
        let lane_ids = lane_model.lane_ids();

        for name in val_lane_names {
            let io = context.add_lane(name, UplinkKind::Value, None).await?;
            value_lane_io.insert(Text::new(name), io);
        }
        for name in map_lane_names {
            if value_lane_io.contains_key(name) {
                return Err(AgentInitError::DuplicateLane(Text::new(name)));
            }
            let io = context.add_lane(name, UplinkKind::Map, None).await?;
            map_lane_io.insert(Text::new(name), io);
        }

        let on_start_handler = lifecycle.on_start();
        if let Err(e) = run_handler(
            meta,
            &lane_model,
            lifecycle,
            on_start_handler,
            &lane_ids,
            &mut Discard,
        ) {
            return Err(AgentInitError::UserCodeError(Box::new(e)));
        }
        Ok(self
            .run_agent(
                route,
                config,
                lane_model,
                lane_ids,
                value_lane_io,
                map_lane_io,
            )
            .boxed())
    }

    async fn run_agent(
        self,
        route: RelativeUri,
        config: AgentConfig,
        lane_model: LaneModel,
        lane_ids: HashMap<u64, Text>,
        value_lane_io: HashMap<Text, (ByteWriter, ByteReader)>,
        map_lane_io: HashMap<Text, (ByteWriter, ByteReader)>,
    ) -> Result<(), AgentTaskError> {
        let AgentModel { lifecycle, .. } = self;
        let meta = AgentMetadata::new(&route, &config);

        let mut lane_ids_rev = HashMap::new();
        for (id, name) in &lane_ids {
            lane_ids_rev.insert(name.clone(), *id);
        }

        let mut lane_readers = SelectAll::new();
        let mut lane_writers = HashMap::new();
        let mut pending_writes = FuturesUnordered::new();

        for (name, (tx, rx)) in value_lane_io {
            let id = lane_ids_rev[&name];
            lane_readers.push(LaneReader::value(id, rx));
            lane_writers.insert(id, LaneWriter::new(id, tx));
        }

        for (name, (tx, rx)) in map_lane_io {
            let id = lane_ids_rev[&name];
            lane_readers.push(LaneReader::map(id, rx));
            lane_writers.insert(id, LaneWriter::new(id, tx));
        }

        let mut dirty_lanes: HashSet<u64> = HashSet::new();

        loop {
            let task_event: TaskEvent = tokio::select! {
                biased;
                write_done = pending_writes.next(), if !pending_writes.is_empty() => {
                    if let Some((writer, result)) = write_done {
                        TaskEvent::WriteComplete {
                            writer, result
                        }
                    } else {
                        continue;
                    }
                }
                maybe_req = lane_readers.next() => {
                    match maybe_req {
                        Some((id, Ok(Either::Left(request)))) => TaskEvent::ValueRequest{
                            id, request
                        },
                        Some((id, Ok(Either::Right(request)))) => TaskEvent::MapRequest{
                            id, request
                        },
                        Some((id, Err(error))) => TaskEvent::RequestError {
                            id, error
                        },
                        _ => {
                            break Ok(());
                        }
                    }
                }
            };
            match task_event {
                TaskEvent::WriteComplete { writer, result } => {
                    if result.is_err() {
                        break Ok(()); //Failing to write indicates that the runtime has stopped so we can exit without an error.
                    }
                    lane_writers.insert(writer.id, writer);
                }
                TaskEvent::ValueRequest { id, request } => {
                    let name = &lane_ids[&id];
                    match request {
                        LaneRequest::Command(body) => {
                            if let Some(handler) = lane_model.on_value_command(name.as_str(), body)
                            {
                                if let Err(e) = run_handler(
                                    meta,
                                    &lane_model,
                                    &lifecycle,
                                    handler,
                                    &lane_ids,
                                    &mut dirty_lanes,
                                ) {
                                    break Err(AgentTaskError::UserCodeError(Box::new(e)));
                                }
                            }
                        }
                        LaneRequest::Sync(remote_id) => {
                            if let Some(handler) = lane_model.on_sync(name.as_str(), remote_id) {
                                if let Err(e) = run_handler(
                                    meta,
                                    &lane_model,
                                    &lifecycle,
                                    handler,
                                    &lane_ids,
                                    &mut dirty_lanes,
                                ) {
                                    break Err(AgentTaskError::UserCodeError(Box::new(e)));
                                }
                            }
                        }
                    }
                }
                TaskEvent::MapRequest { id, request } => {
                    let name = &lane_ids[&id];
                    match request {
                        LaneRequest::Command(body) => {
                            if let Some(handler) = lane_model.on_map_command(name.as_str(), body) {
                                if let Err(e) = run_handler(
                                    meta,
                                    &lane_model,
                                    &lifecycle,
                                    handler,
                                    &lane_ids,
                                    &mut dirty_lanes,
                                ) {
                                    break Err(AgentTaskError::UserCodeError(Box::new(e)));
                                }
                            }
                        }
                        LaneRequest::Sync(remote_id) => {
                            if let Some(handler) = lane_model.on_sync(name.as_str(), remote_id) {
                                if let Err(e) = run_handler(
                                    meta,
                                    &lane_model,
                                    &lifecycle,
                                    handler,
                                    &lane_ids,
                                    &mut dirty_lanes,
                                ) {
                                    break Err(AgentTaskError::UserCodeError(Box::new(e)));
                                }
                            }
                        }
                    }
                }
                TaskEvent::RequestError { id, error } => {
                    let lane = lane_ids[&id].clone();
                    break Err(AgentTaskError::BadFrame { lane, error });
                }
            }
            dirty_lanes.retain(|id| {
                if let Some(mut tx) = lane_writers.remove(id) {
                    let name = &lane_ids[id];
                    match lane_model.write_event(name.as_str(), &mut tx.buffer) {
                        Some(WriteResult::Done) => {
                            pending_writes.push(tx.write());
                            false
                        }
                        Some(WriteResult::DataStillAvailable) => {
                            pending_writes.push(tx.write());
                            true
                        }
                        _ => false,
                    }
                } else {
                    true
                }
            });
        }
    }
}

trait IdCollector {
    fn add_id(&mut self, id: u64);
}

struct Discard;

impl IdCollector for Discard {
    fn add_id(&mut self, _id: u64) {}
}

impl IdCollector for HashSet<u64> {
    fn add_id(&mut self, id: u64) {
        self.insert(id);
    }
}

fn run_handler<Context, Lifecycle, Handler, Collector>(
    meta: AgentMetadata,
    context: &Context,
    lifecycle: &Lifecycle,
    mut handler: Handler,
    lanes: &HashMap<u64, Text>,
    collector: &mut Collector,
) -> Result<(), EventHandlerError>
where
    Lifecycle: AgentLifecycle<Context>,
    Handler: EventHandler<Context, Completion = ()>,
    Collector: IdCollector,
{
    loop {
        match handler.step(meta, context) {
            StepResult::Continue { modified_lane } => {
                if let Some((id, lane)) =
                    modified_lane.and_then(|id| lanes.get(&id).map(|name| (id, name)))
                {
                    if let Some(consequence) = lifecycle.lane_event(context, lane.as_str()) {
                        collector.add_id(id);
                        run_handler(meta, context, lifecycle, consequence, lanes, collector)?;
                    }
                }
            }
            StepResult::Fail(err) => {
                break Err(err);
            }
            StepResult::Complete { modified_lane, .. } => {
                if let Some((id, lane)) =
                    modified_lane.and_then(|id| lanes.get(&id).map(|name| (id, name)))
                {
                    if let Some(consequence) = lifecycle.lane_event(context, lane.as_str()) {
                        collector.add_id(id);
                        run_handler(meta, context, lifecycle, consequence, lanes, collector)?;
                    }
                    break Ok(());
                }
            }
        }
    }
}