// Copyright 2015-2024 Swim Inc.
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
    cell::{Cell, RefCell},
    collections::HashMap,
    num::NonZeroUsize,
};

use bytes::BytesMut;
use futures::{
    future::BoxFuture,
    stream::{unfold, BoxStream, FuturesUnordered},
    FutureExt, SinkExt, Stream, StreamExt,
};
use swimos_agent::{
    agent_model::downlink::{BoxDownlinkChannel, BoxDownlinkChannelFactory, DownlinkChannelEvent},
    event_handler::{
        ActionContext, BoxHandlerAction, DownlinkSpawnOnDone, EventHandler, EventHandlerError,
        HandlerFuture, LaneSpawnOnDone, LaneSpawner, LinkSpawner, Spawner, StepResult,
    },
};
use swimos_agent_protocol::{
    encoding::downlink::DownlinkNotificationEncoder, DownlinkNotification,
};
use swimos_api::{
    address::Address,
    agent::WarpLaneKind,
    error::{CommanderRegistrationError, DynamicRegistrationError},
};
use swimos_form::write::StructuralWritable;
use swimos_model::Text;
use swimos_recon::print_recon_compact;
use swimos_utilities::{
    byte_channel::{byte_channel, ByteWriter},
    non_zero_usize,
};
use tokio::time::Instant;
use tokio_util::codec::FramedWrite;

use crate::{
    test_support::{make_meta, make_uri},
    ConnectorAgent,
};

#[derive(Default)]
pub struct TestSpawner {
    futures: FuturesUnordered<HandlerFuture<ConnectorAgent>>,
    downlinks: RefCell<Vec<DownlinkRecord>>,
    timers: RefCell<Vec<TimerRecord>>,
    lanes: RefCell<Vec<LaneRecord>>,
    lane_id: Cell<u64>,
}

impl TestSpawner {
    fn take_requests(&self) -> RequestsRecord {
        let mut guard = self.downlinks.borrow_mut();
        let downlinks = std::mem::take(&mut *guard);
        let mut guard = self.timers.borrow_mut();
        let timers = std::mem::take(&mut *guard);
        let mut guard = self.lanes.borrow_mut();
        let lanes = std::mem::take(&mut *guard);
        RequestsRecord {
            downlinks,
            timers,
            lanes,
        }
    }

    fn lanes_on_done(&self) -> Vec<BoxHandlerAction<'static, ConnectorAgent, ()>> {
        let mut guard = self.lanes.borrow_mut();
        let mut handlers = vec![];
        for LaneRecord { on_done, .. } in guard.iter_mut() {
            let n = self.lane_id.get();
            self.lane_id.set(n + 1);
            if let Some(on_done) = on_done.take() {
                handlers.push(on_done(Ok(n)));
            }
        }
        handlers
    }
}

impl Spawner<ConnectorAgent> for TestSpawner {
    fn spawn_suspend(&self, fut: HandlerFuture<ConnectorAgent>) {
        self.futures.push(fut);
    }

    fn schedule_timer(&self, at: Instant, id: u64) {
        self.timers.borrow_mut().push(TimerRecord { _at: at, id });
    }
}

pub struct DownlinkRecord {
    pub path: Address<Text>,
    pub make_channel: BoxDownlinkChannelFactory<ConnectorAgent>,
    pub _on_done: DownlinkSpawnOnDone<ConnectorAgent>,
}

impl std::fmt::Debug for DownlinkRecord {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("DownlinkRecord")
            .field("path", &self.path)
            .field("make_channel", &"...")
            .field("on_done", &"...")
            .finish()
    }
}

#[derive(Debug)]
pub struct TimerRecord {
    pub _at: Instant,
    pub id: u64,
}

pub struct LaneRecord {
    pub name: String,
    pub kind: WarpLaneKind,
    pub on_done: Option<LaneSpawnOnDone<ConnectorAgent>>,
}

impl std::fmt::Debug for LaneRecord {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("LaneRecord")
            .field("name", &self.name)
            .field("kind", &self.kind)
            .field("on_done", &"...")
            .finish()
    }
}

#[derive(Debug)]
pub struct RequestsRecord {
    pub downlinks: Vec<DownlinkRecord>,
    pub timers: Vec<TimerRecord>,
    pub lanes: Vec<LaneRecord>,
}

impl RequestsRecord {
    pub fn is_empty(&self) -> bool {
        self.downlinks.is_empty() && self.timers.is_empty() && self.lanes.is_empty()
    }
}

impl LinkSpawner<ConnectorAgent> for TestSpawner {
    fn spawn_downlink(
        &self,
        path: Address<Text>,
        make_channel: BoxDownlinkChannelFactory<ConnectorAgent>,
        on_done: DownlinkSpawnOnDone<ConnectorAgent>,
    ) {
        self.downlinks.borrow_mut().push(DownlinkRecord {
            path,
            make_channel,
            _on_done: on_done,
        })
    }

    fn register_commander(&self, _path: Address<Text>) -> Result<u16, CommanderRegistrationError> {
        panic!("Registering commanders not supported.");
    }
}

impl LaneSpawner<ConnectorAgent> for TestSpawner {
    fn spawn_warp_lane(
        &self,
        name: &str,
        kind: WarpLaneKind,
        on_done: LaneSpawnOnDone<ConnectorAgent>,
    ) -> Result<(), DynamicRegistrationError> {
        self.lanes.borrow_mut().push(LaneRecord {
            name: name.to_string(),
            kind,
            on_done: Some(on_done),
        });
        Ok(())
    }
}

pub async fn run_handle_with_futs<H>(
    agent: &ConnectorAgent,
    handler: H,
) -> Result<RequestsRecord, Box<dyn std::error::Error + Send>>
where
    H: EventHandler<ConnectorAgent>,
{
    let mut spawner = TestSpawner::default();
    run_handler(&spawner, agent, handler)?;
    while !spawner.futures.is_empty() {
        match spawner.futures.next().await {
            Some(h) => {
                run_handler(&spawner, agent, h)?;
            }
            None => break,
        }
    }
    Ok(spawner.take_requests())
}

pub async fn run_handle_with_futs_and_lanes<H>(
    agent: &ConnectorAgent,
    handler: H,
) -> Result<RequestsRecord, Box<dyn std::error::Error + Send>>
where
    H: EventHandler<ConnectorAgent>,
{
    let mut spawner = TestSpawner::default();
    run_handler(&spawner, agent, handler)?;
    let mut lane_handlers = spawner.lanes_on_done();
    while !(spawner.futures.is_empty() && lane_handlers.is_empty()) {
        if let Some(h) = lane_handlers.pop() {
            run_handler(&spawner, agent, h)?;
        } else {
            match spawner.futures.next().await {
                Some(h) => {
                    run_handler(&spawner, agent, h)?;
                }
                None => break,
            }
        }
        lane_handlers.extend(spawner.lanes_on_done());
    }
    Ok(spawner.take_requests())
}

pub fn run_handler<H>(
    spawner: &TestSpawner,
    agent: &ConnectorAgent,
    mut handler: H,
) -> Result<(), Box<dyn std::error::Error + Send>>
where
    H: EventHandler<ConnectorAgent>,
{
    let uri = make_uri();
    let route_params = HashMap::new();
    let meta = make_meta(&uri, &route_params);

    let mut join_lane_init = HashMap::new();
    let mut command_buffer = BytesMut::new();

    let mut action_context = ActionContext::new(
        spawner,
        spawner,
        spawner,
        &mut join_lane_init,
        &mut command_buffer,
    );

    loop {
        match handler.step(&mut action_context, meta, agent) {
            StepResult::Continue { .. } => {}
            StepResult::Fail(EventHandlerError::EffectError(err)) => return Err(err),
            StepResult::Fail(err) => panic!("{:?}", err),
            StepResult::Complete { .. } => {
                break;
            }
        }
    }
    Ok(())
}

const BUFFER_SIZE: NonZeroUsize = non_zero_usize!(4096);

pub fn drive_downlink<'a, T>(
    factory: BoxDownlinkChannelFactory<ConnectorAgent>,
    agent: &'a ConnectorAgent,
    input: BoxStream<'static, T>,
) -> impl Stream<Item = RequestsRecord> + 'a
where
    T: StructuralWritable + Send + 'static,
{
    let (in_tx, in_rx) = byte_channel(BUFFER_SIZE);
    let (out_tx, _out_rx) = byte_channel(BUFFER_SIZE);
    let channel = factory.create_box(agent, out_tx, in_rx);

    let provider = downlink_provider(in_tx, input).boxed();
    let pump = DownlinkPump::new(provider, agent, channel);
    unfold(pump, |pump| pump.consume())
}

async fn downlink_provider<T>(in_tx: ByteWriter, mut data: BoxStream<'static, T>)
where
    T: StructuralWritable + 'static,
{
    let mut writer = FramedWrite::new(in_tx, DownlinkNotificationEncoder);
    writer
        .send(DownlinkNotification::<&[u8]>::Linked)
        .await
        .expect("Send failed.");
    writer
        .send(DownlinkNotification::<&[u8]>::Synced)
        .await
        .expect("Send failed.");
    while let Some(value) = data.next().await {
        let content = format!("{}", print_recon_compact(&value));
        let bytes = content.as_bytes();
        writer
            .send(DownlinkNotification::Event { body: bytes })
            .await
            .expect("Send failed.");
    }
    writer
        .send(DownlinkNotification::<&[u8]>::Unlinked)
        .await
        .expect("Send failed.");
}

struct DownlinkPump<'a> {
    provider: Option<BoxFuture<'static, ()>>,
    agent: &'a ConnectorAgent,
    channel: BoxDownlinkChannel<ConnectorAgent>,
}

impl<'a> DownlinkPump<'a> {
    fn new(
        provider: BoxFuture<'static, ()>,
        agent: &'a ConnectorAgent,
        channel: BoxDownlinkChannel<ConnectorAgent>,
    ) -> Self {
        DownlinkPump {
            provider: Some(provider),
            agent,
            channel,
        }
    }

    async fn consume(mut self) -> Option<(RequestsRecord, Self)> {
        let DownlinkPump {
            provider,
            agent,
            channel,
        } = &mut self;
        let requests = loop {
            let result = if let Some(task) = provider.as_mut() {
                tokio::select! {
                    biased;
                    result = channel.await_ready() => result,
                    _ = task => {
                        *provider = None;
                        continue;
                    },
                }
            } else {
                channel.await_ready().await
            };
            match result {
                Some(Ok(ev)) => {
                    if ev == DownlinkChannelEvent::HandlerReady {
                        if let Some(handler) = channel.next_event(agent) {
                            let record = run_handle_with_futs(agent, handler)
                                .await
                                .expect("Handler failed.");
                            break Some(record);
                        }
                    }
                }
                Some(Err(e)) => panic!("Downlink failed: {}", e),
                None => break None,
            };
        };
        requests.map(|req| (req, self))
    }
}
