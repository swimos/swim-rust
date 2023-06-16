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

use std::{
    cell::RefCell,
    collections::HashMap,
    num::NonZeroUsize,
    ops::Deref,
    pin::{pin, Pin},
    sync::{
        atomic::{AtomicBool, Ordering},
        Arc,
    },
    task::{Context, Poll, Waker},
};

use bytes::BytesMut;
use futures::{
    future::join3,
    task::{waker, ArcWake},
    Sink, SinkExt, Stream, StreamExt,
};
use parking_lot::Mutex;
use swim_api::protocol::{
    downlink::{DownlinkNotification, DownlinkNotificationEncoder},
    map::{MapMessage, MapMessageEncoder, MapOperation, MapOperationDecoder, MapOperationEncoder},
};
use swim_model::{address::Address, Text};
use swim_utilities::{
    io::byte_channel::{self, ByteReader, ByteWriter},
    non_zero_usize, trigger,
};
use tokio::{io::AsyncWriteExt, sync::mpsc};
use tokio_util::codec::{Encoder, FramedRead, FramedWrite};

use crate::{
    agent_model::downlink::{
        handlers::{BoxDownlinkChannel, DownlinkChannelEvent},
        MapDownlinkHandle,
    },
    config::MapDownlinkConfig,
    downlink_lifecycle::{
        map::{
            on_clear::OnDownlinkClear, on_remove::OnDownlinkRemove, on_update::OnDownlinkUpdate,
        },
        on_failed::OnFailed,
        on_linked::OnLinked,
        on_synced::OnSynced,
        on_unlinked::OnUnlinked,
    },
    event_handler::{BoxEventHandler, HandlerActionExt, SideEffect},
};

use super::{HostedMapDownlinkFactory, MapDlState, MapWriteStream};

struct FakeAgent;

#[derive(Debug, PartialEq, Eq)]
enum Event {
    Linked,
    Synced(HashMap<i32, Text>),
    Updated(i32, Text, Option<Text>, HashMap<i32, Text>),
    Removed(i32, Text, HashMap<i32, Text>),
    Cleared(HashMap<i32, Text>),
    Unlinked,
    Failed,
}

impl Event {
    fn synced<'a, I>(it: I) -> Self
    where
        I: IntoIterator<Item = (i32, &'a str)>,
    {
        Event::Synced(it.into_iter().map(|(k, v)| (k, Text::new(v))).collect())
    }

    fn cleared<'a, I>(it: I) -> Self
    where
        I: IntoIterator<Item = (i32, &'a str)>,
    {
        Event::Cleared(it.into_iter().map(|(k, v)| (k, Text::new(v))).collect())
    }

    fn updated<'a, I>(key: i32, new_value: &'a str, prev: Option<&'a str>, it: I) -> Self
    where
        I: IntoIterator<Item = (i32, &'a str)>,
    {
        Event::Updated(
            key,
            Text::new(new_value),
            prev.map(Text::new),
            it.into_iter().map(|(k, v)| (k, Text::new(v))).collect(),
        )
    }

    fn removed<'a, I>(key: i32, prev: &'a str, it: I) -> Self
    where
        I: IntoIterator<Item = (i32, &'a str)>,
    {
        Event::Removed(
            key,
            Text::new(prev),
            it.into_iter().map(|(k, v)| (k, Text::new(v))).collect(),
        )
    }
}

type Events = Arc<Mutex<Vec<Event>>>;

#[derive(Default)]
struct FakeLifecycle {
    events: Events,
}

impl OnLinked<FakeAgent> for FakeLifecycle {
    type OnLinkedHandler<'a> = BoxEventHandler<'a, FakeAgent>
    where
        Self: 'a;

    fn on_linked(&self) -> Self::OnLinkedHandler<'_> {
        SideEffect::from(move || {
            self.events.lock().push(Event::Linked);
        })
        .boxed()
    }
}

impl OnUnlinked<FakeAgent> for FakeLifecycle {
    type OnUnlinkedHandler<'a> = BoxEventHandler<'a, FakeAgent>
    where
        Self: 'a;

    fn on_unlinked(&self) -> Self::OnUnlinkedHandler<'_> {
        SideEffect::from(move || {
            self.events.lock().push(Event::Unlinked);
        })
        .boxed()
    }
}

impl OnFailed<FakeAgent> for FakeLifecycle {
    type OnFailedHandler<'a> = BoxEventHandler<'a, FakeAgent>
    where
        Self: 'a;

    fn on_failed(&self) -> Self::OnFailedHandler<'_> {
        SideEffect::from(move || {
            self.events.lock().push(Event::Failed);
        })
        .boxed()
    }
}

impl OnSynced<HashMap<i32, Text>, FakeAgent> for FakeLifecycle {
    type OnSyncedHandler<'a> = BoxEventHandler<'a, FakeAgent>
    where
        Self: 'a;

    fn on_synced<'a>(&'a self, value: &HashMap<i32, Text>) -> Self::OnSyncedHandler<'a> {
        let map = value.clone();
        SideEffect::from(move || {
            self.events.lock().push(Event::Synced(map));
        })
        .boxed()
    }
}

impl OnDownlinkUpdate<i32, Text, FakeAgent> for FakeLifecycle {
    type OnUpdateHandler<'a> = BoxEventHandler<'a, FakeAgent>
    where
        Self: 'a;

    fn on_update<'a>(
        &'a self,
        key: i32,
        map: &HashMap<i32, Text>,
        previous: Option<Text>,
        new_value: &Text,
    ) -> Self::OnUpdateHandler<'a> {
        let map = map.clone();
        let new_value = new_value.clone();
        SideEffect::from(move || {
            self.events
                .lock()
                .push(Event::Updated(key, new_value, previous, map));
        })
        .boxed()
    }
}

impl OnDownlinkRemove<i32, Text, FakeAgent> for FakeLifecycle {
    type OnRemoveHandler<'a> = BoxEventHandler<'a, FakeAgent>
    where
        Self: 'a;

    fn on_remove<'a>(
        &'a self,
        key: i32,
        map: &HashMap<i32, Text>,
        removed: Text,
    ) -> Self::OnRemoveHandler<'a> {
        let map = map.clone();
        SideEffect::from(move || {
            self.events.lock().push(Event::Removed(key, removed, map));
        })
        .boxed()
    }
}

impl OnDownlinkClear<i32, Text, FakeAgent> for FakeLifecycle {
    type OnClearHandler<'a> = BoxEventHandler<'a, FakeAgent>
    where
        Self: 'a;

    fn on_clear(&self, map: HashMap<i32, Text>) -> Self::OnClearHandler<'_> {
        SideEffect::from(move || {
            self.events.lock().push(Event::Cleared(map));
        })
        .boxed()
    }
}

type State = RefCell<MapDlState<i32, Text>>;

const BUFFER_SIZE: NonZeroUsize = non_zero_usize!(4096);

type MsgEncoder = MapMessageEncoder<MapOperationEncoder>;

struct Writer {
    sender: FramedWrite<ByteWriter, DownlinkNotificationEncoder>,
    encoder: MsgEncoder,
    buffer: BytesMut,
}

impl Writer {
    fn new(writer: ByteWriter) -> Self {
        Writer {
            sender: FramedWrite::new(writer, Default::default()),
            encoder: Default::default(),
            buffer: Default::default(),
        }
    }

    async fn send(
        &mut self,
        not: DownlinkNotification<MapMessage<i32, Text>>,
    ) -> Result<(), std::io::Error> {
        let Writer {
            sender,
            encoder,
            buffer,
        } = self;
        let bytes = match not {
            DownlinkNotification::Linked => DownlinkNotification::Linked,
            DownlinkNotification::Synced => DownlinkNotification::Synced,
            DownlinkNotification::Event { body } => {
                encoder.encode(body, buffer)?;
                DownlinkNotification::Event {
                    body: buffer.split().freeze(),
                }
            }
            DownlinkNotification::Unlinked => DownlinkNotification::Unlinked,
        };
        sender.send(bytes).await
    }
}

struct TestContext {
    channel: BoxDownlinkChannel<FakeAgent>,
    events: Events,
    sender: Option<Writer>,
    output_tx: Option<mpsc::UnboundedSender<MapOperation<i32, Text>>>,
    out_rx: ByteReader,
    stop_tx: Option<trigger::Sender>,
}

const NODE: &str = "/node";
const LANE: &str = "lane";

fn make_hosted_input(agent: &FakeAgent, config: MapDownlinkConfig) -> TestContext {
    let events: Events = Default::default();
    let lc = FakeLifecycle {
        events: events.clone(),
    };

    let (in_tx, in_rx) = byte_channel::byte_channel(BUFFER_SIZE);
    let (out_tx, out_rx) = byte_channel::byte_channel(BUFFER_SIZE);
    let (stop_tx, stop_rx) = trigger::trigger();

    let address = Address::text(None, NODE, LANE);

    let (write_tx, write_rx) = mpsc::unbounded_channel();

    let fac =
        HostedMapDownlinkFactory::new(address, lc, State::default(), config, stop_rx, write_rx);

    let chan = fac.create(agent, out_tx, in_rx);
    TestContext {
        channel: chan,
        events,
        output_tx: Some(write_tx),
        out_rx,
        sender: Some(Writer::new(in_tx)),
        stop_tx: Some(stop_tx),
    }
}

async fn clean_shutdown(context: &mut TestContext, agent: &FakeAgent, expect_unlinked: bool) {
    let TestContext {
        channel,
        events,
        stop_tx,
        ..
    } = context;

    if let Some(stop) = stop_tx.take() {
        stop.trigger();
    }

    if expect_unlinked {
        assert!(matches!(channel.await_ready().await, Some(Ok(_))));
        let next = channel.next_event(agent);
        let handler = next.expect("Expected handler.");
        run_handler(handler, agent);
        assert_eq!(take_events(events), vec![Event::Unlinked]);
    }

    assert!(matches!(channel.await_ready().await, None));
}

#[tokio::test]
async fn shutdown_when_input_stops() {
    let agent = FakeAgent;
    let mut context = make_hosted_input(&agent, MapDownlinkConfig::default());
    let TestContext {
        channel, sender, ..
    } = &mut context;

    assert!(channel.next_event(&agent).is_none());

    sender.take();

    let event = channel.await_ready().await;
    println!("{:?}", event);

    assert!(event.is_none());

    assert!(channel.next_event(&agent).is_none());
}

#[tokio::test]
async fn shutdown_on_stop_trigger() {
    let agent = FakeAgent;

    let mut context = make_hosted_input(&agent, MapDownlinkConfig::default());
    clean_shutdown(&mut context, &agent, false).await;
}

#[tokio::test]
async fn terminate_on_error() {
    let agent = FakeAgent;

    let mut context = make_hosted_input(&agent, MapDownlinkConfig::default());

    let TestContext {
        channel,
        sender,
        events,
        ..
    } = &mut context;

    assert!(sender
        .as_mut()
        .expect("Sender dropped.")
        .sender
        .get_mut()
        .write_u8(100)
        .await
        .is_ok()); //Invalid message kind tag.

    assert!(matches!(channel.await_ready().await, Some(Err(_))));
    let handler = channel
        .next_event(&agent)
        .expect("Expected failure response.");
    run_handler(handler, &agent);
    assert_eq!(take_events(events), vec![Event::Failed]);

    assert!(matches!(channel.await_ready().await, None));
}

fn take_events(events: &Events) -> Vec<Event> {
    std::mem::take(&mut *events.lock())
}

use super::super::test_support::run_handler;

enum Instruction {
    Incoming {
        notification: DownlinkNotification<MapMessage<i32, Text>>,
        expected: Option<Vec<Event>>,
    },
    Outgoing(MapOperation<i32, Text>),
    DropOutgoing,
}

fn incoming(
    notification: DownlinkNotification<MapMessage<i32, Text>>,
    expected: Option<Vec<Event>>,
) -> Instruction {
    Instruction::Incoming {
        notification,
        expected,
    }
}

async fn run_with_expectations(
    context: &mut TestContext,
    agent: &FakeAgent,
    instructions: Vec<Instruction>,
) {
    let TestContext {
        channel,
        events,
        sender,
        output_tx,
        out_rx: _out_rx,
        ..
    } = context;

    for instruction in instructions {
        match instruction {
            Instruction::Incoming {
                notification: not,
                expected,
            } => {
                assert!(sender
                    .as_mut()
                    .expect("Sender dropped.")
                    .send(not)
                    .await
                    .is_ok());
                assert!(matches!(channel.await_ready().await, Some(Ok(_))));
                let next = channel.next_event(agent);
                if let Some(expected) = expected {
                    let handler = next.expect("Expected handler.");
                    run_handler(handler, agent);

                    assert_eq!(take_events(events), expected);
                } else {
                    assert!(next.is_none());
                }
            }
            Instruction::Outgoing(op) => {
                output_tx
                    .as_ref()
                    .expect("Output dropped.")
                    .send(op)
                    .expect("Channel dropped");
                assert!(matches!(
                    channel.await_ready().await,
                    Some(Ok(DownlinkChannelEvent::WriteCompleted))
                ));
            }
            Instruction::DropOutgoing => {
                *output_tx = None;
                assert!(matches!(
                    channel.await_ready().await,
                    Some(Ok(DownlinkChannelEvent::WriteStreamTerminated))
                ));
            }
        }
    }
}

#[tokio::test]
async fn write_output() {
    let agent = FakeAgent;
    let mut context = make_hosted_input(&agent, MapDownlinkConfig::default());
    let op = MapOperation::Update {
        key: 5,
        value: Text::new("five"),
    };
    run_with_expectations(
        &mut context,
        &agent,
        vec![Instruction::Outgoing(op.clone())],
    )
    .await;

    clean_shutdown(&mut context, &agent, false).await;

    let TestContext { out_rx, .. } = &mut context;

    //Flush is not guaranteed until the next poll of channel after the write "completes"
    //so we only check that the value was written after the fact.
    let mut reader = FramedRead::new(out_rx, MapOperationDecoder::<i32, Text>::default());

    let output = reader.next().await;
    assert!(matches!(output, Some(Ok(o)) if o == op));
}

#[tokio::test]
async fn write_terminated() {
    let agent = FakeAgent;
    let mut context = make_hosted_input(&agent, MapDownlinkConfig::default());

    run_with_expectations(&mut context, &agent, vec![Instruction::DropOutgoing]).await;

    clean_shutdown(&mut context, &agent, false).await;
}

#[tokio::test]
async fn emit_linked_handler() {
    let agent = FakeAgent;

    let mut context = make_hosted_input(&agent, MapDownlinkConfig::default());

    run_with_expectations(
        &mut context,
        &agent,
        vec![incoming(
            DownlinkNotification::Linked,
            Some(vec![Event::Linked]),
        )],
    )
    .await;

    clean_shutdown(&mut context, &agent, true).await;
}

fn upd(key: i32, value: &str) -> DownlinkNotification<MapMessage<i32, Text>> {
    DownlinkNotification::Event {
        body: MapMessage::Update {
            key,
            value: Text::new(value),
        },
    }
}

fn rem(key: i32) -> DownlinkNotification<MapMessage<i32, Text>> {
    DownlinkNotification::Event {
        body: MapMessage::Remove { key },
    }
}

fn clr() -> DownlinkNotification<MapMessage<i32, Text>> {
    DownlinkNotification::Event {
        body: MapMessage::Clear,
    }
}

fn tke(n: u64) -> DownlinkNotification<MapMessage<i32, Text>> {
    DownlinkNotification::Event {
        body: MapMessage::Take(n),
    }
}

fn drp(n: u64) -> DownlinkNotification<MapMessage<i32, Text>> {
    DownlinkNotification::Event {
        body: MapMessage::Drop(n),
    }
}

#[tokio::test]
async fn emit_synced_handler() {
    let agent = FakeAgent;

    let mut context = make_hosted_input(&agent, MapDownlinkConfig::default());

    run_with_expectations(
        &mut context,
        &agent,
        vec![
            incoming(DownlinkNotification::Linked, Some(vec![Event::Linked])),
            incoming(upd(1, "a"), None),
            incoming(upd(2, "b"), None),
            incoming(upd(3, "c"), None),
            incoming(
                DownlinkNotification::Synced,
                Some(vec![Event::synced([(1, "a"), (2, "b"), (3, "c")])]),
            ),
        ],
    )
    .await;

    clean_shutdown(&mut context, &agent, true).await;
}

#[tokio::test]
async fn emit_event_handlers() {
    let agent = FakeAgent;
    let mut context = make_hosted_input(&agent, MapDownlinkConfig::default());

    run_with_expectations(
        &mut context,
        &agent,
        vec![
            incoming(DownlinkNotification::Linked, Some(vec![Event::Linked])),
            incoming(upd(1, "a"), None),
            incoming(
                DownlinkNotification::Synced,
                Some(vec![Event::synced([(1, "a")])]),
            ),
            incoming(
                upd(2, "b"),
                Some(vec![Event::updated(2, "b", None, [(1, "a"), (2, "b")])]),
            ),
            incoming(
                upd(1, "aa"),
                Some(vec![Event::updated(
                    1,
                    "aa",
                    Some("a"),
                    [(1, "aa"), (2, "b")],
                )]),
            ),
            incoming(rem(2), Some(vec![Event::removed(2, "b", [(1, "aa")])])),
            incoming(clr(), Some(vec![Event::cleared([(1, "aa")])])),
        ],
    )
    .await;

    clean_shutdown(&mut context, &agent, true).await;
}

#[tokio::test]
async fn emit_events_before_synced() {
    let config = MapDownlinkConfig {
        events_when_not_synced: true,
        ..Default::default()
    };
    let agent = FakeAgent;
    let mut context = make_hosted_input(&agent, config);

    run_with_expectations(
        &mut context,
        &agent,
        vec![
            incoming(DownlinkNotification::Linked, Some(vec![Event::Linked])),
            incoming(
                upd(1, "a"),
                Some(vec![Event::updated(1, "a", None, [(1, "a")])]),
            ),
            incoming(
                upd(2, "b"),
                Some(vec![Event::updated(2, "b", None, [(1, "a"), (2, "b")])]),
            ),
            incoming(
                DownlinkNotification::Synced,
                Some(vec![Event::synced([(1, "a"), (2, "b")])]),
            ),
        ],
    )
    .await;

    clean_shutdown(&mut context, &agent, true).await;
}

#[tokio::test]
async fn emit_unlinked_handler() {
    let agent = FakeAgent;
    let mut context = make_hosted_input(&agent, MapDownlinkConfig::default());

    run_with_expectations(
        &mut context,
        &agent,
        vec![
            incoming(DownlinkNotification::Linked, Some(vec![Event::Linked])),
            incoming(DownlinkNotification::Unlinked, Some(vec![Event::Unlinked])),
        ],
    )
    .await;

    clean_shutdown(&mut context, &agent, false).await;
}

#[tokio::test]
async fn emit_take_handlers() {
    let agent = FakeAgent;

    let mut context = make_hosted_input(&agent, MapDownlinkConfig::default());

    run_with_expectations(
        &mut context,
        &agent,
        vec![
            incoming(DownlinkNotification::Linked, Some(vec![Event::Linked])),
            incoming(upd(1, "a"), None),
            incoming(upd(2, "b"), None),
            incoming(upd(3, "c"), None),
            incoming(upd(4, "d"), None),
            incoming(upd(5, "e"), None),
            incoming(
                DownlinkNotification::Synced,
                Some(vec![Event::synced([
                    (1, "a"),
                    (2, "b"),
                    (3, "c"),
                    (4, "d"),
                    (5, "e"),
                ])]),
            ),
            incoming(
                tke(2),
                Some(vec![
                    Event::removed(3, "c", [(1, "a"), (2, "b"), (4, "d"), (5, "e")]),
                    Event::removed(4, "d", [(1, "a"), (2, "b"), (5, "e")]),
                    Event::removed(5, "e", [(1, "a"), (2, "b")]),
                ]),
            ),
        ],
    )
    .await;

    clean_shutdown(&mut context, &agent, true).await;
}

#[tokio::test]
async fn emit_drop_handlers() {
    let agent = FakeAgent;

    let mut context = make_hosted_input(&agent, MapDownlinkConfig::default());

    run_with_expectations(
        &mut context,
        &agent,
        vec![
            incoming(DownlinkNotification::Linked, Some(vec![Event::Linked])),
            incoming(upd(1, "a"), None),
            incoming(upd(2, "b"), None),
            incoming(upd(3, "c"), None),
            incoming(upd(4, "d"), None),
            incoming(upd(5, "e"), None),
            incoming(
                DownlinkNotification::Synced,
                Some(vec![Event::synced([
                    (1, "a"),
                    (2, "b"),
                    (3, "c"),
                    (4, "d"),
                    (5, "e"),
                ])]),
            ),
            incoming(
                drp(2),
                Some(vec![
                    Event::removed(1, "a", [(2, "b"), (3, "c"), (4, "d"), (5, "e")]),
                    Event::removed(2, "b", [(3, "c"), (4, "d"), (5, "e")]),
                ]),
            ),
        ],
    )
    .await;

    clean_shutdown(&mut context, &agent, true).await;
}

#[tokio::test]
async fn emit_drop_all_handlers() {
    let agent = FakeAgent;
    let mut context = make_hosted_input(&agent, MapDownlinkConfig::default());

    run_with_expectations(
        &mut context,
        &agent,
        vec![
            incoming(DownlinkNotification::Linked, Some(vec![Event::Linked])),
            incoming(upd(1, "a"), None),
            incoming(upd(2, "b"), None),
            incoming(upd(3, "c"), None),
            incoming(upd(4, "d"), None),
            incoming(upd(5, "e"), None),
            incoming(
                DownlinkNotification::Synced,
                Some(vec![Event::synced([
                    (1, "a"),
                    (2, "b"),
                    (3, "c"),
                    (4, "d"),
                    (5, "e"),
                ])]),
            ),
            incoming(
                drp(5),
                Some(vec![Event::cleared([
                    (1, "a"),
                    (2, "b"),
                    (3, "c"),
                    (4, "d"),
                    (5, "e"),
                ])]),
            ),
        ],
    )
    .await;

    clean_shutdown(&mut context, &agent, true).await;
}

#[tokio::test]
async fn revive_unlinked_downlink() {
    let config = MapDownlinkConfig {
        terminate_on_unlinked: false,
        ..Default::default()
    };

    let agent = FakeAgent;
    let mut context = make_hosted_input(&agent, config);

    run_with_expectations(
        &mut context,
        &agent,
        vec![
            incoming(DownlinkNotification::Linked, Some(vec![Event::Linked])),
            incoming(upd(1, "a"), None),
            incoming(
                DownlinkNotification::Synced,
                Some(vec![Event::synced([(1, "a")])]),
            ),
            incoming(DownlinkNotification::Unlinked, Some(vec![Event::Unlinked])),
            incoming(DownlinkNotification::Linked, Some(vec![Event::Linked])),
            incoming(upd(2, "b"), None),
            incoming(
                DownlinkNotification::Synced,
                Some(vec![Event::synced([(2, "b")])]),
            ),
        ],
    )
    .await;

    clean_shutdown(&mut context, &agent, true).await;
}

#[tokio::test]
async fn map_downlink_writer() {
    let (op_tx, op_rx) = mpsc::unbounded_channel::<MapOperation<i32, Text>>();
    let (tx, rx) = byte_channel::byte_channel(BUFFER_SIZE);
    let (stop_tx, _stop_rx) = trigger::trigger();
    let mut stream = MapWriteStream::new(tx, op_rx);

    let receiver = FramedRead::new(rx, MapOperationDecoder::<i32, Text>::default());

    let driver = async move {
        while let Some(result) = stream.next().await {
            assert!(result.is_ok());
        }
    };

    let read = async move { receiver.collect::<Vec<_>>().await };

    let write = async move {
        let handle = MapDownlinkHandle::new(
            Address::text(None, NODE, LANE),
            op_tx,
            stop_tx,
            &Default::default(),
        );
        for i in 'a'..='j' {
            for j in 0..3 {
                assert!(handle.update(j, Text::from(i.to_string())).is_ok());
            }
        }
        assert!(handle.remove(2).is_ok());
    };

    let (_, received, r) = join3(driver, read, tokio::spawn(write)).await;
    assert!(r.is_ok());

    let mut key0 = None;
    let mut key1 = None;
    let mut key2 = None;
    let mut key2_removed = false;

    for result in received {
        match result {
            Ok(MapOperation::Update { key, value }) => match key {
                0 => {
                    if let Some(v) = &key0 {
                        assert!(v < &value);
                    }
                    key0 = Some(value);
                }
                1 => {
                    if let Some(v) = &key1 {
                        assert!(v < &value);
                    }
                    key1 = Some(value);
                }
                2 if !key2_removed => {
                    if let Some(v) = &key2 {
                        assert!(v < &value);
                    }
                    key2 = Some(value);
                }
                ow => panic!("Unexpected key: {}", ow),
            },
            Ok(MapOperation::Remove { key: 2 }) => {
                key2_removed = true;
            }
            ow => panic!("Unexpected result: {:?}", ow),
        }
    }
    assert!(key2_removed);
    assert_eq!(key0, Some(Text::new("j")));
    assert_eq!(key1, Some(Text::new("j")));
}

#[derive(Debug, Default)]
struct TestWaker {
    woken: AtomicBool,
}

impl TestWaker {
    fn reset(&self) {
        self.woken.store(false, Ordering::SeqCst)
    }

    fn was_woken(&self) -> bool {
        let r = self.woken.load(Ordering::SeqCst);
        self.reset();
        r
    }
}

impl ArcWake for TestWaker {
    fn wake_by_ref(arc_self: &Arc<Self>) {
        arc_self.woken.store(true, Ordering::SeqCst);
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum SinkState {
    HasCapacity,
    Full,
    Closed,
    FailOnReady,
    FailOnSend,
    FailOnFlush,
    FailOnClose,
}

#[derive(Debug)]
struct TestSinkInner {
    state: SinkState,
    will_flush: bool,
    will_close: bool,
    operations: Vec<MapOperation<i32, Text>>,
    ready: bool,
    flushed: bool,
}

impl TestSinkInner {
    fn full() -> Self {
        Self::with_state(SinkState::Full)
    }

    fn with_state(state: SinkState) -> Self {
        TestSinkInner {
            state,
            will_flush: true,
            will_close: true,
            operations: Default::default(),
            ready: false,
            flushed: false,
        }
    }
}

struct TestSink {
    inner: Arc<Mutex<TestSinkInner>>,
}

impl Default for TestSinkInner {
    fn default() -> Self {
        Self {
            state: SinkState::HasCapacity,
            will_flush: true,
            will_close: true,
            operations: Default::default(),
            ready: false,
            flushed: false,
        }
    }
}

impl Sink<MapOperation<i32, Text>> for TestSink {
    type Error = std::io::Error;

    fn poll_ready(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        let mut guard = self.get_mut().inner.lock();
        let TestSinkInner { state, ready, .. } = &mut *guard;
        match state {
            SinkState::Full => Poll::Pending,
            SinkState::FailOnReady | SinkState::Closed => {
                Poll::Ready(Err(std::io::Error::from(std::io::ErrorKind::BrokenPipe)))
            }
            _ => {
                *ready = true;
                Poll::Ready(Ok(()))
            }
        }
    }

    fn start_send(self: Pin<&mut Self>, item: MapOperation<i32, Text>) -> Result<(), Self::Error> {
        let mut guard = self.get_mut().inner.lock();
        let TestSinkInner {
            state,
            ready,
            operations,
            ..
        } = &mut *guard;
        assert!(*ready);
        *ready = false;
        if matches!(state, SinkState::FailOnSend | SinkState::Closed) {
            Err(std::io::Error::from(std::io::ErrorKind::BrokenPipe))
        } else {
            operations.push(item);
            Ok(())
        }
    }

    fn poll_flush(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        let mut guard = self.get_mut().inner.lock();
        let TestSinkInner {
            state,
            will_flush,
            flushed,
            ..
        } = &mut *guard;
        if matches!(state, SinkState::FailOnFlush | SinkState::Closed) {
            Poll::Ready(Err(std::io::Error::from(std::io::ErrorKind::BrokenPipe)))
        } else if *will_flush {
            *flushed = true;
            Poll::Ready(Ok(()))
        } else {
            Poll::Pending
        }
    }

    fn poll_close(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        let mut guard = self.get_mut().inner.lock();
        let TestSinkInner {
            state, will_close, ..
        } = &mut *guard;
        if matches!(state, SinkState::FailOnClose | SinkState::Closed) {
            Poll::Ready(Err(std::io::Error::from(std::io::ErrorKind::BrokenPipe)))
        } else if *will_close {
            *state = SinkState::Closed;
            Poll::Ready(Ok(()))
        } else {
            Poll::Pending
        }
    }
}

struct WriteStreamContext {
    set_tx: Option<mpsc::UnboundedSender<MapOperation<i32, Text>>>,
    sink: Arc<Mutex<TestSinkInner>>,
    wake_state: Arc<TestWaker>,
    waker: Waker,
}

impl WriteStreamContext {
    fn future_context(&self) -> Context<'_> {
        Context::from_waker(&self.waker)
    }

    fn sink_data(&self) -> impl Deref<Target = TestSinkInner> + '_ {
        self.sink.lock()
    }

    fn free_capacity(&mut self) {
        self.sink.lock().state = SinkState::HasCapacity;
    }

    fn send(&mut self, operation: MapOperation<i32, Text>) {
        self.set_tx
            .as_mut()
            .expect("Sender closed.")
            .send(operation)
            .expect("Channel dropped.");
    }

    fn drop_sender(&mut self) {
        self.set_tx = None;
    }
}

fn init_write_test(
    sink: Option<TestSinkInner>,
) -> (WriteStreamContext, MapWriteStream<i32, Text, TestSink>) {
    let (set_tx, set_rx) = mpsc::unbounded_channel::<MapOperation<i32, Text>>();

    let inner = Arc::new(Mutex::new(sink.unwrap_or_default()));
    let sink = TestSink {
        inner: inner.clone(),
    };

    let stream = MapWriteStream::with_sink(sink, set_rx);

    let state = Arc::new(TestWaker::default());
    let context = WriteStreamContext {
        set_tx: Some(set_tx),
        sink: inner,
        wake_state: state.clone(),
        waker: waker(state),
    };

    (context, stream)
}

#[test]
fn writer_no_data() {
    let (context, stream) = init_write_test(None);
    let stream = pin!(stream);

    assert!(stream.poll_next(&mut context.future_context()).is_pending());
    assert!(!context.wake_state.was_woken());
    assert!(context.sink_data().flushed);
}

#[test]
fn writer_data_available_with_capacity() {
    let (mut context, stream) = init_write_test(None);
    let mut stream = pin!(stream);

    let values = [
        MapOperation::Update {
            key: 1,
            value: Text::new("one"),
        },
        MapOperation::Update {
            key: 2,
            value: Text::new("two"),
        },
        MapOperation::Update {
            key: 3,
            value: Text::new("three"),
        },
    ];

    let mut expected = vec![];
    for op in values {
        context.send(op.clone());
        expected.push(op);

        let poll = stream.as_mut().poll_next(&mut context.future_context());
        assert!(matches!(poll, Poll::Ready(Some(Ok(())))));
        assert!(!context.wake_state.was_woken());
        let TestSinkInner {
            state,
            operations,
            ready,
            flushed,
            ..
        } = &*context.sink_data();
        assert_eq!(*state, SinkState::HasCapacity);
        assert!(!*ready);
        assert!(!*flushed);
        assert_eq!(operations, &expected);
    }
}

#[test]
fn writer_data_available_no_capacity() {
    let (mut context, stream) = init_write_test(Some(TestSinkInner::full()));
    let mut stream = pin!(stream);
    let op = MapOperation::Update {
        key: 1,
        value: Text::new("one"),
    };
    context.send(op.clone());

    assert!(stream
        .as_mut()
        .poll_next(&mut context.future_context())
        .is_pending());

    //Woken so we can potentially consume more.
    assert!(context.wake_state.was_woken());

    {
        let TestSinkInner {
            state,
            operations,
            ready,
            flushed,
            ..
        } = &*context.sink_data();
        assert_eq!(*state, SinkState::Full);
        assert!(!*ready);
        assert!(!*flushed);
        assert!(operations.is_empty());
    }
    //Free up capacity and try again.
    context.free_capacity();
    let poll = stream.poll_next(&mut context.future_context());
    assert!(matches!(poll, Poll::Ready(Some(Ok(_)))));

    let TestSinkInner {
        state,
        operations,
        ready,
        flushed,
        ..
    } = &*context.sink_data();
    assert_eq!(*state, SinkState::HasCapacity);
    assert!(!*ready);
    assert!(!*flushed);
    assert_eq!(operations, &[op]);
}

#[test]
fn writer_stop_no_data() {
    let (mut context, stream) = init_write_test(None);
    let stream = pin!(stream);

    context.drop_sender();
    let poll = stream.poll_next(&mut context.future_context());
    assert!(matches!(poll, Poll::Ready(None)));

    let TestSinkInner {
        state,
        operations,
        ready,
        flushed,
        ..
    } = &*context.sink_data();
    assert_eq!(*state, SinkState::Closed);
    assert!(!*ready);
    assert!(!*flushed);
    assert!(operations.is_empty());
}

#[test]
fn writer_stop_data_available() {
    let (mut context, stream) = init_write_test(None);
    let mut stream = pin!(stream);

    let op1 = MapOperation::Update {
        key: 5,
        value: Text::new("five"),
    };
    let op2 = MapOperation::Update {
        key: 3,
        value: Text::new("three"),
    };
    context.send(op1.clone());
    context.send(op2.clone());
    context.drop_sender();

    let poll = stream.as_mut().poll_next(&mut context.future_context());
    assert!(matches!(poll, Poll::Ready(Some(Ok(_)))));

    {
        let TestSinkInner {
            state,
            operations,
            ready,
            flushed,
            ..
        } = &*context.sink_data();
        assert_eq!(*state, SinkState::HasCapacity);
        assert!(!*ready);
        assert!(!*flushed);
        assert_eq!(operations, &[op1.clone()]);
    }

    let poll = stream.as_mut().poll_next(&mut context.future_context());
    assert!(matches!(poll, Poll::Ready(Some(Ok(_)))));

    {
        let TestSinkInner {
            state,
            operations,
            ready,
            flushed,
            ..
        } = &*context.sink_data();
        assert_eq!(*state, SinkState::HasCapacity);
        assert!(!*ready);
        assert!(!*flushed);
        assert_eq!(operations, &[op1.clone(), op2.clone()]);
    }

    let poll = stream.poll_next(&mut context.future_context());
    assert!(matches!(poll, Poll::Ready(None)));

    let TestSinkInner {
        state,
        operations,
        ready,
        flushed,
        ..
    } = &*context.sink_data();
    assert_eq!(*state, SinkState::Closed);
    assert!(!*ready);
    assert!(!*flushed);
    assert_eq!(operations, &[op1, op2]);
}

#[test]
fn writer_stop_data_pending() {
    let (mut context, stream) = init_write_test(Some(TestSinkInner::full()));
    let mut stream = pin!(stream);

    let op = MapOperation::Update {
        key: 5,
        value: Text::new("five"),
    };
    context.send(op.clone());

    let poll = stream.as_mut().poll_next(&mut context.future_context());
    assert!(poll.is_pending());

    {
        let TestSinkInner {
            state,
            operations,
            ready,
            flushed,
            ..
        } = &*context.sink_data();
        assert_eq!(*state, SinkState::Full);
        assert!(!*ready);
        assert!(!*flushed);
        assert!(operations.is_empty());
    }

    context.free_capacity();
    context.drop_sender();

    let poll = stream.as_mut().poll_next(&mut context.future_context());
    assert!(matches!(poll, Poll::Ready(Some(Ok(_)))));

    {
        let TestSinkInner {
            state,
            operations,
            ready,
            flushed,
            ..
        } = &*context.sink_data();
        assert_eq!(*state, SinkState::HasCapacity);
        assert!(!*ready);
        assert!(!*flushed);
        assert_eq!(operations, &[op.clone()]);
    }

    let poll = stream.poll_next(&mut context.future_context());
    assert!(matches!(poll, Poll::Ready(None)));

    let TestSinkInner {
        state,
        operations,
        ready,
        flushed,
        ..
    } = &*context.sink_data();
    assert_eq!(*state, SinkState::Closed);
    assert!(!*ready);
    assert!(!*flushed);
    assert_eq!(operations, &[op]);
}

#[test]
fn writer_fail_on_ready() {
    let (mut context, stream) =
        init_write_test(Some(TestSinkInner::with_state(SinkState::FailOnReady)));
    let mut stream = pin!(stream);
    context.send(MapOperation::Update {
        key: 5,
        value: Text::new("five"),
    });

    let poll = stream.as_mut().poll_next(&mut context.future_context());
    assert!(matches!(poll, Poll::Ready(Some(Err(_)))));

    let poll = stream.as_mut().poll_next(&mut context.future_context());
    assert!(matches!(poll, Poll::Ready(None)));
}

#[test]
fn writer_fail_on_send() {
    let (mut context, stream) =
        init_write_test(Some(TestSinkInner::with_state(SinkState::FailOnSend)));
    let mut stream = pin!(stream);
    context.send(MapOperation::Update {
        key: 56,
        value: Text::new("fiftysix"),
    });

    let poll = stream.as_mut().poll_next(&mut context.future_context());
    assert!(matches!(poll, Poll::Ready(Some(Err(_)))));

    let poll = stream.as_mut().poll_next(&mut context.future_context());
    assert!(matches!(poll, Poll::Ready(None)));
}

#[test]
fn writer_fail_on_flush() {
    let (context, stream) =
        init_write_test(Some(TestSinkInner::with_state(SinkState::FailOnFlush)));
    let mut stream = pin!(stream);

    let poll = stream.as_mut().poll_next(&mut context.future_context());
    assert!(matches!(poll, Poll::Ready(Some(Err(_)))));

    let poll = stream.as_mut().poll_next(&mut context.future_context());
    assert!(matches!(poll, Poll::Ready(None)));
}

#[test]
fn writer_fail_on_close() {
    let (mut context, stream) =
        init_write_test(Some(TestSinkInner::with_state(SinkState::FailOnClose)));
    let mut stream = pin!(stream);

    context.drop_sender();
    let poll = stream.as_mut().poll_next(&mut context.future_context());
    assert!(matches!(poll, Poll::Ready(Some(Err(_)))));

    let poll = stream.as_mut().poll_next(&mut context.future_context());
    assert!(matches!(poll, Poll::Ready(None)));
}
