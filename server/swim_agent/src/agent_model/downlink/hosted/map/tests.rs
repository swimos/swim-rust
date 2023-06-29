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

use std::{cell::RefCell, collections::HashMap, num::NonZeroUsize, pin::pin, sync::Arc};

use bytes::BytesMut;
use futures::{future::join3, SinkExt, StreamExt};
use parking_lot::Mutex;
use swim_api::protocol::{
    downlink::{DownlinkNotification, DownlinkNotificationEncoder},
    map::{MapMessage, MapMessageEncoder, MapOperation, MapOperationDecoder, MapOperationEncoder},
};
use swim_model::{address::Address, Text};
use swim_utilities::{
    io::byte_channel::{self, ByteWriter},
    non_zero_usize, trigger,
};
use tokio::{io::AsyncWriteExt, sync::mpsc};
use tokio_util::codec::{Encoder, FramedRead, FramedWrite};

use crate::{
    agent_model::downlink::{hosted::map_dl_write_stream, MapDownlinkHandle},
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

use super::{DownlinkChannel, HostedMapDownlinkChannel, MapDlState};

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
    channel: HostedMapDownlinkChannel<i32, Text, FakeLifecycle, State>,
    events: Events,
    sender: Writer,
    stop_tx: Option<trigger::Sender>,
}

const NODE: &str = "/node";
const LANE: &str = "lane";

fn make_hosted_input(config: MapDownlinkConfig) -> TestContext {
    let events: Events = Default::default();
    let lc = FakeLifecycle {
        events: events.clone(),
    };

    let (tx, rx) = byte_channel::byte_channel(BUFFER_SIZE);
    let (stop_tx, stop_rx) = trigger::trigger();

    let address = Address::text(None, NODE, LANE);

    let chan = HostedMapDownlinkChannel::new(
        address,
        rx,
        lc,
        State::default(),
        config,
        stop_rx,
        Default::default(),
    );
    TestContext {
        channel: chan,
        events,
        sender: Writer::new(tx),
        stop_tx: Some(stop_tx),
    }
}

#[tokio::test]
async fn shutdown_when_input_stops() {
    let TestContext {
        mut channel,
        sender,
        ..
    } = make_hosted_input(MapDownlinkConfig::default());

    let agent = FakeAgent;

    assert!(channel.next_event(&agent).is_none());

    drop(sender);

    assert!(channel.await_ready().await.is_none());

    assert!(channel.next_event(&agent).is_none());
}

#[tokio::test]
async fn shutdown_on_stop_trigger() {
    let TestContext {
        mut channel,
        sender: _sender,
        stop_tx,
        ..
    } = make_hosted_input(MapDownlinkConfig::default());

    let agent = FakeAgent;

    assert!(channel.next_event(&agent).is_none());

    stop_tx.expect("Stop trigger missing.").trigger();

    assert!(channel.await_ready().await.is_none());

    assert!(channel.next_event(&agent).is_none());
}

#[tokio::test]
async fn terminate_on_error() {
    let TestContext {
        mut channel,
        mut sender,
        events,
        ..
    } = make_hosted_input(MapDownlinkConfig::default());

    let agent = FakeAgent;

    assert!(sender.sender.get_mut().write_u8(100).await.is_ok()); //Invalid message kind tag.

    assert!(matches!(channel.await_ready().await, Some(Err(_))));
    let handler = channel
        .next_event(&agent)
        .expect("Expected failure response.");
    run_handler(handler, &agent);
    assert_eq!(take_events(&events), vec![Event::Failed]);
}

fn take_events(events: &Events) -> Vec<Event> {
    std::mem::take(&mut *events.lock())
}

use super::super::test_support::run_handler;

type NotificationsAndEvents = Vec<(
    DownlinkNotification<MapMessage<i32, Text>>,
    Option<Vec<Event>>,
)>;

async fn run_with_expectations(
    context: &mut TestContext,
    agent: &FakeAgent,
    notifications: NotificationsAndEvents,
) {
    let TestContext {
        channel,
        events,
        sender,
        stop_tx,
    } = context;

    *stop_tx = None;

    for (not, expected) in notifications {
        assert!(sender.send(not).await.is_ok());
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
}

#[tokio::test]
async fn emit_linked_handler() {
    let mut context = make_hosted_input(MapDownlinkConfig::default());

    let agent = FakeAgent;

    run_with_expectations(
        &mut context,
        &agent,
        vec![(DownlinkNotification::Linked, Some(vec![Event::Linked]))],
    )
    .await;
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
    let mut context = make_hosted_input(MapDownlinkConfig::default());

    let agent = FakeAgent;

    run_with_expectations(
        &mut context,
        &agent,
        vec![
            (DownlinkNotification::Linked, Some(vec![Event::Linked])),
            (upd(1, "a"), None),
            (upd(2, "b"), None),
            (upd(3, "c"), None),
            (
                DownlinkNotification::Synced,
                Some(vec![Event::synced([(1, "a"), (2, "b"), (3, "c")])]),
            ),
        ],
    )
    .await;
}

#[tokio::test]
async fn emit_event_handlers() {
    let mut context = make_hosted_input(MapDownlinkConfig::default());

    let agent = FakeAgent;

    run_with_expectations(
        &mut context,
        &agent,
        vec![
            (DownlinkNotification::Linked, Some(vec![Event::Linked])),
            (upd(1, "a"), None),
            (
                DownlinkNotification::Synced,
                Some(vec![Event::synced([(1, "a")])]),
            ),
            (
                upd(2, "b"),
                Some(vec![Event::updated(2, "b", None, [(1, "a"), (2, "b")])]),
            ),
            (
                upd(1, "aa"),
                Some(vec![Event::updated(
                    1,
                    "aa",
                    Some("a"),
                    [(1, "aa"), (2, "b")],
                )]),
            ),
            (rem(2), Some(vec![Event::removed(2, "b", [(1, "aa")])])),
            (clr(), Some(vec![Event::cleared([(1, "aa")])])),
        ],
    )
    .await;
}

#[tokio::test]
async fn emit_events_before_synced() {
    let config = MapDownlinkConfig {
        events_when_not_synced: true,
        ..Default::default()
    };
    let mut context = make_hosted_input(config);

    let agent = FakeAgent;

    run_with_expectations(
        &mut context,
        &agent,
        vec![
            (DownlinkNotification::Linked, Some(vec![Event::Linked])),
            (
                upd(1, "a"),
                Some(vec![Event::updated(1, "a", None, [(1, "a")])]),
            ),
            (
                upd(2, "b"),
                Some(vec![Event::updated(2, "b", None, [(1, "a"), (2, "b")])]),
            ),
            (
                DownlinkNotification::Synced,
                Some(vec![Event::synced([(1, "a"), (2, "b")])]),
            ),
        ],
    )
    .await;
}

#[tokio::test]
async fn emit_unlinked_handler() {
    let mut context = make_hosted_input(MapDownlinkConfig::default());

    let agent = FakeAgent;

    run_with_expectations(
        &mut context,
        &agent,
        vec![
            (DownlinkNotification::Linked, Some(vec![Event::Linked])),
            (DownlinkNotification::Unlinked, Some(vec![Event::Unlinked])),
        ],
    )
    .await;

    let TestContext { channel, .. } = &mut context;

    assert!(channel.await_ready().await.is_none());

    assert!(channel.next_event(&agent).is_none());
}

#[tokio::test]
async fn emit_take_handlers() {
    let mut context = make_hosted_input(MapDownlinkConfig::default());

    let agent = FakeAgent;

    run_with_expectations(
        &mut context,
        &agent,
        vec![
            (DownlinkNotification::Linked, Some(vec![Event::Linked])),
            (upd(1, "a"), None),
            (upd(2, "b"), None),
            (upd(3, "c"), None),
            (upd(4, "d"), None),
            (upd(5, "e"), None),
            (
                DownlinkNotification::Synced,
                Some(vec![Event::synced([
                    (1, "a"),
                    (2, "b"),
                    (3, "c"),
                    (4, "d"),
                    (5, "e"),
                ])]),
            ),
            (
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
}

#[tokio::test]
async fn emit_drop_handlers() {
    let mut context = make_hosted_input(MapDownlinkConfig::default());

    let agent = FakeAgent;

    run_with_expectations(
        &mut context,
        &agent,
        vec![
            (DownlinkNotification::Linked, Some(vec![Event::Linked])),
            (upd(1, "a"), None),
            (upd(2, "b"), None),
            (upd(3, "c"), None),
            (upd(4, "d"), None),
            (upd(5, "e"), None),
            (
                DownlinkNotification::Synced,
                Some(vec![Event::synced([
                    (1, "a"),
                    (2, "b"),
                    (3, "c"),
                    (4, "d"),
                    (5, "e"),
                ])]),
            ),
            (
                drp(2),
                Some(vec![
                    Event::removed(1, "a", [(2, "b"), (3, "c"), (4, "d"), (5, "e")]),
                    Event::removed(2, "b", [(3, "c"), (4, "d"), (5, "e")]),
                ]),
            ),
        ],
    )
    .await;
}

#[tokio::test]
async fn emit_drop_all_handlers() {
    let mut context = make_hosted_input(MapDownlinkConfig::default());

    let agent = FakeAgent;

    run_with_expectations(
        &mut context,
        &agent,
        vec![
            (DownlinkNotification::Linked, Some(vec![Event::Linked])),
            (upd(1, "a"), None),
            (upd(2, "b"), None),
            (upd(3, "c"), None),
            (upd(4, "d"), None),
            (upd(5, "e"), None),
            (
                DownlinkNotification::Synced,
                Some(vec![Event::synced([
                    (1, "a"),
                    (2, "b"),
                    (3, "c"),
                    (4, "d"),
                    (5, "e"),
                ])]),
            ),
            (
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
}

#[tokio::test]
async fn revive_unlinked_downlink() {
    let config = MapDownlinkConfig {
        terminate_on_unlinked: false,
        ..Default::default()
    };

    let mut context = make_hosted_input(config);

    let agent = FakeAgent;

    run_with_expectations(
        &mut context,
        &agent,
        vec![
            (DownlinkNotification::Linked, Some(vec![Event::Linked])),
            (upd(1, "a"), None),
            (
                DownlinkNotification::Synced,
                Some(vec![Event::synced([(1, "a")])]),
            ),
            (DownlinkNotification::Unlinked, Some(vec![Event::Unlinked])),
            (DownlinkNotification::Linked, Some(vec![Event::Linked])),
            (upd(2, "b"), None),
            (
                DownlinkNotification::Synced,
                Some(vec![Event::synced([(2, "b")])]),
            ),
        ],
    )
    .await;

    let TestContext {
        mut channel,
        sender,
        ..
    } = context;

    drop(sender);
    assert!(channel.await_ready().await.is_none());

    assert!(channel.next_event(&agent).is_none());
}

const CHANNEL_SIZE: usize = 8;

#[tokio::test]
async fn map_downlink_writer() {
    let (op_tx, op_rx) = mpsc::channel::<MapOperation<i32, Text>>(CHANNEL_SIZE);
    let (tx, rx) = byte_channel::byte_channel(BUFFER_SIZE);
    let (stop_tx, _stop_rx) = trigger::trigger();
    let mut stream = pin!(map_dl_write_stream(tx, op_rx));

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
                assert!(handle.update(j, Text::from(i.to_string())).await.is_ok());
            }
        }
        assert!(handle.remove(2).await.is_ok());
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
