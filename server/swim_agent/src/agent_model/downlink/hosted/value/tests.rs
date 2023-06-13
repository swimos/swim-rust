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

use std::{cell::RefCell, num::NonZeroUsize, sync::Arc};

use futures::{future::join3, SinkExt, StreamExt};
use parking_lot::Mutex;
use swim_api::protocol::downlink::{
    DownlinkNotification, DownlinkNotificationEncoder, DownlinkOperation, DownlinkOperationDecoder,
};
use swim_model::{address::Address, Text};
use swim_recon::printer::print_recon_compact;
use swim_utilities::{
    io::byte_channel::{self, ByteReader, ByteWriter},
    non_zero_usize,
    sync::circular_buffer,
    trigger,
};
use tokio::{io::AsyncWriteExt, task::yield_now};
use tokio_util::codec::{FramedRead, FramedWrite};

use super::{HostedValueDownlinkFactory, SimpleDownlinkConfig};
use crate::{
    agent_model::downlink::{
        handlers::BoxDownlinkChannel,
        hosted::{value::ValueWriteStream, ValueDownlinkHandle},
    },
    downlink_lifecycle::{
        on_failed::OnFailed,
        on_linked::OnLinked,
        on_synced::OnSynced,
        on_unlinked::OnUnlinked,
        value::{on_event::OnDownlinkEvent, on_set::OnDownlinkSet},
    },
    event_handler::{BoxEventHandler, HandlerActionExt, SideEffect},
};

struct FakeAgent;

#[derive(Debug, PartialEq, Eq)]
enum Event {
    Linked,
    Synced(i32),
    Event(i32),
    Set(Option<i32>, i32),
    Unlinked,
    Failed,
}

#[derive(Debug)]
struct FakeLifecycle {
    inner: Arc<Mutex<Vec<Event>>>,
}

impl OnLinked<FakeAgent> for FakeLifecycle {
    type OnLinkedHandler<'a> = BoxEventHandler<'a, FakeAgent>
    where
        Self: 'a;

    fn on_linked(&self) -> Self::OnLinkedHandler<'_> {
        let state = self.inner.clone();
        SideEffect::from(move || {
            state.lock().push(Event::Linked);
        })
        .boxed()
    }
}

impl OnUnlinked<FakeAgent> for FakeLifecycle {
    type OnUnlinkedHandler<'a> = BoxEventHandler<'a, FakeAgent>
    where
        Self: 'a;

    fn on_unlinked(&self) -> Self::OnUnlinkedHandler<'_> {
        let state = self.inner.clone();
        SideEffect::from(move || {
            state.lock().push(Event::Unlinked);
        })
        .boxed()
    }
}

impl OnFailed<FakeAgent> for FakeLifecycle {
    type OnFailedHandler<'a> = BoxEventHandler<'a, FakeAgent>
    where
        Self: 'a;

    fn on_failed(&self) -> Self::OnFailedHandler<'_> {
        let state = self.inner.clone();
        SideEffect::from(move || {
            state.lock().push(Event::Failed);
        })
        .boxed()
    }
}

impl OnSynced<i32, FakeAgent> for FakeLifecycle {
    type OnSyncedHandler<'a> = BoxEventHandler<'a, FakeAgent>
    where
        Self: 'a;

    fn on_synced<'a>(&'a self, value: &i32) -> Self::OnSyncedHandler<'a> {
        let state = self.inner.clone();
        let n = *value;
        SideEffect::from(move || {
            state.lock().push(Event::Synced(n));
        })
        .boxed()
    }
}

impl OnDownlinkEvent<i32, FakeAgent> for FakeLifecycle {
    type OnEventHandler<'a> = BoxEventHandler<'a, FakeAgent>
    where
        Self: 'a;

    fn on_event<'a>(&'a self, value: &i32) -> Self::OnEventHandler<'a> {
        let state = self.inner.clone();
        let n = *value;
        SideEffect::from(move || {
            state.lock().push(Event::Event(n));
        })
        .boxed()
    }
}

impl OnDownlinkSet<i32, FakeAgent> for FakeLifecycle {
    type OnSetHandler<'a> = BoxEventHandler<'a, FakeAgent>
    where
        Self: 'a;

    fn on_set<'a>(&'a self, previous: Option<i32>, new_value: &i32) -> Self::OnSetHandler<'a> {
        let state = self.inner.clone();
        let n = *new_value;
        SideEffect::from(move || {
            state.lock().push(Event::Set(previous, n));
        })
        .boxed()
    }
}

type Events = Arc<Mutex<Vec<Event>>>;
type State = RefCell<Option<i32>>;

const BUFFER_SIZE: NonZeroUsize = non_zero_usize!(4096);
const OUT_CHAN_SIZE: NonZeroUsize = non_zero_usize!(8);

struct TestContext {
    channel: BoxDownlinkChannel<FakeAgent>,
    events: Events,
    sender: FramedWrite<ByteWriter, DownlinkNotificationEncoder>,
    write_tx: circular_buffer::Sender<i32>,
    out_rx: ByteReader,
    stop_tx: Option<trigger::Sender>,
}

fn make_hosted_input(context: &FakeAgent, config: SimpleDownlinkConfig) -> TestContext {
    let inner: Events = Default::default();
    let lc = FakeLifecycle {
        inner: inner.clone(),
    };

    let (in_tx, in_rx) = byte_channel::byte_channel(BUFFER_SIZE);
    let (out_tx, out_rx) = byte_channel::byte_channel(BUFFER_SIZE);

    let address = Address::new(None, Text::new("/node"), Text::new("lane"));
    let (stop_tx, stop_rx) = trigger::trigger();

    let (write_tx, write_rx) = circular_buffer::channel(OUT_CHAN_SIZE);
    let fac =
        HostedValueDownlinkFactory::new(address, lc, State::default(), config, stop_rx, write_rx);
    let chan = fac.create(context, out_tx, in_rx);

    TestContext {
        channel: chan,
        events: inner,
        sender: FramedWrite::new(in_tx, Default::default()),
        write_tx,
        out_rx,
        stop_tx: Some(stop_tx),
    }
}

#[tokio::test]
async fn shutdown_when_input_stops() {
    let agent = FakeAgent;

    let TestContext {
        mut channel,
        sender,
        ..
    } = make_hosted_input(&agent, SimpleDownlinkConfig::default());

    assert!(channel.next_event(&agent).is_none());

    drop(sender);

    assert!(channel.await_ready().await.is_none());

    assert!(channel.next_event(&agent).is_none());
}

#[tokio::test]
async fn shutdown_on_stop_trigger() {
    let agent = FakeAgent;

    let TestContext {
        mut channel,
        sender: _sender,
        stop_tx,
        events,
        ..
    } = make_hosted_input(&agent, SimpleDownlinkConfig::default());

    assert!(channel.next_event(&agent).is_none());

    stop_tx.expect("Stop trigger missing.").trigger();

    assert!(channel.await_ready().await.is_some());
    let handler = channel
        .next_event(&agent)
        .expect("Expected unlinked handler.");
    run_handler(handler, &agent);
    assert_eq!(take_events(&events), vec![Event::Unlinked]);

    assert!(channel.await_ready().await.is_none());
    assert!(channel.next_event(&agent).is_none());
}

#[tokio::test]
async fn terminate_on_error() {
    let agent = FakeAgent;
    let TestContext {
        mut channel,
        mut sender,
        events,
        ..
    } = make_hosted_input(&agent, SimpleDownlinkConfig::default());

    assert!(sender.get_mut().write_u8(100).await.is_ok()); //Invalid message kind tag.

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

fn to_bytes(not: DownlinkNotification<i32>) -> DownlinkNotification<Vec<u8>> {
    match not {
        DownlinkNotification::Linked => DownlinkNotification::Linked,
        DownlinkNotification::Synced => DownlinkNotification::Synced,
        DownlinkNotification::Event { body } => {
            let recon = format!("{}", print_recon_compact(&body));
            DownlinkNotification::Event {
                body: recon.into_bytes(),
            }
        }
        DownlinkNotification::Unlinked => DownlinkNotification::Unlinked,
    }
}

use super::super::test_support::run_handler;

async fn run_with_expectations(
    context: &mut TestContext,
    agent: &FakeAgent,
    notifications: Vec<(DownlinkNotification<i32>, Option<Vec<Event>>)>,
) {
    let TestContext {
        channel,
        events,
        sender,
        stop_tx,
        write_tx: _write_tx,
        out_rx: _out_rx,
    } = context;

    *stop_tx = None;
    for (not, expected) in notifications {
        let bytes_not = to_bytes(not);
        assert!(sender.send(bytes_not).await.is_ok());
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
    let agent = FakeAgent;
    let mut context = make_hosted_input(&agent, SimpleDownlinkConfig::default());

    run_with_expectations(
        &mut context,
        &agent,
        vec![(DownlinkNotification::Linked, Some(vec![Event::Linked]))],
    )
    .await;
}

#[tokio::test]
async fn emit_synced_handler() {
    let agent = FakeAgent;
    let mut context = make_hosted_input(&agent, SimpleDownlinkConfig::default());

    run_with_expectations(
        &mut context,
        &agent,
        vec![
            (DownlinkNotification::Linked, Some(vec![Event::Linked])),
            (DownlinkNotification::Event { body: 13 }, None),
            (DownlinkNotification::Synced, Some(vec![Event::Synced(13)])),
        ],
    )
    .await;
}

#[tokio::test]
async fn emit_event_handlers() {
    let agent = FakeAgent;
    let mut context = make_hosted_input(&agent, SimpleDownlinkConfig::default());

    run_with_expectations(
        &mut context,
        &agent,
        vec![
            (DownlinkNotification::Linked, Some(vec![Event::Linked])),
            (DownlinkNotification::Event { body: 13 }, None),
            (DownlinkNotification::Synced, Some(vec![Event::Synced(13)])),
            (
                DownlinkNotification::Event { body: 15 },
                Some(vec![Event::Event(15), Event::Set(Some(13), 15)]),
            ),
        ],
    )
    .await;
}

#[tokio::test]
async fn emit_events_before_synced() {
    let agent = FakeAgent;

    let config = SimpleDownlinkConfig {
        events_when_not_synced: true,
        terminate_on_unlinked: true,
    };
    let mut context = make_hosted_input(&agent, config);

    run_with_expectations(
        &mut context,
        &agent,
        vec![
            (DownlinkNotification::Linked, Some(vec![Event::Linked])),
            (
                DownlinkNotification::Event { body: 13 },
                Some(vec![Event::Event(13), Event::Set(None, 13)]),
            ),
            (DownlinkNotification::Synced, Some(vec![Event::Synced(13)])),
        ],
    )
    .await;
}

#[tokio::test]
async fn emit_unlinked_handler() {
    let agent = FakeAgent;
    let mut context = make_hosted_input(&agent, SimpleDownlinkConfig::default());

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
async fn revive_unlinked_downlink() {
    let config = SimpleDownlinkConfig {
        events_when_not_synced: true,
        terminate_on_unlinked: false,
    };

    let agent = FakeAgent;

    let mut context = make_hosted_input(&agent, config);

    run_with_expectations(
        &mut context,
        &agent,
        vec![
            (DownlinkNotification::Linked, Some(vec![Event::Linked])),
            (
                DownlinkNotification::Event { body: 13 },
                Some(vec![Event::Event(13), Event::Set(None, 13)]),
            ),
            (DownlinkNotification::Synced, Some(vec![Event::Synced(13)])),
            (DownlinkNotification::Unlinked, Some(vec![Event::Unlinked])),
            (DownlinkNotification::Linked, Some(vec![Event::Linked])),
            (
                DownlinkNotification::Event { body: 27 },
                Some(vec![Event::Event(27), Event::Set(None, 27)]),
            ),
            (DownlinkNotification::Synced, Some(vec![Event::Synced(27)])),
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

#[tokio::test]
async fn value_downlink_writer() {
    let (set_tx, set_rx) = circular_buffer::watch_channel::<i32>();
    let (tx, rx) = byte_channel::byte_channel(BUFFER_SIZE);
    let (stop_tx, _stop_rx) = trigger::trigger();
    let mut stream = ValueWriteStream::new(tx, set_rx);

    let mut receiver = FramedRead::new(rx, DownlinkOperationDecoder);

    let driver = async move {
        while let Some(result) = stream.next().await {
            assert!(result.is_ok());
        }
    };

    let read = async move {
        let mut n: i32 = -1;
        while let Some(Ok(DownlinkOperation { body })) = receiver.next().await {
            let i = std::str::from_utf8(body.as_ref())
                .expect("Invalid UTF8")
                .parse::<i32>()
                .expect("Not an valid i32.");
            assert!(i > n);
            n = i;
        }
        assert_eq!(n, 10);
    };

    let write = async move {
        let address = Address::new(None, Text::new("/node"), Text::new("lane"));
        let mut handle = ValueDownlinkHandle::new(address, set_tx, stop_tx, &Default::default());
        for i in 0..=10 {
            assert!(handle.set(i).is_ok());
            if i % 2 == 0 {
                yield_now().await;
            }
        }
    };

    let (_, _, r) = join3(driver, read, tokio::spawn(write)).await;
    assert!(r.is_ok());
}
