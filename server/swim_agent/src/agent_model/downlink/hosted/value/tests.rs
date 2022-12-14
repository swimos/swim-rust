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

use std::{cell::RefCell, num::NonZeroUsize, sync::Arc};

use futures::{future::join3, pin_mut, SinkExt, StreamExt};
use parking_lot::Mutex;
use swim_api::protocol::downlink::{
    DownlinkNotification, DownlinkNotificationEncoder, DownlinkOperation, DownlinkOperationDecoder,
};
use swim_model::{address::Address, Text};
use swim_recon::printer::print_recon_compact;
use swim_utilities::{
    algebra::non_zero_usize,
    io::byte_channel::{self, ByteWriter},
    sync::circular_buffer,
};
use tokio::{io::AsyncWriteExt, task::yield_now};
use tokio_util::codec::{FramedRead, FramedWrite};

use super::{HostedValueDownlinkChannel, ValueDownlinkConfig};
use crate::{
    agent_model::downlink::{
        handlers::DownlinkChannel,
        hosted::{value_dl_write_stream, ValueDownlinkHandle},
    },
    downlink_lifecycle::{
        on_linked::OnLinked,
        on_synced::OnSynced,
        on_unlinked::OnUnlinked,
        value::{on_event::OnDownlinkEvent, on_set::OnDownlinkSet},
    },
    event_handler::{BoxEventHandler, EventHandlerExt, SideEffect},
};

struct FakeAgent;

#[derive(Debug, PartialEq, Eq)]
enum Event {
    Linked,
    Synced(i32),
    Event(i32),
    Set(Option<i32>, i32),
    Unlinked,
}

#[derive(Debug)]
struct FakeLifecycle {
    inner: Arc<Mutex<Vec<Event>>>,
}

impl<'a> OnLinked<'a, FakeAgent> for FakeLifecycle {
    type OnLinkedHandler = BoxEventHandler<'a, FakeAgent>;

    fn on_linked(&'a self) -> Self::OnLinkedHandler {
        let state = self.inner.clone();
        SideEffect::from(move || {
            state.lock().push(Event::Linked);
        })
        .boxed()
    }
}

impl<'a> OnUnlinked<'a, FakeAgent> for FakeLifecycle {
    type OnUnlinkedHandler = BoxEventHandler<'a, FakeAgent>;

    fn on_unlinked(&'a self) -> Self::OnUnlinkedHandler {
        let state = self.inner.clone();
        SideEffect::from(move || {
            state.lock().push(Event::Unlinked);
        })
        .boxed()
    }
}

impl<'a> OnSynced<'a, i32, FakeAgent> for FakeLifecycle {
    type OnSyncedHandler = BoxEventHandler<'a, FakeAgent>;

    fn on_synced(&'a self, value: &i32) -> Self::OnSyncedHandler {
        let state = self.inner.clone();
        let n = *value;
        SideEffect::from(move || {
            state.lock().push(Event::Synced(n));
        })
        .boxed()
    }
}

impl<'a> OnDownlinkEvent<'a, i32, FakeAgent> for FakeLifecycle {
    type OnEventHandler = BoxEventHandler<'a, FakeAgent>;

    fn on_event(&'a self, value: &i32) -> Self::OnEventHandler {
        let state = self.inner.clone();
        let n = *value;
        SideEffect::from(move || {
            state.lock().push(Event::Event(n));
        })
        .boxed()
    }
}

impl<'a> OnDownlinkSet<'a, i32, FakeAgent> for FakeLifecycle {
    type OnSetHandler = BoxEventHandler<'a, FakeAgent>;

    fn on_set(&'a self, previous: Option<i32>, new_value: &i32) -> Self::OnSetHandler {
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

struct TestContext {
    channel: HostedValueDownlinkChannel<i32, FakeLifecycle, State>,
    events: Events,
    sender: FramedWrite<ByteWriter, DownlinkNotificationEncoder>,
}

fn make_hosted_input(config: ValueDownlinkConfig) -> TestContext {
    let inner: Events = Default::default();
    let lc = FakeLifecycle {
        inner: inner.clone(),
    };

    let (tx, rx) = byte_channel::byte_channel(BUFFER_SIZE);

    let address = Address::new(None, Text::new("/node"), Text::new("lane"));

    let chan = HostedValueDownlinkChannel::new(address, rx, lc, State::default(), config);
    TestContext {
        channel: chan,
        events: inner,
        sender: FramedWrite::new(tx, Default::default()),
    }
}

#[tokio::test]
async fn shutdown_when_input_stops() {
    let TestContext {
        mut channel,
        sender,
        ..
    } = make_hosted_input(ValueDownlinkConfig::default());

    let agent = FakeAgent;

    assert!(channel.next_event(&agent).is_none());

    drop(sender);

    assert!(channel.await_ready().await.is_none());

    assert!(channel.next_event(&agent).is_none());
}

#[tokio::test]
async fn terminate_on_error() {
    let TestContext {
        mut channel,
        mut sender,
        ..
    } = make_hosted_input(ValueDownlinkConfig::default());

    let agent = FakeAgent;

    assert!(sender.get_mut().write_u8(100).await.is_ok()); //Invalid message kind tag.

    assert!(matches!(channel.await_ready().await, Some(Err(_))));
    assert!(channel.next_event(&agent).is_none());
    assert!(channel.await_ready().await.is_none());
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
    } = context;

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
    let mut context = make_hosted_input(ValueDownlinkConfig::default());

    let agent = FakeAgent;

    run_with_expectations(
        &mut context,
        &agent,
        vec![(DownlinkNotification::Linked, Some(vec![Event::Linked]))],
    )
    .await;
}

#[tokio::test]
async fn emit_synced_handler() {
    let mut context = make_hosted_input(ValueDownlinkConfig::default());

    let agent = FakeAgent;

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
    let mut context = make_hosted_input(ValueDownlinkConfig::default());

    let agent = FakeAgent;

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
    let config = ValueDownlinkConfig {
        events_when_not_synced: true,
        terminate_on_unlinked: true,
    };
    let mut context = make_hosted_input(config);

    let agent = FakeAgent;

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
    let mut context = make_hosted_input(ValueDownlinkConfig::default());

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
async fn revive_unlinked_downlink() {
    let config = ValueDownlinkConfig {
        events_when_not_synced: true,
        terminate_on_unlinked: false,
    };

    let mut context = make_hosted_input(config);

    let agent = FakeAgent;

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
    let stream = value_dl_write_stream(tx, set_rx);
    pin_mut!(stream);

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
        let mut handle = ValueDownlinkHandle::new(address, set_tx);
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
