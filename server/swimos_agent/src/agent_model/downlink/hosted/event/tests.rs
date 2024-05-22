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

use std::{num::NonZeroUsize, sync::Arc};

use futures::SinkExt;
use parking_lot::Mutex;
use swimos_agent_protocol::downlink::{DownlinkNotification, DownlinkNotificationEncoder};
use swimos_model::{address::Address, Text};
use swimos_recon::print_recon_compact;
use swimos_utilities::{
    io::byte_channel::{self, ByteWriter},
    non_zero_usize, trigger,
};
use tokio::io::AsyncWriteExt;
use tokio_util::codec::FramedWrite;

use super::{HostedEventDownlinkFactory, SimpleDownlinkConfig};
use crate::{
    agent_model::downlink::handlers::{BoxDownlinkChannel, DownlinkChannelEvent},
    downlink_lifecycle::{
        event::on_event::OnConsumeEvent, on_failed::OnFailed, on_linked::OnLinked,
        on_synced::OnSynced, on_unlinked::OnUnlinked,
    },
    event_handler::{BoxEventHandler, HandlerActionExt, SideEffect},
};

struct FakeAgent;

#[derive(Debug, PartialEq, Eq)]
enum TestEvent {
    Linked,
    Synced,
    Event(i32),
    Unlinked,
    Failed,
}

#[derive(Debug)]
struct FakeLifecycle {
    inner: Arc<Mutex<Vec<TestEvent>>>,
}

impl OnLinked<FakeAgent> for FakeLifecycle {
    type OnLinkedHandler<'a> = BoxEventHandler<'a, FakeAgent>
    where
        Self: 'a;

    fn on_linked(&self) -> Self::OnLinkedHandler<'_> {
        let state = self.inner.clone();
        SideEffect::from(move || {
            state.lock().push(TestEvent::Linked);
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
            state.lock().push(TestEvent::Unlinked);
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
            state.lock().push(TestEvent::Failed);
        })
        .boxed()
    }
}

impl OnSynced<(), FakeAgent> for FakeLifecycle {
    type OnSyncedHandler<'a> = BoxEventHandler<'a, FakeAgent>
    where
        Self: 'a;

    fn on_synced<'a>(&'a self, _: &()) -> Self::OnSyncedHandler<'a> {
        let state = self.inner.clone();
        SideEffect::from(move || {
            state.lock().push(TestEvent::Synced);
        })
        .boxed()
    }
}

impl OnConsumeEvent<i32, FakeAgent> for FakeLifecycle {
    type OnEventHandler<'a> = BoxEventHandler<'a, FakeAgent>
    where
        Self: 'a;

    fn on_event(&self, value: i32) -> Self::OnEventHandler<'_> {
        let state = self.inner.clone();
        SideEffect::from(move || {
            state.lock().push(TestEvent::Event(value));
        })
        .boxed()
    }
}

type Events = Arc<Mutex<Vec<TestEvent>>>;

const BUFFER_SIZE: NonZeroUsize = non_zero_usize!(4096);

struct TestContext {
    channel: BoxDownlinkChannel<FakeAgent>,
    events: Events,
    sender: FramedWrite<ByteWriter, DownlinkNotificationEncoder>,
    stop_tx: Option<trigger::Sender>,
}

fn make_hosted_input(config: SimpleDownlinkConfig) -> TestContext {
    let inner: Events = Default::default();
    let lc = FakeLifecycle {
        inner: inner.clone(),
    };

    let (tx, rx) = byte_channel::byte_channel(BUFFER_SIZE);
    let (stop_tx, stop_rx) = trigger::trigger();

    let address = Address::new(None, Text::new("/node"), Text::new("lane"));

    let fac = HostedEventDownlinkFactory::new(address, lc, config, stop_rx, false);

    let chan = fac.create(rx);
    TestContext {
        channel: chan,
        events: inner,
        sender: FramedWrite::new(tx, Default::default()),
        stop_tx: Some(stop_tx),
    }
}

async fn expect_write_stopped(channel: &mut BoxDownlinkChannel<FakeAgent>, agent: &FakeAgent) {
    assert!(channel.next_event(agent).is_none());
    assert!(matches!(
        channel.await_ready().await,
        Some(Ok(DownlinkChannelEvent::WriteStreamTerminated))
    ));
    assert!(channel.next_event(agent).is_none());
}

async fn clean_shutdown(mut context: TestContext, agent: FakeAgent, expect_unlink: bool) {
    let TestContext {
        channel,
        events,
        stop_tx,
        ..
    } = &mut context;
    if let Some(stop) = stop_tx.take() {
        stop.trigger();
    }
    if expect_unlink {
        expect_unlink_shutdown(channel, &agent, events).await;
    } else {
        expect_no_unlink_shutdown(channel, &agent).await;
    }
}

async fn expect_unlink_shutdown(
    channel: &mut BoxDownlinkChannel<FakeAgent>,
    agent: &FakeAgent,
    events: &Events,
) {
    assert!(matches!(
        channel.await_ready().await,
        Some(Ok(DownlinkChannelEvent::HandlerReady))
    ));
    let handler = channel
        .next_event(agent)
        .expect("Expected unlinked handler.");
    run_handler(handler, agent);
    assert_eq!(take_events(events), vec![TestEvent::Unlinked]);

    assert!(channel.await_ready().await.is_none());
    assert!(channel.next_event(agent).is_none());
}

async fn expect_no_unlink_shutdown(channel: &mut BoxDownlinkChannel<FakeAgent>, agent: &FakeAgent) {
    assert!(channel.await_ready().await.is_none());
    assert!(channel.next_event(agent).is_none());
}

#[tokio::test]
async fn event_dl_shutdown_when_input_stops() {
    let TestContext {
        mut channel,
        sender,
        stop_tx: _stop_tx,
        events: _events,
    } = make_hosted_input(SimpleDownlinkConfig::default());

    let agent = FakeAgent;

    expect_write_stopped(&mut channel, &agent).await;

    drop(sender);

    expect_no_unlink_shutdown(&mut channel, &agent).await;
}

#[tokio::test]
async fn event_dl_shutdown_on_stop_signal() {
    let TestContext {
        mut channel,
        sender: _sender,
        stop_tx,
        events: _events,
    } = make_hosted_input(SimpleDownlinkConfig::default());

    let agent = FakeAgent;

    expect_write_stopped(&mut channel, &agent).await;

    stop_tx.expect("Stop trigger missing.").trigger();

    expect_no_unlink_shutdown(&mut channel, &agent).await;
}

#[tokio::test]
async fn event_dl_terminate_on_error() {
    let TestContext {
        mut channel,
        mut sender,
        events,
        stop_tx: _stop_tx,
    } = make_hosted_input(SimpleDownlinkConfig::default());

    let agent = FakeAgent;

    expect_write_stopped(&mut channel, &agent).await;

    assert!(sender.get_mut().write_u8(100).await.is_ok()); //Invalid message kind tag.

    assert!(matches!(channel.await_ready().await, Some(Err(_))));
    let handler = channel
        .next_event(&agent)
        .expect("Expected failure response.");
    run_handler(handler, &agent);
    assert_eq!(take_events(&events), vec![TestEvent::Failed]);
}

fn take_events(events: &Events) -> Vec<TestEvent> {
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
    notifications: Vec<(DownlinkNotification<i32>, Option<Vec<TestEvent>>)>,
) {
    let TestContext {
        channel,
        events,
        sender,
        ..
    } = context;

    for (not, expected) in notifications {
        let bytes_not = to_bytes(not);
        assert!(sender.send(bytes_not).await.is_ok());
        assert!(matches!(
            channel.await_ready().await,
            Some(Ok(DownlinkChannelEvent::HandlerReady))
        ));
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
async fn event_dl_emit_linked_handler() {
    let mut context = make_hosted_input(SimpleDownlinkConfig::default());

    let agent = FakeAgent;

    expect_write_stopped(&mut context.channel, &agent).await;

    run_with_expectations(
        &mut context,
        &agent,
        vec![(DownlinkNotification::Linked, Some(vec![TestEvent::Linked]))],
    )
    .await;

    clean_shutdown(context, agent, true).await;
}

#[tokio::test]
async fn event_dl_emit_synced_handler() {
    let mut context = make_hosted_input(SimpleDownlinkConfig::default());

    let agent = FakeAgent;

    expect_write_stopped(&mut context.channel, &agent).await;

    run_with_expectations(
        &mut context,
        &agent,
        vec![
            (DownlinkNotification::Linked, Some(vec![TestEvent::Linked])),
            (DownlinkNotification::Event { body: 13 }, None),
            (DownlinkNotification::Synced, Some(vec![TestEvent::Synced])),
        ],
    )
    .await;

    clean_shutdown(context, agent, true).await;
}

#[tokio::test]
async fn event_dl_emit_event_handlers() {
    let mut context = make_hosted_input(SimpleDownlinkConfig::default());

    let agent = FakeAgent;

    expect_write_stopped(&mut context.channel, &agent).await;

    run_with_expectations(
        &mut context,
        &agent,
        vec![
            (DownlinkNotification::Linked, Some(vec![TestEvent::Linked])),
            (DownlinkNotification::Event { body: 13 }, None),
            (DownlinkNotification::Synced, Some(vec![TestEvent::Synced])),
            (
                DownlinkNotification::Event { body: 15 },
                Some(vec![TestEvent::Event(15)]),
            ),
        ],
    )
    .await;

    clean_shutdown(context, agent, true).await;
}

#[tokio::test]
async fn event_dl_emit_events_before_synced() {
    let config = SimpleDownlinkConfig {
        events_when_not_synced: true,
        terminate_on_unlinked: true,
    };
    let mut context = make_hosted_input(config);

    let agent = FakeAgent;

    expect_write_stopped(&mut context.channel, &agent).await;

    run_with_expectations(
        &mut context,
        &agent,
        vec![
            (DownlinkNotification::Linked, Some(vec![TestEvent::Linked])),
            (
                DownlinkNotification::Event { body: 13 },
                Some(vec![TestEvent::Event(13)]),
            ),
            (DownlinkNotification::Synced, Some(vec![TestEvent::Synced])),
        ],
    )
    .await;

    clean_shutdown(context, agent, true).await;
}

#[tokio::test]
async fn event_dl_emit_unlinked_handler() {
    let mut context = make_hosted_input(SimpleDownlinkConfig::default());

    let agent = FakeAgent;

    expect_write_stopped(&mut context.channel, &agent).await;

    run_with_expectations(
        &mut context,
        &agent,
        vec![
            (DownlinkNotification::Linked, Some(vec![TestEvent::Linked])),
            (
                DownlinkNotification::Unlinked,
                Some(vec![TestEvent::Unlinked]),
            ),
        ],
    )
    .await;

    context.stop_tx = None;

    clean_shutdown(context, agent, false).await;
}

#[tokio::test]
async fn event_dl_revive_unlinked_downlink() {
    let config = SimpleDownlinkConfig {
        events_when_not_synced: true,
        terminate_on_unlinked: false,
    };

    let mut context = make_hosted_input(config);

    let agent = FakeAgent;

    expect_write_stopped(&mut context.channel, &agent).await;

    run_with_expectations(
        &mut context,
        &agent,
        vec![
            (DownlinkNotification::Linked, Some(vec![TestEvent::Linked])),
            (
                DownlinkNotification::Event { body: 13 },
                Some(vec![TestEvent::Event(13)]),
            ),
            (DownlinkNotification::Synced, Some(vec![TestEvent::Synced])),
            (
                DownlinkNotification::Unlinked,
                Some(vec![TestEvent::Unlinked]),
            ),
            (DownlinkNotification::Linked, Some(vec![TestEvent::Linked])),
            (
                DownlinkNotification::Event { body: 27 },
                Some(vec![TestEvent::Event(27)]),
            ),
            (DownlinkNotification::Synced, Some(vec![TestEvent::Synced])),
        ],
    )
    .await;

    clean_shutdown(context, agent, true).await;
}
