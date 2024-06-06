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
    num::NonZeroUsize,
    ops::Deref,
    pin::{pin, Pin},
    sync::{
        atomic::{AtomicBool, Ordering},
        Arc,
    },
    task::{Context, Poll, Waker},
};

use futures::{
    future::join3,
    task::{waker, ArcWake},
    Sink, SinkExt, Stream, StreamExt,
};
use parking_lot::Mutex;
use swimos_agent_protocol::encoding::downlink::{
    DownlinkNotificationEncoder, DownlinkOperationDecoder,
};
use swimos_agent_protocol::{DownlinkNotification, DownlinkOperation};
use swimos_api::address::Address;
use swimos_form::read::RecognizerReadable;
use swimos_model::Text;
use swimos_recon::{print_recon_compact, WithLenRecognizerDecoder};
use swimos_utilities::{
    byte_channel::{self, ByteReader, ByteWriter},
    circular_buffer, non_zero_usize, trigger,
};
use tokio::{io::AsyncWriteExt, task::yield_now};
use tokio_util::codec::{FramedRead, FramedWrite};

use super::{HostedValueDownlinkFactory, SimpleDownlinkConfig};
use crate::{
    agent_model::downlink::{
        handlers::{BoxDownlinkChannel, DownlinkChannelEvent},
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
enum TestEvent {
    Linked,
    Synced(i32),
    Event(i32),
    Set(Option<i32>, i32),
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

impl OnSynced<i32, FakeAgent> for FakeLifecycle {
    type OnSyncedHandler<'a> = BoxEventHandler<'a, FakeAgent>
    where
        Self: 'a;

    fn on_synced<'a>(&'a self, value: &i32) -> Self::OnSyncedHandler<'a> {
        let state = self.inner.clone();
        let n = *value;
        SideEffect::from(move || {
            state.lock().push(TestEvent::Synced(n));
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
            state.lock().push(TestEvent::Event(n));
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
            state.lock().push(TestEvent::Set(previous, n));
        })
        .boxed()
    }
}

type Events = Arc<Mutex<Vec<TestEvent>>>;
type State = RefCell<Option<i32>>;

const BUFFER_SIZE: NonZeroUsize = non_zero_usize!(4096);
const OUT_CHAN_SIZE: NonZeroUsize = non_zero_usize!(8);

struct TestContext {
    channel: BoxDownlinkChannel<FakeAgent>,
    events: Events,
    sender: FramedWrite<ByteWriter, DownlinkNotificationEncoder>,
    write_tx: Option<circular_buffer::Sender<i32>>,
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
        write_tx: Some(write_tx),
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
        out_rx: _out_rx,
        events: _events,
        write_tx: _write_tx,
        stop_tx: _stop_tx,
    } = make_hosted_input(&agent, SimpleDownlinkConfig::default());

    assert!(channel.next_event(&agent).is_none());

    drop(sender);

    let r = channel.await_ready().await;
    println!("{:?}", r);

    assert!(r.is_none());

    assert!(channel.next_event(&agent).is_none());
}

#[tokio::test]
async fn shutdown_on_stop_trigger() {
    let agent = FakeAgent;

    let TestContext {
        mut channel,
        sender: _sender,
        out_rx: _out_rx,
        events,
        write_tx: _write_tx,
        stop_tx,
    } = make_hosted_input(&agent, SimpleDownlinkConfig::default());

    assert!(channel.next_event(&agent).is_none());

    stop_tx.expect("Stop trigger missing.").trigger();

    assert!(channel.await_ready().await.is_none());
    assert!(channel.next_event(&agent).is_none());

    assert!(take_events(&events).is_empty());
}

#[tokio::test]
async fn terminate_on_error() {
    let agent = FakeAgent;
    let TestContext {
        mut channel,
        mut sender,
        out_rx: _out_rx,
        events,
        write_tx: _write_tx,
        stop_tx: _stop_tx,
    } = make_hosted_input(&agent, SimpleDownlinkConfig::default());

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

enum Instruction {
    Incoming {
        notification: DownlinkNotification<i32>,
        expected: Option<Vec<TestEvent>>,
    },
    Outgoing(i32),
    DropOutgoing,
}

fn incoming(
    notification: DownlinkNotification<i32>,
    expected: Option<Vec<TestEvent>>,
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
        write_tx,
        ..
    } = context;

    for instruction in instructions {
        match instruction {
            Instruction::Incoming {
                notification: not,
                expected,
            } => {
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
            Instruction::Outgoing(n) => {
                write_tx
                    .as_mut()
                    .expect("Output dropped.")
                    .try_send(n)
                    .expect("Channel dropped");
                assert!(matches!(
                    channel.await_ready().await,
                    Some(Ok(DownlinkChannelEvent::WriteCompleted))
                ));
            }
            Instruction::DropOutgoing => {
                *write_tx = None;
                assert!(matches!(
                    channel.await_ready().await,
                    Some(Ok(DownlinkChannelEvent::WriteStreamTerminated))
                ));
            }
        }
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
        assert_eq!(take_events(events), vec![TestEvent::Unlinked]);
    }

    assert!(channel.await_ready().await.is_none());
}

#[tokio::test]
async fn write_output() {
    let agent = FakeAgent;
    let mut context = make_hosted_input(&agent, SimpleDownlinkConfig::default());

    run_with_expectations(&mut context, &agent, vec![Instruction::Outgoing(5)]).await;

    clean_shutdown(&mut context, &agent, false).await;

    let TestContext { out_rx, .. } = &mut context;

    //Flush is not guaranteed until the next poll of channel after the write "completes"
    //so we only check that the value was written after the fact.
    let mut reader = FramedRead::new(
        out_rx,
        WithLenRecognizerDecoder::new(i32::make_recognizer()),
    );

    let output = reader.next().await;
    assert!(matches!(output, Some(Ok(5))));
}

#[tokio::test]
async fn write_terminated() {
    let agent = FakeAgent;
    let mut context = make_hosted_input(&agent, SimpleDownlinkConfig::default());

    run_with_expectations(&mut context, &agent, vec![Instruction::DropOutgoing]).await;

    clean_shutdown(&mut context, &agent, false).await;
}

#[tokio::test]
async fn emit_linked_handler() {
    let agent = FakeAgent;
    let mut context = make_hosted_input(&agent, SimpleDownlinkConfig::default());

    run_with_expectations(
        &mut context,
        &agent,
        vec![incoming(
            DownlinkNotification::Linked,
            Some(vec![TestEvent::Linked]),
        )],
    )
    .await;

    clean_shutdown(&mut context, &agent, true).await;
}

#[tokio::test]
async fn emit_synced_handler() {
    let agent = FakeAgent;
    let mut context = make_hosted_input(&agent, SimpleDownlinkConfig::default());

    run_with_expectations(
        &mut context,
        &agent,
        vec![
            incoming(DownlinkNotification::Linked, Some(vec![TestEvent::Linked])),
            incoming(DownlinkNotification::Event { body: 13 }, None),
            incoming(
                DownlinkNotification::Synced,
                Some(vec![TestEvent::Synced(13)]),
            ),
        ],
    )
    .await;

    clean_shutdown(&mut context, &agent, true).await;
}

#[tokio::test]
async fn emit_event_handlers() {
    let agent = FakeAgent;
    let mut context = make_hosted_input(&agent, SimpleDownlinkConfig::default());

    run_with_expectations(
        &mut context,
        &agent,
        vec![
            incoming(DownlinkNotification::Linked, Some(vec![TestEvent::Linked])),
            incoming(DownlinkNotification::Event { body: 13 }, None),
            incoming(
                DownlinkNotification::Synced,
                Some(vec![TestEvent::Synced(13)]),
            ),
            incoming(
                DownlinkNotification::Event { body: 15 },
                Some(vec![TestEvent::Event(15), TestEvent::Set(Some(13), 15)]),
            ),
        ],
    )
    .await;

    clean_shutdown(&mut context, &agent, true).await;
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
            incoming(DownlinkNotification::Linked, Some(vec![TestEvent::Linked])),
            incoming(
                DownlinkNotification::Event { body: 13 },
                Some(vec![TestEvent::Event(13), TestEvent::Set(None, 13)]),
            ),
            incoming(
                DownlinkNotification::Synced,
                Some(vec![TestEvent::Synced(13)]),
            ),
        ],
    )
    .await;

    clean_shutdown(&mut context, &agent, true).await;
}

#[tokio::test]
async fn emit_unlinked_handler() {
    let agent = FakeAgent;
    let mut context = make_hosted_input(&agent, SimpleDownlinkConfig::default());

    run_with_expectations(
        &mut context,
        &agent,
        vec![
            incoming(DownlinkNotification::Linked, Some(vec![TestEvent::Linked])),
            incoming(
                DownlinkNotification::Unlinked,
                Some(vec![TestEvent::Unlinked]),
            ),
        ],
    )
    .await;

    clean_shutdown(&mut context, &agent, false).await;
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
            incoming(DownlinkNotification::Linked, Some(vec![TestEvent::Linked])),
            incoming(
                DownlinkNotification::Event { body: 13 },
                Some(vec![TestEvent::Event(13), TestEvent::Set(None, 13)]),
            ),
            incoming(
                DownlinkNotification::Synced,
                Some(vec![TestEvent::Synced(13)]),
            ),
            incoming(
                DownlinkNotification::Unlinked,
                Some(vec![TestEvent::Unlinked]),
            ),
            incoming(DownlinkNotification::Linked, Some(vec![TestEvent::Linked])),
            incoming(
                DownlinkNotification::Event { body: 27 },
                Some(vec![TestEvent::Event(27), TestEvent::Set(None, 27)]),
            ),
            incoming(
                DownlinkNotification::Synced,
                Some(vec![TestEvent::Synced(27)]),
            ),
        ],
    )
    .await;

    clean_shutdown(&mut context, &agent, true).await;
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
    values: Vec<i32>,
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
            values: Default::default(),
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
            values: Default::default(),
            ready: false,
            flushed: false,
        }
    }
}

impl Sink<i32> for TestSink {
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

    fn start_send(self: Pin<&mut Self>, item: i32) -> Result<(), Self::Error> {
        let mut guard = self.get_mut().inner.lock();
        let TestSinkInner {
            state,
            ready,
            values,
            ..
        } = &mut *guard;
        assert!(*ready);
        *ready = false;
        if matches!(state, SinkState::FailOnSend | SinkState::Closed) {
            Err(std::io::Error::from(std::io::ErrorKind::BrokenPipe))
        } else {
            values.push(item);
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
    set_tx: Option<circular_buffer::Sender<i32>>,
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

    fn send(&mut self, n: i32) {
        self.set_tx
            .as_mut()
            .expect("Sender closed.")
            .try_send(n)
            .expect("Channel dropped.");
    }

    fn drop_sender(&mut self) {
        self.set_tx = None;
    }
}

fn init_write_test(
    sink: Option<TestSinkInner>,
) -> (WriteStreamContext, ValueWriteStream<i32, TestSink>) {
    let (set_tx, set_rx) = circular_buffer::channel::<i32>(non_zero_usize!(2));

    let inner = Arc::new(Mutex::new(sink.unwrap_or_default()));
    let sink = TestSink {
        inner: inner.clone(),
    };

    let stream = ValueWriteStream::with_sink(sink, set_rx);

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

    let values = [1, 2, 3];

    let mut expected = vec![];
    for n in values {
        context.send(n);
        expected.push(n);

        let poll = stream.as_mut().poll_next(&mut context.future_context());
        assert!(matches!(poll, Poll::Ready(Some(Ok(())))));
        assert!(!context.wake_state.was_woken());
        let TestSinkInner {
            state,
            values,
            ready,
            flushed,
            ..
        } = &*context.sink_data();
        assert_eq!(*state, SinkState::HasCapacity);
        assert!(!*ready);
        assert!(!*flushed);
        assert_eq!(values, &expected);
    }
}

#[test]
fn writer_data_available_no_capacity() {
    let (mut context, stream) = init_write_test(Some(TestSinkInner::full()));
    let mut stream = pin!(stream);

    context.send(1);

    assert!(stream
        .as_mut()
        .poll_next(&mut context.future_context())
        .is_pending());

    //Woken so we can potentially consume more.
    assert!(context.wake_state.was_woken());

    {
        let TestSinkInner {
            state,
            values,
            ready,
            flushed,
            ..
        } = &*context.sink_data();
        assert_eq!(*state, SinkState::Full);
        assert!(!*ready);
        assert!(!*flushed);
        assert!(values.is_empty());
    }
    //Free up capacity and try again.
    context.free_capacity();
    let poll = stream.poll_next(&mut context.future_context());
    assert!(matches!(poll, Poll::Ready(Some(Ok(_)))));

    let TestSinkInner {
        state,
        values,
        ready,
        flushed,
        ..
    } = &*context.sink_data();
    assert_eq!(*state, SinkState::HasCapacity);
    assert!(!*ready);
    assert!(!*flushed);
    assert_eq!(values, &[1]);
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
        values,
        ready,
        flushed,
        ..
    } = &*context.sink_data();
    assert_eq!(*state, SinkState::Closed);
    assert!(!*ready);
    assert!(!*flushed);
    assert!(values.is_empty());
}

#[test]
fn writer_stop_data_available() {
    let (mut context, stream) = init_write_test(None);
    let mut stream = pin!(stream);

    context.send(5);
    context.send(3);
    context.drop_sender();

    let poll = stream.as_mut().poll_next(&mut context.future_context());
    assert!(matches!(poll, Poll::Ready(Some(Ok(_)))));

    {
        let TestSinkInner {
            state,
            values,
            ready,
            flushed,
            ..
        } = &*context.sink_data();
        assert_eq!(*state, SinkState::HasCapacity);
        assert!(!*ready);
        assert!(!*flushed);
        assert_eq!(values, &[5]);
    }

    let poll = stream.as_mut().poll_next(&mut context.future_context());
    assert!(matches!(poll, Poll::Ready(Some(Ok(_)))));

    {
        let TestSinkInner {
            state,
            values,
            ready,
            flushed,
            ..
        } = &*context.sink_data();
        assert_eq!(*state, SinkState::HasCapacity);
        assert!(!*ready);
        assert!(!*flushed);
        assert_eq!(values, &[5, 3]);
    }

    let poll = stream.poll_next(&mut context.future_context());
    assert!(matches!(poll, Poll::Ready(None)));

    let TestSinkInner {
        state,
        values,
        ready,
        flushed,
        ..
    } = &*context.sink_data();
    assert_eq!(*state, SinkState::Closed);
    assert!(!*ready);
    assert!(!*flushed);
    assert_eq!(values, &[5, 3]);
}

#[test]
fn writer_stop_data_pending() {
    let (mut context, stream) = init_write_test(Some(TestSinkInner::full()));
    let mut stream = pin!(stream);

    context.send(5);

    let poll = stream.as_mut().poll_next(&mut context.future_context());
    assert!(poll.is_pending());

    {
        let TestSinkInner {
            state,
            values,
            ready,
            flushed,
            ..
        } = &*context.sink_data();
        assert_eq!(*state, SinkState::Full);
        assert!(!*ready);
        assert!(!*flushed);
        assert!(values.is_empty());
    }

    context.free_capacity();
    context.drop_sender();

    let poll = stream.as_mut().poll_next(&mut context.future_context());
    assert!(matches!(poll, Poll::Ready(Some(Ok(_)))));

    {
        let TestSinkInner {
            state,
            values,
            ready,
            flushed,
            ..
        } = &*context.sink_data();
        assert_eq!(*state, SinkState::HasCapacity);
        assert!(!*ready);
        assert!(!*flushed);
        assert_eq!(values, &[5]);
    }

    let poll = stream.poll_next(&mut context.future_context());
    assert!(matches!(poll, Poll::Ready(None)));

    let TestSinkInner {
        state,
        values,
        ready,
        flushed,
        ..
    } = &*context.sink_data();
    assert_eq!(*state, SinkState::Closed);
    assert!(!*ready);
    assert!(!*flushed);
    assert_eq!(values, &[5]);
}

#[test]
fn writer_fail_on_ready() {
    let (mut context, stream) =
        init_write_test(Some(TestSinkInner::with_state(SinkState::FailOnReady)));
    let mut stream = pin!(stream);
    context.send(56);

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
    context.send(56);

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
