// Copyright 2015-2020 SWIM.AI inc.
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

use crate::agent::context::AgentExecutionContext;
use crate::agent::lane::channels::task::LaneIoError;
use crate::agent::lane::channels::uplink::spawn::UplinkErrorReport;
use crate::agent::lane::channels::AgentExecutionConfig;
use crate::agent::{AttachError, LaneIo};
use crate::routing::{TaggedClientEnvelope, TaggedEnvelope};
use either::Either;
use futures::future::{join, FusedFuture};
use futures::stream::FuturesUnordered;
use futures::task::{Context, Poll};
use futures::{ready, select, select_biased, FutureExt};
use futures::{Stream, StreamExt};
use pin_utils::core_reexport::fmt::Formatter;
use pin_utils::pin_mut;
use std::borrow::Borrow;
use std::collections::hash_map::Entry;
use std::collections::{HashMap, VecDeque};
use std::error::Error;
use std::fmt::Display;
use std::future::Future;
use std::hash::Hash;
use std::pin::Pin;
use swim_common::warp::envelope::OutgoingLinkMessage;
use swim_common::warp::path::RelativePath;
use tokio::sync::mpsc::error::TrySendError;
use tokio::sync::{mpsc, oneshot};
use utilities::sync::trigger;

pub struct EnvelopeDispatcher<Context> {
    agent_route: String,
    config: AgentExecutionConfig,
    context: Context,
    lanes: HashMap<String, Box<dyn LaneIo<Context>>>,
}

struct OpenRequest {
    name: String,
    callback: oneshot::Sender<mpsc::Sender<TaggedClientEnvelope>>,
}

impl OpenRequest {
    fn new(name: String, callback: oneshot::Sender<mpsc::Sender<TaggedClientEnvelope>>) -> Self {
        OpenRequest { name, callback }
    }
}

#[derive(Debug)]
pub enum DispatcherError {
    AttachmentFailed(AttachError),
    LaneTaskFailed(LaneIoError),
}

impl Display for DispatcherError {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            DispatcherError::AttachmentFailed(err) => write!(f, "{}", err),
            DispatcherError::LaneTaskFailed(err) => write!(f, "{}", err),
        }
    }
}

impl Error for DispatcherError {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        match self {
            DispatcherError::AttachmentFailed(err) => Some(err),
            DispatcherError::LaneTaskFailed(err) => Some(err),
        }
    }
}

impl<Context> EnvelopeDispatcher<Context>
where
    Context: AgentExecutionContext + Clone + Send + Sync + 'static,
{
    pub fn new(
        agent_route: String,
        config: AgentExecutionConfig,
        context: Context,
        lanes: HashMap<String, Box<dyn LaneIo<Context>>>,
    ) -> Self {
        EnvelopeDispatcher {
            agent_route,
            config,
            context,
            lanes,
        }
    }

    pub async fn run(
        self,
        incoming: impl Stream<Item = TaggedEnvelope>,
    ) -> Result<(), DispatcherError> {
        let EnvelopeDispatcher {
            agent_route,
            config,
            context,
            mut lanes,
        } = self;

        let (open_tx, open_rx) = mpsc::channel::<OpenRequest>(1);

        let open_config = config.clone();
        let open_context: Context = context.clone();

        let (tripwire_tx, tripwire_rx) = trigger::trigger();

        let open_task = async move {
            let _tripwire = tripwire_tx;

            let mut lane_io_tasks = FuturesUnordered::new();

            let requests = open_rx.fuse();
            pin_mut!(requests);

            loop {
                let next: Option<Either<OpenRequest, Result<Vec<UplinkErrorReport>, LaneIoError>>> =
                    if lane_io_tasks.is_empty() {
                        requests.next().await.map(Either::Left)
                    } else {
                        select! {
                            request = requests.next() => request.map(Either::Left),
                            completed = lane_io_tasks.next() => completed.map(Either::Right),
                        }
                    };

                match next {
                    Some(Either::Left(OpenRequest { name, callback })) => {
                        if let Some(lane_io) = lanes.remove(&name) {
                            let (lane_tx, lane_rx) = mpsc::channel(5);
                            let route = RelativePath::new(agent_route.as_str(), name.as_str());
                            let task_result = lane_io.attach_boxed(
                                route,
                                lane_rx,
                                open_config.clone(),
                                open_context.clone(),
                            );
                            match task_result {
                                Ok(task) => {
                                    lane_io_tasks.push(task);
                                    if callback.send(lane_tx).is_err() {
                                        //TODO Log error.
                                    }
                                }
                                Err(e) => {
                                    //TODO Log error.
                                    break Err(DispatcherError::AttachmentFailed(e));
                                }
                            }
                        }
                    }
                    Some(Either::Right(Err(lane_io_err))) => {
                        break Err(DispatcherError::LaneTaskFailed(lane_io_err))
                    }
                    _ => {
                        break Ok(());
                    }
                }
            }
        };

        let mut dispatcher = Dispatcher::new(config.max_concurrency, open_tx);
        let dispatch_task = dispatcher.dispatch_envelopes(incoming.take_until(tripwire_rx));

        let (result, succeeded) = join(open_task, dispatch_task).await;

        if succeeded {
            dispatcher.flush().await;
        }

        result
    }
}

struct ReadyFutureInner<T, L> {
    sender: mpsc::Sender<T>,
    label: L,
}

struct ReadyFuture<T, L> {
    inner: Option<ReadyFutureInner<T, L>>,
}

impl<T, L> ReadyFuture<T, L> {
    fn new(label: L, sender: mpsc::Sender<T>) -> Self {
        ReadyFuture {
            inner: Some(ReadyFutureInner { label, sender }),
        }
    }
}

impl<T: Unpin, L: Unpin> Future for ReadyFuture<T, L> {
    type Output = (L, Result<mpsc::Sender<T>, ()>);

    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let ReadyFutureInner { sender, .. } = self
            .as_mut()
            .get_mut()
            .inner
            .as_mut()
            .expect("Ready future polled twice.");
        let result = ready!(sender.poll_ready(cx));
        let ReadyFutureInner { sender, label } = match self.get_mut().inner.take() {
            Some(inner) => inner,
            _ => unreachable!(),
        };
        Poll::Ready((label, result.map(|_| sender).map_err(|_| ())))
    }
}

struct AwaitNewLaneInner<T, L> {
    rx: oneshot::Receiver<mpsc::Sender<T>>,
    label: L,
}

struct AwaitNewLane<T, L> {
    inner: Option<AwaitNewLaneInner<T, L>>,
}

impl<T, L> AwaitNewLane<T, L> {
    fn new(label: L, rx: oneshot::Receiver<mpsc::Sender<T>>) -> Self {
        AwaitNewLane {
            inner: Some(AwaitNewLaneInner { rx, label }),
        }
    }
}

impl<T: Unpin, L: Unpin> Future for AwaitNewLane<T, L> {
    type Output = (L, Result<mpsc::Sender<T>, ()>);

    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let AwaitNewLaneInner { rx, .. } = self
            .as_mut()
            .get_mut()
            .inner
            .as_mut()
            .expect("Await new lane future polled twice.");
        let result = ready!(rx.poll_unpin(cx));
        let AwaitNewLaneInner { label, .. } = match self.get_mut().inner.take() {
            Some(inner) => inner,
            _ => unreachable!(),
        };
        Poll::Ready((label, result.map_err(|_| ())))
    }
}

struct Selector<T, L>(FuturesUnordered<ReadyFuture<T, L>>);

impl<T, L> Default for Selector<T, L> {
    fn default() -> Self {
        Selector(FuturesUnordered::default())
    }
}

type SelectResult<T, L> = Option<(L, Result<mpsc::Sender<T>, ()>)>;

impl<T, L> Selector<T, L>
where
    T: Send + Unpin,
    L: Send + Unpin,
{
    fn is_empty(&self) -> bool {
        let Selector(inner) = self;
        inner.is_empty()
    }

    fn add(&mut self, label: L, sender: mpsc::Sender<T>) {
        let Selector(inner) = self;
        inner.push(ReadyFuture::new(label, sender));
    }

    fn select<'a>(&'a mut self) -> impl FusedFuture<Output = SelectResult<T, L>> + Send + 'a {
        let Selector(inner) = self;
        inner.next()
    }
}

struct Dispatcher {
    selector: Selector<TaggedClientEnvelope, String>,
    idle_senders: HashMap<String, Option<mpsc::Sender<TaggedClientEnvelope>>>,
    open_tx: mpsc::Sender<OpenRequest>,
    await_new: FuturesUnordered<AwaitNewLane<TaggedClientEnvelope, String>>,
    pending: PendingEnvelopes,
    stalled: Option<(String, TaggedClientEnvelope)>,
}

#[derive(Debug)]
struct PendingEnvelopes {
    max_pending: usize,
    num_pending: usize,
    pending: HashMap<String, VecDeque<TaggedClientEnvelope>>,
    queue_store: Vec<VecDeque<TaggedClientEnvelope>>,
}

impl PendingEnvelopes {
    fn new(max_pending: usize) -> Self {
        PendingEnvelopes {
            max_pending,
            num_pending: 0,
            pending: Default::default(),
            queue_store: vec![],
        }
    }

    fn enqueue(
        &mut self,
        lane: String,
        envelope: TaggedClientEnvelope,
    ) -> Result<(), (String, TaggedClientEnvelope)> {
        self.push(lane, envelope, false)
    }

    fn replace(
        &mut self,
        lane: String,
        envelope: TaggedClientEnvelope,
    ) -> Result<(), (String, TaggedClientEnvelope)> {
        self.push(lane, envelope, true)
    }

    fn push(
        &mut self,
        lane: String,
        envelope: TaggedClientEnvelope,
        front: bool,
    ) -> Result<(), (String, TaggedClientEnvelope)> {
        let PendingEnvelopes {
            max_pending,
            num_pending,
            pending,
            queue_store,
        } = self;
        if *num_pending < *max_pending {
            let queue = match pending.entry(lane) {
                Entry::Occupied(entry) => entry.into_mut(),
                Entry::Vacant(entry) => {
                    entry.insert(queue_store.pop().unwrap_or_else(VecDeque::new))
                }
            };
            if front {
                queue.push_front(envelope);
            } else {
                queue.push_back(envelope);
            }
            *num_pending += 1;
            Ok(())
        } else {
            Err((lane, envelope))
        }
    }

    fn pop<Q>(&mut self, lane: &Q) -> Option<TaggedClientEnvelope>
    where
        String: Borrow<Q>,
        Q: Hash + Eq,
    {
        let PendingEnvelopes {
            num_pending,
            pending,
            queue_store,
            ..
        } = self;
        let (envelope, cleared_queue) = if let Some(queue) = pending.get_mut(lane) {
            let envelope = queue.pop_front();
            (envelope, queue.is_empty())
        } else {
            (None, false)
        };
        if cleared_queue {
            if let Some(queue) = pending.remove(lane) {
                queue_store.push(queue);
            }
        }
        if envelope.is_some() {
            *num_pending -= 1;
        }
        envelope
    }
}

const GUARANTEED_CAPACITY: &str = "Inserting pending with guaranteed capacity should succeed.";

fn lane(env: &OutgoingLinkMessage) -> &str {
    env.path.lane.as_str()
}

impl Dispatcher {
    fn new(max_concurrency: usize, open_tx: mpsc::Sender<OpenRequest>) -> Self {
        Dispatcher {
            selector: Default::default(),
            idle_senders: Default::default(),
            open_tx,
            await_new: Default::default(),
            pending: PendingEnvelopes::new(max_concurrency),
            stalled: None,
        }
    }

    async fn dispatch_envelopes(&mut self, envelopes: impl Stream<Item = TaggedEnvelope>) -> bool {
        let Dispatcher {
            selector,
            idle_senders,
            open_tx,
            await_new,
            pending,
            stalled,
        } = self;

        let envelopes = envelopes.fuse();
        pin_mut!(envelopes);

        loop {
            let next = if stalled.is_some() {
                select! {
                    ready = selector.select() => ready.map(Either::Left),
                    ready = await_new.next() => ready.map(Either::Left),
                }
            } else if selector.is_empty() {
                envelopes.next().await.map(Either::Right)
            } else {
                select_biased! {
                    ready = selector.select() => ready.map(Either::Left),
                    ready = await_new.next() => ready.map(Either::Left),
                    envelope = envelopes.next() => envelope.map(Either::Right),
                }
            };

            match next {
                Some(Either::Left((label, Ok(mut sender)))) => {
                    let succeeded = loop {
                        if let Some(envelope) = pending.pop(&label) {
                            match sender.try_send(envelope) {
                                Err(TrySendError::Full(envelope)) => {
                                    pending
                                        .replace(label.clone(), envelope)
                                        .expect(GUARANTEED_CAPACITY);
                                    selector.add(label, sender);
                                    break true;
                                }
                                Err(TrySendError::Closed(_)) => {
                                    break false;
                                }
                                _ => {}
                            }
                        } else {
                            idle_senders.insert(label, Some(sender));
                            break true;
                        }
                    };
                    if !succeeded {
                        break false;
                    }
                    if let Some((label, envelope)) = stalled.take() {
                        if let Err((label, envelope)) = pending.enqueue(label, envelope) {
                            *stalled = Some((label, envelope));
                        }
                    }
                }
                Some(Either::Left((_, Err(_)))) => {
                    break false;
                }
                Some(Either::Right(TaggedEnvelope(addr, envelope))) => {
                    if let Ok(envelope) = envelope.into_outgoing() {
                        if let Some(entry) = idle_senders.get_mut(lane(&envelope)) {
                            let maybe_pending = if let Some(mut sender) = entry.take() {
                                match sender.try_send(TaggedClientEnvelope(addr, envelope)) {
                                    Err(TrySendError::Full(envelope)) => {
                                        selector.add(envelope.lane().to_string(), sender);
                                        Some(envelope)
                                    }
                                    Err(TrySendError::Closed(_)) => {
                                        break false;
                                    }
                                    _ => {
                                        *entry = Some(sender);
                                        None
                                    }
                                }
                            } else {
                                Some(TaggedClientEnvelope(addr, envelope))
                            };

                            if let Some(envelope) = maybe_pending {
                                if let Err((label, envelope)) =
                                    pending.enqueue(envelope.lane().to_string(), envelope)
                                {
                                    *stalled = Some((label, envelope));
                                }
                            }
                        } else {
                            let (req_tx, req_rx) = oneshot::channel();

                            if open_tx
                                .send(OpenRequest::new(lane(&envelope).to_string(), req_tx))
                                .await
                                .is_err()
                            {
                                break false;
                            }
                            await_new.push(AwaitNewLane::new(lane(&envelope).to_string(), req_rx));
                            if let Err((label, envelope)) = pending.enqueue(
                                lane(&envelope).to_string(),
                                TaggedClientEnvelope(addr, envelope),
                            ) {
                                *stalled = Some((label, envelope));
                            }
                        }
                    }
                }
                _ => {
                    break true;
                }
            }
        }
    }

    async fn flush(self) {
        let Dispatcher {
            mut selector,
            mut await_new,
            mut pending,
            mut stalled,
            ..
        } = self;

        let mut new_terminated = false;

        loop {
            let next = if new_terminated {
                selector.select().await
            } else {
                select! {
                  ready = selector.select() => ready,
                  ready = await_new.next() => {
                       match ready {
                          present@Some(_) => present,
                          _ => {
                            new_terminated = true;
                            continue;
                          }
                       }
                  },
                }
            };
            if let Some((label, Ok(mut sender))) = next {
                while let Some(envelope) = pending.pop(&label) {
                    if let Err(TrySendError::Full(envelope)) = sender.try_send(envelope) {
                        pending
                            .replace(label.clone(), envelope)
                            .expect(GUARANTEED_CAPACITY);
                        selector.add(label, sender);
                        break;
                    }
                }
                if let Some((label, envelope)) = stalled.take() {
                    if let Err((label, envelope)) = pending.enqueue(label, envelope) {
                        stalled = Some((label, envelope));
                    }
                }
            } else {
                break;
            }
        }
    }
}
