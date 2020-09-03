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

mod pending;
mod selector;
#[cfg(test)]
mod tests;

use crate::agent::context::AgentExecutionContext;
use crate::agent::lane::channels::task::LaneIoError;
use crate::agent::lane::channels::AgentExecutionConfig;
use crate::agent::{AttachError, LaneIo};
use crate::routing::{TaggedClientEnvelope, TaggedEnvelope};
use either::Either;
use futures::future::join;
use futures::stream::FuturesUnordered;
use futures::task::{Context, Poll};
use futures::{ready, select, select_biased, FutureExt};
use futures::{Stream, StreamExt};
use pin_utils::core_reexport::fmt::Formatter;
use pin_utils::pin_mut;
use std::collections::HashMap;
use std::error::Error;
use std::fmt::Display;
use std::future::Future;
use std::pin::Pin;
use swim_common::warp::envelope::OutgoingLinkMessage;
use swim_common::warp::path::RelativePath;
use tokio::sync::mpsc::error::TrySendError;
use tokio::sync::{mpsc, oneshot, watch};
use tracing::{event, span, Level};
use tracing_futures::Instrument;
use utilities::sync::trigger;
use swim_common::sink::item::ItemSink;

pub struct AgentDispatcher<Context> {
    agent_route: String,
    config: AgentExecutionConfig,
    context: Context,
    lanes: HashMap<String, Box<dyn LaneIo<Context>>>,
    stalled_tx: watch::Sender<bool>,
    stalled_rx: watch::Receiver<bool>,
}

struct OpenRequest {
    name: String,
    callback: oneshot::Sender<Result<mpsc::Sender<TaggedClientEnvelope>, AttachError>>,
}

impl OpenRequest {
    fn new(
        name: String,
        callback: oneshot::Sender<Result<mpsc::Sender<TaggedClientEnvelope>, AttachError>>,
    ) -> Self {
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

impl<Context> AgentDispatcher<Context>
where
    Context: AgentExecutionContext + Clone + Send + Sync + 'static,
{
    pub fn new(
        agent_route: String,
        config: AgentExecutionConfig,
        context: Context,
        lanes: HashMap<String, Box<dyn LaneIo<Context>>>,
    ) -> Self {
        let (stalled_tx, stalled_rx) = watch::channel(false);
        AgentDispatcher {
            agent_route,
            config,
            context,
            lanes,
            stalled_tx,
            stalled_rx
        }
    }

    pub fn stall_watcher(&self) -> watch::Receiver<bool> {
        self.stalled_rx.clone()
    }

    pub async fn run(
        self,
        incoming: impl Stream<Item = TaggedEnvelope>,
    ) -> Result<(), DispatcherError> {
        let AgentDispatcher {
            agent_route,
            config,
            context,
            lanes,
            stalled_tx,
            ..
        } = self;

        let span = span!(Level::INFO, "Agent envelope dispatcher task.", ?agent_route);
        let _enter = span.enter();

        let (open_tx, open_rx) = mpsc::channel(config.lane_attachment_buffer.get());

        let (tripwire_tx, tripwire_rx) = trigger::trigger();

        let attacher = LaneAttachmentTask::new(agent_route, lanes, &config, context);
        let open_task = attacher
            .run(open_rx, tripwire_tx)
            .instrument(span!(Level::INFO, "Lane IO opener task."));

        let mut dispatcher = EnvelopeDispatcher::new(config.max_pending_envelopes, open_tx, stalled_tx);

        let dispatch_task = async move {
            let succeeded = dispatcher
                .dispatch_envelopes(incoming.take_until(tripwire_rx))
                .await;
            if succeeded {
                dispatcher
                    .flush()
                    .instrument(span!(Level::INFO, "Envelope dispatcher flush task."))
                    .await;
            }
        }
        .instrument(span!(Level::INFO, "Envelope dispatcher task."));

        let (result, _) = join(open_task, dispatch_task).await;

        result
    }
}

struct LaneAttachmentTask<'a, Context> {
    agent_route: String,
    lanes: HashMap<String, Box<dyn LaneIo<Context>>>,
    config: &'a AgentExecutionConfig,
    context: Context,
}

impl<'a, Context> LaneAttachmentTask<'a, Context>
where
    Context: AgentExecutionContext + Clone + Send + Sync + 'static,
{
    fn new(
        agent_route: String,
        lanes: HashMap<String, Box<dyn LaneIo<Context>>>,
        config: &'a AgentExecutionConfig,
        context: Context,
    ) -> Self {
        LaneAttachmentTask {
            agent_route,
            config,
            lanes,
            context,
        }
    }

    async fn run(
        self,
        requests: mpsc::Receiver<OpenRequest>,
        _tripwire: trigger::Sender,
    ) -> Result<(), DispatcherError> {
        let LaneAttachmentTask {
            agent_route,
            mut lanes,
            config,
            context,
        } = self;

        let mut lane_io_tasks = FuturesUnordered::new();

        let requests = requests.fuse();
        pin_mut!(requests);

        loop {
            let next = if lane_io_tasks.is_empty() {
                requests.next().await.map(Either::Left)
            } else {
                select! {
                    request = requests.next() => request.map(Either::Left),
                    completed = lane_io_tasks.next() => completed.map(Either::Right),
                }
            };

            match next {
                Some(Either::Left(OpenRequest { name, callback })) => {
                    event!(
                        Level::DEBUG,
                        message = "Attachment requested for lane.",
                        ?name
                    );
                    if let Some(lane_io) = lanes.remove(&name) {
                        let (lane_tx, lane_rx) = mpsc::channel(config.lane_buffer.get());
                        let route = RelativePath::new(agent_route.as_str(), name.as_str());
                        let task_result =
                            lane_io.attach_boxed(route, lane_rx, config.clone(), context.clone());
                        match task_result {
                            Ok(task) => {
                                lane_io_tasks.push(task);
                                if callback.send(Ok(lane_tx)).is_err() {
                                    event!(Level::ERROR, message = BAD_CALLBACK, ?name);
                                }
                            }
                            Err(error) => {
                                event!(
                                    Level::ERROR,
                                    message = "Attaching to a lane failed.",
                                    ?name,
                                    ?error
                                );
                                if callback.send(Err(error.clone())).is_err() {
                                    event!(Level::ERROR, message = BAD_CALLBACK, ?name);
                                }
                                break Err(DispatcherError::AttachmentFailed(error));
                            }
                        }
                    } else {
                        if callback
                            .send(Err(AttachError::LaneDoesNotExist(name.clone())))
                            .is_err()
                        {
                            event!(Level::ERROR, message = BAD_CALLBACK, ?name);
                        }
                    }
                }
                Some(Either::Right(Err(lane_io_err))) => {
                    event!(Level::ERROR, message = "Lane IO task failed.", error = ?lane_io_err);
                    event!(Level::ERROR, message = "Lane IO task failed.", error = ?lane_io_err);
                    break Err(DispatcherError::LaneTaskFailed(lane_io_err));
                }
                _ => {
                    break Ok(());
                }
            }
        }
    }
}

struct AwaitNewLaneInner<T, L> {
    rx: oneshot::Receiver<Result<mpsc::Sender<T>, AttachError>>,
    label: L,
}

struct AwaitNewLane<T, L> {
    inner: Option<AwaitNewLaneInner<T, L>>,
}

impl<T, L> AwaitNewLane<T, L> {
    fn new(label: L, rx: oneshot::Receiver<Result<mpsc::Sender<T>, AttachError>>) -> Self {
        AwaitNewLane {
            inner: Some(AwaitNewLaneInner { rx, label }),
        }
    }
}

impl<T: Unpin, L: Unpin> Future for AwaitNewLane<T, L> {
    type Output = (L, Result<mpsc::Sender<T>, Option<AttachError>>);

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
        Poll::Ready((
            label,
            match result {
                Ok(Ok(s)) => Ok(s),
                Ok(Err(e)) => Err(Some(e)),
                Err(_) => Err(Some(AttachError::AgentStopping)),
            },
        ))
    }
}

struct EnvelopeDispatcher {
    selector: selector::Selector<TaggedClientEnvelope, String>,
    idle_senders: HashMap<String, Option<mpsc::Sender<TaggedClientEnvelope>>>,
    open_tx: mpsc::Sender<OpenRequest>,
    await_new: FuturesUnordered<AwaitNewLane<TaggedClientEnvelope, String>>,
    pending: pending::PendingEnvelopes,
    stalled: Option<(String, TaggedClientEnvelope)>,
    stalled_tx: watch::Sender<bool>,
}

const GUARANTEED_CAPACITY: &str = "Inserting pending with guaranteed capacity should succeed.";
const BAD_CALLBACK: &str = "Could not send input channel to the envelope dispatcher.";

fn lane(env: &OutgoingLinkMessage) -> &str {
    env.path.lane.as_str()
}

fn convert_select_err<T>(
    result: Option<(String, Result<T, ()>)>) -> Option<(String, Result<T, Option<AttachError>>)> {
    match result {
        Some((label, Ok(t))) => Some((label, Ok(t))),
        Some((label, Err(_))) => Some((label, Err(None))),
        None => None,
    }
}

impl EnvelopeDispatcher {
    fn new(max_concurrency: usize,
           open_tx: mpsc::Sender<OpenRequest>,
           stalled_tx: watch::Sender<bool>) -> Self {
        EnvelopeDispatcher {
            selector: Default::default(),
            idle_senders: Default::default(),
            open_tx,
            await_new: Default::default(),
            pending: pending::PendingEnvelopes::new(max_concurrency),
            stalled: None,
            stalled_tx,
        }
    }

    async fn dispatch_envelopes(&mut self, envelopes: impl Stream<Item = TaggedEnvelope>) -> bool {
        let EnvelopeDispatcher {
            selector,
            idle_senders,
            open_tx,
            await_new,
            pending,
            stalled,
            stalled_tx,
        } = self;

        let envelopes = envelopes.fuse();
        pin_mut!(envelopes);

        //let mut i = 0;

        loop {
            let next = if stalled.is_some() {
                select! {
                    ready = selector.select() => convert_select_err(ready).map(Either::Left),
                    ready = await_new.next() => ready.map(Either::Left),
                }
            } else if selector.is_empty() && await_new.is_empty() {
                envelopes.next().await.map(Either::Right)
            } else {
                select_biased! {
                    ready = selector.select() => convert_select_err(ready).map(Either::Left),
                    ready = await_new.next() => ready.map(Either::Left),
                    envelope = envelopes.next() => envelope.map(Either::Right),
                }
            };

            /*if i == 4 {
                panic!("{:?}", next);
            } else {
                i += 1;
            }*/

            match next {
                Some(Either::Left((label, Ok(mut sender)))) => {
                    event!(
                        Level::DEBUG,
                        message = "Sender selected for dispatch.",
                        ?label
                    );
                    let succeeded = loop {
                        if let Some(envelope) = pending.pop(&label) {
                            match sender.try_send(envelope) {
                                Err(TrySendError::Full(envelope)) => {
                                    event!(
                                        Level::TRACE,
                                        message = "Returning sender to selector.",
                                        ?label
                                    );
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
                            event!(
                                Level::TRACE,
                                message = "Returning sender to the idle map.",
                                ?label
                            );
                            idle_senders.insert(label, Some(sender));
                            break true;
                        }
                    };
                    if !succeeded {
                        break false;
                    }
                    if let Some((label, envelope)) = stalled.take() {
                        if let Err((label, envelope)) = pending.enqueue(label, envelope) {
                            event!(Level::TRACE, message = "Stall was not resolved.");
                            *stalled = Some((label, envelope));
                        } else {
                            event!(Level::TRACE, message = "Dispatcher no longer stalled.");
                            let _ = stalled_tx.broadcast(false);
                        }
                    }
                }
                Some(Either::Left((_, Err(err)))) => {
                    match err {
                        Some(AttachError::LaneDoesNotExist(label)) => {
                            pending.clear(&label);
                        },
                        _ => {
                            break false;
                        }
                    }
                }
                Some(Either::Right(TaggedEnvelope(addr, envelope))) => {
                    event!(
                        Level::TRACE,
                        message = "Attempting to dispatch envelope.",
                        ?envelope
                    );
                    if let Ok(envelope) = envelope.into_outgoing() {
                        if let Some(entry) = idle_senders.get_mut(lane(&envelope)) {
                            let maybe_pending = if let Some(mut sender) = entry.take() {
                                match sender.try_send(TaggedClientEnvelope(addr, envelope)) {
                                    Err(TrySendError::Full(envelope)) => {
                                        event!(
                                            Level::TRACE,
                                            message = "Lane busy.",
                                            ?envelope,
                                            lane = envelope.lane()
                                        );
                                        selector.add(envelope.lane().to_string(), sender);
                                        Some(envelope)
                                    }
                                    Err(TrySendError::Closed(_)) => {
                                        break false;
                                    }
                                    _ => {
                                        event!(
                                            Level::TRACE,
                                            message = "Envelope dispatched successfully."
                                        );
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
                                    event!(Level::TRACE, message = "Dispatcher has stalled.");
                                    let _ = stalled_tx.send_item(true);
                                    *stalled = Some((label, envelope));
                                }
                            }
                        } else {
                            event!(
                                Level::TRACE,
                                message = "Requesting lane to be attached for envelope.",
                                ?envelope
                            );
                            let (req_tx, req_rx) = oneshot::channel();

                            let label = lane(&envelope).to_string();

                            if open_tx
                                .send(OpenRequest::new(label.clone(), req_tx))
                                .await
                                .is_err()
                            {
                                break false;
                            }
                            await_new.push(AwaitNewLane::new(label.clone(), req_rx));
                            idle_senders.insert(label.clone(), None);
                            if let Err((label, envelope)) = pending.enqueue(
                                label,
                                TaggedClientEnvelope(addr, envelope),
                            ) {
                                event!(Level::TRACE, message = "Dispatcher has stalled.");
                                let _ = stalled_tx.broadcast(true);
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
        let EnvelopeDispatcher {
            mut selector,
            mut await_new,
            mut pending,
            mut stalled,
            mut stalled_tx,
            ..
        } = self;

        let mut new_terminated = await_new.is_empty();

        loop {
            let next = if new_terminated {
                convert_select_err(selector.select().await)
            } else {
                select! {
                  ready = selector.select() => convert_select_err(ready),
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
            match next {
                Some((label, Ok(mut sender))) => {
                    event!(
                    Level::DEBUG,
                    message = "Sender selected for dispatch.",
                    ?label
                );
                    while let Some(envelope) = pending.pop(&label) {
                        if let Err(TrySendError::Full(envelope)) = sender.try_send(envelope) {
                            event!(
                            Level::TRACE,
                            message = "Returning sender to selector.",
                            ?label
                        );
                            pending
                                .replace(label.clone(), envelope)
                                .expect(GUARANTEED_CAPACITY);
                            selector.add(label, sender);
                            break;
                        }
                    }
                    if let Some((label, envelope)) = stalled.take() {
                        if let Err((label, envelope)) = pending.enqueue(label, envelope) {
                            event!(Level::TRACE, message = "Stall was not resolved.");
                            stalled = Some((label, envelope));
                        } else {
                            event!(Level::TRACE, message = "Dispatcher no longer stalled.");
                            let _ = stalled_tx.send_item(false);
                        }
                    }
                },
                Some((_, Err(Some(AttachError::LaneDoesNotExist(_))))) => {},
                _ => {
                    break;
                }
            }
        }
    }
}
