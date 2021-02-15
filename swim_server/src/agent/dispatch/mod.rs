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

pub mod error;
#[cfg(test)]
mod tests;

use crate::agent::context::AgentExecutionContext;
use crate::agent::dispatch::error::{DispatcherError, DispatcherErrors};
use crate::agent::lane::channels::task::LaneIoError;
use crate::agent::lane::channels::uplink::spawn::UplinkErrorReport;
use crate::agent::lane::channels::AgentExecutionConfig;
use crate::agent::{AttachError, LaneIo};
use crate::routing::{LaneIdentifier, TaggedClientEnvelope, TaggedEnvelope};
use either::Either;
use futures::future::{join, BoxFuture};
use futures::stream::{FusedStream, FuturesUnordered};
use futures::task::{Context, Poll};
use futures::{ready, select_biased, FutureExt};
use futures::{Stream, StreamExt};
use pin_utils::pin_mut;
use std::collections::HashMap;
use std::future::Future;
use std::num::NonZeroUsize;
use std::pin::Pin;
use swim_common::warp::envelope::OutgoingLinkMessage;
use swim_common::warp::path::RelativePath;
use tokio::sync::{mpsc, oneshot};
use tokio_stream::wrappers::ReceiverStream;
use tracing::{event, span, Level};
use tracing_futures::Instrument;
use utilities::errors::Recoverable;
use utilities::sync::trigger;
use utilities::uri::RelativeUri;

/// A collection of interlinked tasks that forwards incoming
/// [`swim_common::warp::envelope::Envelope`]s to the lanes of an agent and routes
/// [`swim_common::warp::envelope::Envelope`]s generated by uplinks to those lanes.
pub struct AgentDispatcher<Context> {
    agent_route: RelativeUri,
    config: AgentExecutionConfig,
    context: Context,
    lanes: HashMap<LaneIdentifier, Box<dyn LaneIo<Context>>>,
}

//A request to attach a lane to the dispatcher.
struct OpenRequest {
    identifier: LaneIdentifier,
    rx: mpsc::Receiver<TaggedClientEnvelope>,
    callback: oneshot::Sender<Result<(), AttachError>>,
}

impl OpenRequest {
    fn new(
        identifier: LaneIdentifier,
        rx: mpsc::Receiver<TaggedClientEnvelope>,
        callback: oneshot::Sender<Result<(), AttachError>>,
    ) -> Self {
        OpenRequest {
            identifier,
            rx,
            callback,
        }
    }
}

const LANE_ATTACH_TASK: &str = "Lane IO attachment task.";
const DISPATCHER_FLUSH_TASK: &str = "Envelope dispatcher flush task.";
const INTERNAL_DISPATCH_TASK: &str = "Internal envelope dispatcher task.";

impl<Context> AgentDispatcher<Context>
where
    Context: AgentExecutionContext + Send + Sync + Clone + Send + Sync + 'static,
{
    /// Create a dispatcher for an agent.
    ///
    /// #Arguments
    /// * `agent_route` - The name of the route for the agent.
    /// * `config` - Configuration parameters for the dispatcher.
    /// * `context` - The agent execution cotext for routing outgoing
    /// [`swim_common::warp::envelope::Envelope`]s and scheduling tasks.
    /// * `lanes` - The lanes of the agent, initially unattached to the dispatcher.
    pub fn new(
        agent_route: RelativeUri,
        config: AgentExecutionConfig,
        context: Context,
        lanes: HashMap<LaneIdentifier, Box<dyn LaneIo<Context>>>,
    ) -> Self {
        AgentDispatcher {
            agent_route,
            config,
            context,
            lanes,
        }
    }

    /// Run the dispatcher task.
    ///
    /// # Arguments
    /// * `incoming` - The stream of incoming [`swim_common::warp::envelope::Envelope`]s to the
    /// agent.
    pub async fn run(
        self,
        incoming: impl Stream<Item = TaggedEnvelope>,
    ) -> Result<DispatcherErrors, DispatcherErrors> {
        let AgentDispatcher {
            agent_route,
            config,
            context,
            lanes,
            ..
        } = self;

        let (open_tx, open_rx) = mpsc::channel(config.lane_attachment_buffer.get());

        let (tripwire_tx, tripwire_rx) = trigger::trigger();

        let attacher = LaneAttachmentTask::new(agent_route, lanes, &config, context);
        let open_task = attacher
            .run(open_rx, tripwire_tx)
            .instrument(span!(Level::INFO, LANE_ATTACH_TASK));

        let mut dispatcher =
            EnvelopeDispatcher::new(open_tx, config.yield_after, config.lane_buffer);

        let dispatch_task = async move {
            let succeeded = dispatcher
                .dispatch_envelopes(incoming.take_until(tripwire_rx))
                .await;
            if succeeded {
                dispatcher
                    .flush()
                    .instrument(span!(Level::INFO, DISPATCHER_FLUSH_TASK))
                    .await;
            }
        }
        .instrument(span!(Level::INFO, INTERNAL_DISPATCH_TASK));

        let (result, _) = join(open_task, dispatch_task).await;

        result
    }
}

// A task that attaches the lanes to the dispatcher when the first envelope is routed to them.
struct LaneAttachmentTask<'a, Context> {
    agent_route: RelativeUri,
    lanes: HashMap<LaneIdentifier, Box<dyn LaneIo<Context>>>,
    config: &'a AgentExecutionConfig,
    context: Context,
}

enum LaneTaskEvent {
    Request(OpenRequest),
    LaneTaskSuccess(Vec<UplinkErrorReport>),
    LaneTaskFailure(LaneIoError),
}

type IoTaskResult = Result<Vec<UplinkErrorReport>, LaneIoError>;

async fn next_attachment_event(
    requests: &mut (impl FusedStream<Item = OpenRequest> + Unpin),
    lane_io_tasks: &mut FuturesUnordered<BoxFuture<'static, IoTaskResult>>,
) -> Option<LaneTaskEvent> {
    loop {
        if requests.is_terminated() && lane_io_tasks.is_empty() {
            break None;
        } else if lane_io_tasks.is_empty() {
            match requests.next().await {
                Some(req) => {
                    break Some(LaneTaskEvent::Request(req));
                }
                _ => {
                    break None;
                }
            }
        } else {
            select_biased! {
                completion = lane_io_tasks.next() => {
                    match completion {
                        Some(Ok(errs)) => {
                            break Some(LaneTaskEvent::LaneTaskSuccess(errs));
                        },
                        Some(Err(err)) => {
                            break Some(LaneTaskEvent::LaneTaskFailure(err));
                        },
                        _ => {}
                    }
                },
                maybe_request = requests.next() => {
                    if let Some(req) = maybe_request {
                        break Some(LaneTaskEvent::Request(req));
                    }
                }
            }
        }
    }
}

impl<'a, Context> LaneAttachmentTask<'a, Context>
where
    Context: AgentExecutionContext + Send + Sync + Clone + Send + Sync + 'static,
{
    fn new(
        agent_route: RelativeUri,
        lanes: HashMap<LaneIdentifier, Box<dyn LaneIo<Context>>>,
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
        tripwire: trigger::Sender,
    ) -> Result<DispatcherErrors, DispatcherErrors> {
        let LaneAttachmentTask {
            agent_route,
            mut lanes,
            config,
            context,
        } = self;

        let mut tripwire = Some(tripwire);

        let mut lane_io_tasks = FuturesUnordered::new();

        let requests = ReceiverStream::new(requests).fuse();
        pin_mut!(requests);

        let yield_mod = config.yield_after.get();
        let mut iteration_count: usize = 0;

        let mut errors = DispatcherErrors::new();

        loop {
            let next = next_attachment_event(&mut requests, &mut lane_io_tasks).await;

            match next {
                Some(LaneTaskEvent::Request(OpenRequest {
                    identifier,
                    rx: lane_rx,
                    callback,
                })) => {
                    event!(
                        Level::DEBUG,
                        message = "Attachment requested for lane.",
                        ?identifier
                    );
                    if let Some(lane_io) = lanes.remove(&identifier) {
                        let route = RelativePath::new(
                            agent_route.to_string(),
                            identifier.lane_uri().to_string(),
                        );
                        let task_result =
                            lane_io.attach_boxed(route, lane_rx, config.clone(), context.clone());
                        match task_result {
                            Ok(task) => {
                                lane_io_tasks.push(task);
                                if callback.send(Ok(())).is_err() {
                                    event!(Level::ERROR, message = BAD_CALLBACK, ?identifier);
                                }
                            }
                            Err(error) => {
                                event!(
                                    Level::ERROR,
                                    message = "Attaching to a lane failed.",
                                    ?identifier,
                                    ?error
                                );
                                if callback.send(Err(error.clone())).is_err() {
                                    event!(Level::ERROR, message = BAD_CALLBACK, ?identifier);
                                }
                                let dispatch_err = DispatcherError::AttachmentFailed(error);
                                if dispatch_err.is_fatal() {
                                    if let Some(tx) = tripwire.take() {
                                        tx.trigger();
                                    }
                                }
                                errors.push(dispatch_err);
                                break;
                            }
                        }
                    } else {
                        errors.push(DispatcherError::AttachmentFailed(
                            AttachError::LaneDoesNotExist(identifier.lane_uri().to_string()),
                        ));
                        if callback
                            .send(Err(AttachError::LaneDoesNotExist(
                                identifier.lane_uri().to_string(),
                            )))
                            .is_err()
                        {
                            event!(Level::ERROR, message = BAD_CALLBACK, ?identifier);
                        }
                    }
                }
                Some(LaneTaskEvent::LaneTaskFailure(lane_io_err)) => {
                    event!(Level::ERROR, message = "Lane IO task failed.", error = ?lane_io_err);
                    errors.push(DispatcherError::LaneTaskFailed(lane_io_err));
                    if let Some(tx) = tripwire.take() {
                        tx.trigger();
                    }
                    break;
                }
                Some(LaneTaskEvent::LaneTaskSuccess(uplink_errors)) => {
                    event!(
                        Level::DEBUG,
                        message = "Lane task completed successfully.",
                        ?uplink_errors
                    );
                }
                _ => {
                    break;
                }
            }
            iteration_count = iteration_count.wrapping_add(1);
            if iteration_count % yield_mod == 0 {
                tokio::task::yield_now().await;
            }
        }
        if errors.is_fatal() {
            Err(errors)
        } else {
            Ok(errors)
        }
    }
}

struct AwaitNewLaneInner<L> {
    rx: oneshot::Receiver<Result<(), AttachError>>,
    label: L,
}

struct AwaitNewLane<L> {
    inner: Option<AwaitNewLaneInner<L>>,
}

impl<L> AwaitNewLane<L> {
    fn new(label: L, rx: oneshot::Receiver<Result<(), AttachError>>) -> Self {
        AwaitNewLane {
            inner: Some(AwaitNewLaneInner { rx, label }),
        }
    }
}

impl<L: Unpin> Future for AwaitNewLane<L> {
    type Output = (L, Result<(), AttachError>);

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
                Ok(Ok(_)) => Ok(()),
                Ok(Err(e)) => Err(e),
                Err(_) => Err(AttachError::AgentStopping),
            },
        ))
    }
}

struct EnvelopeDispatcher {
    senders: HashMap<String, mpsc::Sender<TaggedClientEnvelope>>,
    open_tx: mpsc::Sender<OpenRequest>,
    await_new: FuturesUnordered<AwaitNewLane<String>>,
    yield_after: NonZeroUsize,
    lane_buffer: NonZeroUsize,
}

const BAD_CALLBACK: &str = "Could not send input channel to the envelope dispatcher.";
const SENDER_SELECTED: &str = "Sender selected for dispatch.";
const ATTEMPT_DISPATCH: &str = "Attempting to dispatch envelope.";
const REQUESTING_ATTACH: &str = "Requesting lane to be attached for envelope.";
const NON_EXISTENT_DROP: &str = "Lane does not exist; dropping pending messages.";
const FAILED_START_DROP: &str = "Lane IO task failed to start; dropping pending messages.";

fn lane(env: &OutgoingLinkMessage) -> &str {
    env.path.lane.as_str()
}

impl EnvelopeDispatcher {
    fn new(
        open_tx: mpsc::Sender<OpenRequest>,
        yield_after: NonZeroUsize,
        lane_buffer: NonZeroUsize,
    ) -> Self {
        EnvelopeDispatcher {
            senders: Default::default(),
            open_tx,
            await_new: Default::default(),
            yield_after,
            lane_buffer,
        }
    }

    async fn dispatch_envelopes(&mut self, envelopes: impl Stream<Item = TaggedEnvelope>) -> bool {
        let EnvelopeDispatcher {
            senders,
            open_tx,
            await_new,
            yield_after,
            lane_buffer,
        } = self;

        let envelopes = envelopes.fuse();
        pin_mut!(envelopes);

        let yield_mod = yield_after.get();
        let mut iteration_count: usize = 0;

        loop {
            let next = select_next(await_new, &mut envelopes).await;

            match next {
                Some(Either::Left((label, Ok(_)))) => {
                    event!(Level::DEBUG, message = SENDER_SELECTED, ?label);
                }
                Some(Either::Left((name, Err(err)))) => {
                    senders.remove(&name);
                    if !matches!(err, AttachError::LaneDoesNotExist(_)) {
                        break false;
                    }
                }
                Some(Either::Right(request)) => {
                    event!(Level::TRACE, message = ATTEMPT_DISPATCH, ?request);

                    if let Ok((addr, envelope, identifier)) = request.split_outgoing() {
                        if let Some(sender) = senders.get_mut(lane(&envelope)) {
                            if sender
                                .send(TaggedClientEnvelope(addr, envelope))
                                .await
                                .is_err()
                            {
                                break false;
                            }
                        } else {
                            event!(Level::TRACE, message = REQUESTING_ATTACH, ?envelope);
                            let (req_tx, req_rx) = oneshot::channel();
                            let (uplink_tx, uplink_rx) = mpsc::channel(lane_buffer.get());
                            let label = lane(&envelope).to_string();

                            if open_tx
                                .send(OpenRequest::new(identifier, uplink_rx, req_tx))
                                .await
                                .is_err()
                            {
                                break false;
                            }
                            await_new.push(AwaitNewLane::new(label.clone(), req_rx));
                            if uplink_tx
                                .send(TaggedClientEnvelope(addr, envelope))
                                .await
                                .is_err()
                            {
                                break false;
                            }
                            senders.insert(label.clone(), uplink_tx);
                        }
                    }
                }
                _ => {
                    break true;
                }
            }

            iteration_count = iteration_count.wrapping_add(1);
            if iteration_count % yield_mod == 0 {
                tokio::task::yield_now().await;
            }
        }
    }

    async fn flush(self) {
        let EnvelopeDispatcher {
            mut senders,
            mut await_new,
            yield_after,
            ..
        } = self;

        let yield_mod = yield_after.get();
        let mut iteration_count: usize = 0;
        loop {
            match await_new.next().await {
                Some((label, Ok(_))) => {
                    event!(Level::DEBUG, message = SENDER_SELECTED, ?label);
                }
                Some((label, Err(e))) => {
                    senders.remove(&label);
                    match e {
                        AttachError::LaneDoesNotExist(name) => {
                            event!(Level::WARN, message = NON_EXISTENT_DROP, ?name);
                        }
                        error => {
                            event!(Level::WARN, message = FAILED_START_DROP, ?error);
                        }
                    }
                }
                _ => {
                    break;
                }
            }
            iteration_count = iteration_count.wrapping_add(1);
            if iteration_count % yield_mod == 0 {
                tokio::task::yield_now().await;
            }
        }
    }
}

type LabelledResult<L> = (L, Result<(), AttachError>);

async fn select_next<L>(
    await_new: &mut FuturesUnordered<AwaitNewLane<L>>,
    envelopes: &mut (impl FusedStream<Item = TaggedEnvelope> + Unpin),
) -> Option<Either<LabelledResult<L>, TaggedEnvelope>>
where
    L: Send + Unpin + 'static,
{
    if await_new.is_empty() {
        envelopes.next().await.map(Either::Right)
    } else {
        select_biased! {
            new_sender = await_new.next() => new_sender.map(Either::Left),
            env = envelopes.next() => env.map(Either::Right),
        }
    }
}
