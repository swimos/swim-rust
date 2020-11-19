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
use crate::agent::lane::channels::task::{LaneUplinks, UplinkChannels};
use crate::agent::lane::channels::uplink::{
    Uplink, UplinkAction, UplinkError, UplinkMessageSender,
};
use crate::agent::lane::channels::{
    AgentExecutionConfig, LaneMessageHandler, OutputMessage, TaggedAction,
};
use crate::agent::Eff;
use crate::routing::{RoutingAddr, ServerRouter};
use futures::future::join_all;
use futures::{FutureExt, StreamExt};
use std::collections::hash_map::Entry;
use std::collections::HashMap;
use std::fmt::{Display, Formatter};
use std::num::NonZeroUsize;
use std::sync::Arc;
use swim_common::model::Value;
use swim_common::topic::Topic;
use swim_common::warp::path::RelativePath;
use tokio::sync::mpsc;
use tracing::{event, span, Level};
use tracing_futures::Instrument;
use utilities::sync::trigger;

#[cfg(test)]
mod tests;

const FAILED_ERR_REPORT: &str = "Failed to send error report.";
const UPLINK_TERMINATED: &str = "An uplink terminated uncleanly.";
const NEW_UPLINK: &str = "Creating new uplink.";
const UPLINK_CLEANUP: &str = "Uplink cleanup task.";
const UPLINK_TASK: &str = "Uplink task.";

/// Creates lane uplinks on demand, replacing them if they fail and reporting any errors that
/// occur.
///
/// #Type Parameters
///
/// * `Handler` - Type of the [`LaneMessageHandler`] for the lane, used to create the uplink state
/// machines,
/// * `Top` - Type of the [`Topic`] allowing to uplinks to subscribe to the even stream of the lane.
pub struct UplinkSpawner<Handler, Top> {
    handler: Arc<Handler>,
    topic: Top,
    actions: mpsc::Receiver<TaggedAction>,
    action_buffer_size: NonZeroUsize,
    max_start_attempts: NonZeroUsize,
    route: RelativePath,
}

impl<Handler, Top> UplinkSpawner<Handler, Top>
where
    Handler: LaneMessageHandler,
    OutputMessage<Handler>: Into<Value>,
    Top: Topic<Handler::Event> + Send,
{
    /// Crate a new uplink spawner.
    ///
    /// #Arguments
    ///
    /// * `handler` - [`LaneMessageHandler`] implementation to create the uplink state machines.
    /// * `topic` - Topic with which the uplinks can subscribe to the lane event stream.
    /// * `rx` - The stream of incoming uplink actions (link, sync requests etc.).
    /// * `action_buffer_size` - Size of the action queue for each uplink.
    /// * `max_start_attempts` - The maximum number of times the spawner will attempt to create a
    /// new uplink before giving up.
    /// * `route` - The route of the lane (for labelling outgoing envelopes).
    ///
    pub fn new(
        handler: Arc<Handler>,
        topic: Top,
        rx: mpsc::Receiver<TaggedAction>,
        action_buffer_size: NonZeroUsize,
        max_start_attempts: NonZeroUsize,
        route: RelativePath,
    ) -> Self {
        UplinkSpawner {
            handler,
            topic,
            actions: rx,
            action_buffer_size,
            max_start_attempts,
            route,
        }
    }

    /// Run the uplink spawner as an async task.
    ///
    /// #Arguments
    ///
    /// * `router` - Produces channels on which outgoing envelopes can be sent.
    /// * `spawn_tx` - Channel to an asynchronous tasks spawner (used to run the uplink state
    /// machines.
    /// * `error_collector` - Collects errors whenever an uplink fails.
    ///
    /// #Type Paramameters
    ///
    /// * `Router` - The type of the server router.
    pub async fn run<Router>(
        mut self,
        mut router: Router,
        mut spawn_tx: mpsc::Sender<Eff>,
        mut error_collector: mpsc::Sender<UplinkErrorReport>,
    ) where
        Router: ServerRouter,
    {
        let mut uplink_senders: HashMap<RoutingAddr, UplinkHandle> = HashMap::new();

        while let Some(TaggedAction(addr, mut action)) = self.actions.recv().await {
            let mut attempts = 0;
            let is_done = loop {
                let sender = match uplink_senders.entry(addr) {
                    Entry::Occupied(entry) => Some(entry.into_mut()),
                    Entry::Vacant(entry) => {
                        let span =
                            span!(Level::TRACE, NEW_UPLINK, lane = ?self.route, endpoint = ?addr);
                        if let Some(handle) = self
                            .make_uplink(addr, error_collector.clone(), &mut spawn_tx, &mut router)
                            .instrument(span)
                            .await
                        {
                            Some(entry.insert(handle))
                        } else {
                            None
                        }
                    }
                };
                if let Some(sender) = sender {
                    if let Err(mpsc::error::SendError(act)) = sender.send(action).await {
                        if let Some(handle) = uplink_senders.remove(&addr) {
                            if !handle.cleanup().await {
                                event!(Level::ERROR, message = UPLINK_TERMINATED, route = ?&self.route, ?addr);
                            }
                        }
                        action = act;
                        attempts += 1;
                        if attempts >= self.max_start_attempts.get() {
                            let report =
                                UplinkErrorReport::new(UplinkError::FailedToStart(attempts), addr);
                            if let Err(mpsc::error::SendError(report)) =
                                error_collector.send(report).await
                            {
                                event!(Level::ERROR, message = FAILED_ERR_REPORT, ?report);
                            }
                            //The uplink is unstable so we stop trying to open it but do not
                            //necessarily stop overall.
                            break false;
                        }
                    } else {
                        // We successfully dispatched to the uplink so can continue.
                        break false;
                    }
                } else {
                    //Successfully created the uplink so we can stop.
                    break true;
                }
            };
            if is_done {
                break;
            }
        }
        join_all(uplink_senders.into_iter().map(|(_, h)| h.cleanup()))
            .instrument(span!(Level::DEBUG, UPLINK_CLEANUP))
            .await;
    }

    //Create a new uplink state machine and attach it to the router
    async fn make_uplink<Router>(
        &mut self,
        addr: RoutingAddr,
        mut err_tx: mpsc::Sender<UplinkErrorReport>,
        spawn_tx: &mut mpsc::Sender<Eff>,
        router: &mut Router,
    ) -> Option<UplinkHandle>
    where
        Router: ServerRouter,
    {
        let UplinkSpawner {
            handler,
            topic,
            action_buffer_size,
            route,
            ..
        } = self;
        let (tx, rx) = mpsc::channel(action_buffer_size.get());
        let (cleanup_tx, cleanup_rx) = trigger::trigger();
        let state_machine = handler.make_uplink(addr);
        let updates = if let Ok(sub) = topic.subscribe().await {
            sub.fuse()
        } else {
            return None;
        };
        let uplink = Uplink::new(state_machine, rx.fuse(), updates);

        let sink = if let Ok(sender) = router.resolve_sender(addr).await {
            UplinkMessageSender::new(sender.sender, route.clone())
        } else {
            return None;
        };
        let ul_task = async move {
            if let Err(err) = uplink.run_uplink(sink).await {
                let report = UplinkErrorReport::new(err, addr);
                if let Err(mpsc::error::SendError(report)) = err_tx.send(report).await {
                    event!(Level::ERROR, message = FAILED_ERR_REPORT, ?report);
                }
            } else {
                cleanup_tx.trigger();
            }
        }
        .instrument(span!(Level::INFO, UPLINK_TASK, ?route, ?addr));
        if spawn_tx.send(ul_task.boxed()).await.is_err() {
            return None;
        }
        Some(UplinkHandle::new(tx, cleanup_rx))
    }
}

/// Handle on an uplink state machine that is held by the spawner.
struct UplinkHandle {
    /// Channel used to send external actions to the uplink.
    sender: mpsc::Sender<UplinkAction>,
    /// Triggered when all cleanup is complete for the uplink. If the uplink fails the send end of
    /// this will be dropped, rather than triggered, allowing the cases to be distinguished
    wait_on_cleanup: trigger::Receiver,
}

impl UplinkHandle {
    fn new(sender: mpsc::Sender<UplinkAction>, wait_on_cleanup: trigger::Receiver) -> Self {
        UplinkHandle {
            sender,
            wait_on_cleanup,
        }
    }

    async fn send(
        &mut self,
        action: UplinkAction,
    ) -> Result<(), mpsc::error::SendError<UplinkAction>> {
        self.sender.send(action).await
    }

    /// Stop the uplink cleanly.
    async fn cleanup(self) -> bool {
        let UplinkHandle {
            sender,
            wait_on_cleanup,
        } = self;
        // Dropping the sender will cause the uplink to begin shutting down.
        drop(sender);
        // Wait for the shutdown process to complete.
        wait_on_cleanup.await.is_ok()
    }
}

/// An error report, generated when an uplink fails, specifying the reason for the failure and the
/// endpoint to which the uplink was attached.
#[derive(Debug)]
pub struct UplinkErrorReport {
    pub error: UplinkError,
    pub addr: RoutingAddr,
}

impl UplinkErrorReport {
    pub(crate) fn new(error: UplinkError, addr: RoutingAddr) -> Self {
        UplinkErrorReport { error, addr }
    }
}

impl Display for UplinkErrorReport {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "Uplink to {} failed: {}", &self.addr, &self.error)
    }
}

/// Default spawner factory, using [`UplinkSpawner`].
pub(crate) struct SpawnerUplinkFactory(AgentExecutionConfig);

impl SpawnerUplinkFactory {
    pub(crate) fn new(config: AgentExecutionConfig) -> Self {
        SpawnerUplinkFactory(config)
    }
}

impl LaneUplinks for SpawnerUplinkFactory {
    fn make_task<Handler, Top, Context>(
        &self,
        message_handler: Arc<Handler>,
        channels: UplinkChannels<Top>,
        route: RelativePath,
        context: &Context,
    ) -> Eff
    where
        Handler: LaneMessageHandler + 'static,
        OutputMessage<Handler>: Into<Value>,
        Top: Topic<Handler::Event> + Send + 'static,
        Context: AgentExecutionContext,
    {
        let SpawnerUplinkFactory(AgentExecutionConfig {
            action_buffer,
            max_uplink_start_attempts,
            ..
        }) = self;

        let UplinkChannels {
            events,
            actions,
            error_collector,
        } = channels;

        let spawner = UplinkSpawner::new(
            message_handler,
            events,
            actions,
            *action_buffer,
            *max_uplink_start_attempts,
            route,
        );

        spawner
            .run(context.router_handle(), context.spawner(), error_collector)
            .boxed()
    }
}
