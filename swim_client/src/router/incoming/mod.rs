// Copyright 2015-2021 SWIM.AI inc.
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

use std::collections::HashMap;

use crate::router::{CloseReceiver, CloseResponseSender, RouterEvent, SubscriberRequest};
use futures::future::ready;
use futures::stream::FuturesUnordered;
use futures::{FutureExt, StreamExt};
use std::convert::TryFrom;
use swim_recon::parser::parse_value;
use swim_common::routing::ws::WsMessage;
use swim_common::routing::RoutingError;
use swim_common::warp::envelope::Envelope;
use swim_common::warp::path::RelativePath;
use tokio::sync::mpsc;
use tracing::level_filters::STATIC_MAX_LEVEL;
use tracing::{debug, error, span, trace, warn, Level};

//-------------------------------Connection Pool to Downlink------------------------------------

/// Tasks that the incoming task can handle.
#[derive(Debug)]
pub(crate) enum IncomingRequest {
    Connection(mpsc::Receiver<WsMessage>),
    Subscribe(SubscriberRequest),
    Message(WsMessage),
    Unreachable(String),
    Disconnect,
    Close(Option<CloseResponseSender>),
}

/// The incoming task is responsible for routing messages coming from remote hosts to
/// its subscribers (typically downlinks). A single incoming task is responsible for a single
/// remote host. The subscribers and connections are independent and adding a subscriber *WILL NOT*
/// automatically create a connection. Additionally, if a connection error occurs, all subscribers
/// will be notified, and if the error is non-fatal a new connection may be provided to the task.
pub(crate) struct IncomingHostTask {
    task_rx: mpsc::Receiver<IncomingRequest>,
    close_rx: CloseReceiver,
}

impl IncomingHostTask {
    pub fn new(
        close_rx: CloseReceiver,
        buffer_size: usize,
    ) -> (IncomingHostTask, mpsc::Sender<IncomingRequest>) {
        let (task_tx, task_rx) = mpsc::channel(buffer_size);

        (IncomingHostTask { task_rx, close_rx }, task_tx)
    }

    pub async fn run(self) -> Result<(), RoutingError> {
        let IncomingHostTask {
            mut task_rx,
            close_rx,
        } = self;

        let mut subscribers: HashMap<RelativePath, Vec<mpsc::Sender<RouterEvent>>> = HashMap::new();
        let mut connection: Option<mpsc::Receiver<WsMessage>> = None;

        let mut close_trigger = close_rx.fuse();

        loop {
            let task = if let Some(message_rx) = connection.as_mut() {
                let result = tokio::select! {
                    closed = &mut close_trigger => {
                        match closed {
                            Ok(tx) => Some(IncomingRequest::Close(Some((*tx).clone()))),
                            _ => Some(IncomingRequest::Close(None)),
                        }
                    },
                    task = task_rx.recv() => task,
                    maybe_message = message_rx.recv() => {
                        match maybe_message{
                            Some(message) => Some(IncomingRequest::Message(message)),
                            None => Some(IncomingRequest::Disconnect),
                        }
                    }

                };

                result.ok_or(RoutingError::ConnectionError)?
            } else {
                let result = tokio::select! {
                    closed = &mut close_trigger => {
                        match closed {
                            Ok(tx) => Some(IncomingRequest::Close(Some((*tx).clone()))),
                            _ => Some(IncomingRequest::Close(None)),
                        }
                    },
                    task = task_rx.recv() => task,
                };
                result.ok_or(RoutingError::ConnectionError)?
            };

            let span = span!(Level::TRACE, "incoming_event");
            let _enter = span.enter();

            if Some(Level::DEBUG) <= STATIC_MAX_LEVEL.into_level() {
                debug!("Received request: {:?}", task);
            }

            match task {
                IncomingRequest::Connection(message_rx) => {
                    connection = Some(message_rx);
                }

                IncomingRequest::Subscribe(SubscriberRequest {
                    path: relative_path,
                    subscriber_tx: event_tx,
                    ..
                }) => {
                    subscribers
                        .entry(relative_path)
                        .or_insert_with(Vec::new)
                        .push(event_tx);
                }

                IncomingRequest::Message(message) => {
                    let value = {
                        match &message {
                            WsMessage::Text(s) => parse_value(s),
                            m => {
                                error!("Unimplemented message type received: {:?}", m);
                                continue;
                            }
                        }
                    };

                    match value {
                        Ok(val) => {
                            let envelope = Envelope::try_from(val);

                            match envelope {
                                Ok(env) => {
                                    let message = env.into_incoming();

                                    if let Ok(incoming) = message {
                                        broadcast_destination(
                                            &mut subscribers,
                                            incoming.path.clone(),
                                            RouterEvent::Message(incoming),
                                        )
                                        .await?;
                                    } else {
                                        warn!("Unsupported message: {:?}", message)
                                    }
                                }
                                Err(e) => {
                                    error!("Parsing error {:?}", e);
                                }
                            }
                        }
                        Err(e) => {
                            error!("Parsing error {:?}", e);
                        }
                    }
                }

                IncomingRequest::Unreachable(err) => {
                    trace!("Unreachable Host");
                    broadcast_all(&mut subscribers, RouterEvent::Unreachable(err.to_string()))
                        .await?;

                    break Ok(());
                }

                IncomingRequest::Disconnect => {
                    trace!("Connection closed");
                    connection = None;

                    let unreachable =
                        broadcast_all(&mut subscribers, RouterEvent::ConnectionClosed).await?;
                    remove_unreachable(&mut subscribers, unreachable)?;
                }

                IncomingRequest::Close(close_rx) => {
                    if close_rx.is_some() {
                        trace!("Closing Router");
                        broadcast_all(&mut subscribers, RouterEvent::Stopping).await?;

                        break Ok(());
                    }
                }
            }
            trace!("Completed incoming request")
        }
    }
}

/// Broadcasts an event to all subscribers of the task.
///
/// # Arguments
///
/// * `subscribers`             - A map of all subscribers.
/// * `event`                   - An event to be broadcasted.
async fn broadcast_all(
    subscribers: &mut HashMap<RelativePath, Vec<mpsc::Sender<RouterEvent>>>,
    event: RouterEvent,
) -> Result<Vec<(RelativePath, usize)>, RoutingError> {
    if subscribers.len() == 1 {
        let (dest, dest_subs) = subscribers
            .iter_mut()
            .next()
            .ok_or(RoutingError::ConnectionError)?;

        if dest_subs.len() == 1 {
            let result = dest_subs
                .get_mut(0)
                .ok_or(RoutingError::ConnectionError)?
                .send(event)
                .await;

            return if result.is_err() {
                Ok(vec![(dest.clone(), 0)])
            } else {
                Ok(vec![])
            };
        }
    }

    let futures = FuturesUnordered::new();

    for (dest, dest_subs) in subscribers {
        for (index, sender) in dest_subs.iter_mut().enumerate() {
            futures.push(index_path_sender(
                sender,
                event.clone(),
                dest.clone(),
                index,
            ))
        }
    }

    let results = futures.filter_map(ready).collect::<Vec<_>>().await;

    Ok(results)
}

/// Broadcasts an event to all subscribers of the task that are subscribed to a given path.
/// The path is the combination of the node and lane.
///
/// # Arguments
///
/// * `subscribers`             - A map of all subscribers.
/// * `destination`             - The node and lane.
/// * `event`                   - An event to be broadcasted.
async fn broadcast_destination(
    subscribers: &mut HashMap<RelativePath, Vec<mpsc::Sender<RouterEvent>>>,
    destination: RelativePath,
    event: RouterEvent,
) -> Result<(), RoutingError> {
    if subscribers.contains_key(&destination) {
        let destination_subs = subscribers
            .get_mut(&destination)
            .ok_or(RoutingError::ConnectionError)?;

        if destination_subs.len() == 1 {
            let result = destination_subs
                .get_mut(0)
                .ok_or(RoutingError::ConnectionError)?
                .send(event)
                .await;

            if result.is_err() {
                destination_subs.remove(0);
            }
        } else {
            let futures: FuturesUnordered<_> = destination_subs
                .iter_mut()
                .enumerate()
                .map(|(index, sender)| index_sender(sender, event.clone(), index))
                .collect();

            for index in futures.filter_map(ready).collect::<Vec<_>>().await {
                destination_subs.remove(index);
            }
        }
    } else {
        trace!("No downlink interested in event: {:?}", event);
    };

    Ok(())
}

fn remove_unreachable(
    subscribers: &mut HashMap<RelativePath, Vec<mpsc::Sender<RouterEvent>>>,
    unreachable: Vec<(RelativePath, usize)>,
) -> Result<(), RoutingError> {
    for (path, index) in unreachable {
        let subs_dest = subscribers
            .get_mut(&path)
            .ok_or(RoutingError::ConnectionError)?;

        subs_dest.remove(index);

        if subs_dest.is_empty() {
            subscribers.remove(&path);
        }
    }

    Ok(())
}

async fn index_sender(
    sender: &mut mpsc::Sender<RouterEvent>,
    event: RouterEvent,
    index: usize,
) -> Option<usize> {
    if sender.send(event).await.is_err() {
        Some(index)
    } else {
        None
    }
}

async fn index_path_sender<'a>(
    sender: &mut mpsc::Sender<RouterEvent>,
    event: RouterEvent,
    path: RelativePath,
    index: usize,
) -> Option<(RelativePath, usize)> {
    if sender.send(event).await.is_err() {
        Some((path, index))
    } else {
        None
    }
}
