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

use std::collections::HashMap;

use crate::router::{
    CloseReceiver, CloseResponseSender, RouterEvent, RoutingError, SubscriberRequest,
};
use common::model::parser::parse_single;
use common::warp::envelope::Envelope;
use common::warp::path::RelativePath;
use futures::stream;
use futures::stream::FuturesUnordered;
use futures::stream::StreamExt;
use std::convert::TryFrom;
use tokio::sync::mpsc;
use tokio_tungstenite::tungstenite::protocol::Message;
use tracing::{error, span, trace, warn, Level};

//-------------------------------Connection Pool to Downlink------------------------------------

#[derive(Debug)]
pub enum IncomingRequest {
    Connection(mpsc::Receiver<Message>),
    Subscribe(SubscriberRequest),
    Message(Message),
    Unreachable(String),
    Disconnect,
    Close(Option<CloseResponseSender>),
}

pub struct IncomingHostTask {
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
        let IncomingHostTask { task_rx, close_rx } = self;

        let mut subscribers: HashMap<RelativePath, Vec<mpsc::Sender<RouterEvent>>> = HashMap::new();
        let mut connection: Option<mpsc::Receiver<Message>> = None;

        let mut rx = combine_incoming_streams(task_rx, close_rx);

        loop {
            let task = if let Some(message_rx) = connection.as_mut() {
                let result = tokio::select! {

                    task = rx.next() => {
                        task
                    }

                    maybe_message = message_rx.recv() => {
                        match maybe_message{
                            Some(message) => Some(IncomingRequest::Message(message)),
                            None => Some(IncomingRequest::Disconnect),
                        }
                    }

                };

                result.ok_or(RoutingError::ConnectionError)?
            } else {
                rx.next().await.ok_or(RoutingError::ConnectionError)?
            };

            let span = span!(Level::TRACE, "incoming_event");
            let _enter = span.enter();
            trace!("{:?}", task);

            match task {
                IncomingRequest::Connection(message_rx) => {
                    connection = Some(message_rx);
                }

                IncomingRequest::Subscribe(SubscriberRequest {
                    path: relative_path,
                    subscriber_tx: event_tx,
                }) => {
                    subscribers
                        .entry(relative_path)
                        .or_insert_with(Vec::new)
                        .push(event_tx);
                }

                IncomingRequest::Message(message) => {
                    let message = message
                        .to_text()
                        .map_err(|_| RoutingError::ConnectionError)?;

                    let value = parse_single(message);

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
                    drop(rx);
                    trace!("Unreachable Host");
                    broadcast_all(&mut subscribers, RouterEvent::Unreachable(err.to_string()))
                        .await?;

                    break Ok(());
                }

                IncomingRequest::Disconnect => {
                    trace!("Connection closed");
                    connection = None;

                    broadcast_all(&mut subscribers, RouterEvent::ConnectionClosed).await?;
                }

                IncomingRequest::Close(Some(_)) => {
                    drop(rx);
                    trace!("Closing Router");
                    broadcast_all(&mut subscribers, RouterEvent::Stopping).await?;

                    break Ok(());
                }

                IncomingRequest::Close(None) => { /*NO OP*/ }
            }
            trace!("Completed incoming request")
        }
    }
}

async fn broadcast_all(
    subscribers: &mut HashMap<RelativePath, Vec<mpsc::Sender<RouterEvent>>>,
    event: RouterEvent,
) -> Result<(), RoutingError> {
    let futures = FuturesUnordered::new();

    subscribers
        .iter_mut()
        .flat_map(|(_, dest)| dest)
        .for_each(|subscriber| futures.push(subscriber.send(event.clone())));

    for result in futures.collect::<Vec<_>>().await {
        result?
    }

    Ok(())
}

async fn broadcast_destination(
    subscribers: &mut HashMap<RelativePath, Vec<mpsc::Sender<RouterEvent>>>,
    destination: RelativePath,
    event: RouterEvent,
) -> Result<(), RoutingError> {
    let futures = FuturesUnordered::new();

    if subscribers.contains_key(&destination) {
        let destination_subs = subscribers
            .get_mut(&destination)
            .ok_or(RoutingError::ConnectionError)?;

        for subscriber in destination_subs.iter_mut() {
            futures.push(subscriber.send(event.clone()));
        }
    } else {
        trace!("No downlink interested in event: {:?}", event);
    };

    for result in futures.collect::<Vec<_>>().await {
        result?
    }

    Ok(())
}

fn combine_incoming_streams(
    task_rx: mpsc::Receiver<IncomingRequest>,
    close_rx: CloseReceiver,
) -> impl stream::Stream<Item = IncomingRequest> + Send + 'static {
    let close_requests = close_rx.map(IncomingRequest::Close);
    stream::select(task_rx, close_requests)
}
