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

use crate::router::{RouterEvent, RoutingError};
use common::model::parser::parse_single;
use common::warp::envelope::Envelope;
use common::warp::path::AbsolutePath;
use std::convert::TryFrom;
use tokio::sync::mpsc;
use tokio_tungstenite::tungstenite::protocol::Message;

//-------------------------------Connection Pool to Downlink------------------------------------

pub enum IncomingRequest {
    Connection(mpsc::Receiver<Message>),
    Subscribe((AbsolutePath, mpsc::Sender<RouterEvent>)),
    Message(Message),
    Unreachable,
    Disconnect,
    Close,
}

pub struct IncomingHostTask {
    task_rx: mpsc::Receiver<IncomingRequest>,
}

impl IncomingHostTask {
    pub fn new(buffer_size: usize) -> (IncomingHostTask, mpsc::Sender<IncomingRequest>) {
        let (task_tx, task_rx) = mpsc::channel(buffer_size);

        (IncomingHostTask { task_rx }, task_tx)
    }

    //Todo split into smaller methods
    pub async fn run(self) -> Result<(), RoutingError> {
        let IncomingHostTask { mut task_rx } = self;

        let mut subscribers: HashMap<String, Vec<mpsc::Sender<RouterEvent>>> = HashMap::new();
        let mut connection = None;

        loop {
            if connection.is_none() {
                let task = task_rx.recv().await.ok_or(RoutingError::ConnectionError)?;

                match task {
                    IncomingRequest::Connection(message_rx) => {
                        connection = Some(message_rx);
                    }

                    IncomingRequest::Subscribe((target, event_tx)) => {
                        subscribers
                            .entry(target.destination())
                            .or_insert_with(Vec::new)
                            .push(event_tx);
                    }

                    IncomingRequest::Unreachable => {
                        println!("Unreachable Host");
                        for (_, destination) in subscribers.iter_mut() {
                            for subscriber in destination {
                                subscriber
                                    .send(RouterEvent::Unreachable)
                                    .await
                                    .map_err(|_| RoutingError::ConnectionError)?;
                            }
                        }
                        break;
                    }

                    IncomingRequest::Close => {
                        println!("Closing Router");
                        for (_, destination) in subscribers.iter_mut() {
                            for subscriber in destination {
                                subscriber
                                    .send(RouterEvent::Stopping)
                                    .await
                                    .map_err(|_| RoutingError::ConnectionError)?;
                            }
                        }
                        break;
                    }

                    _ => {}
                }
            } else {
                let task = tokio::select! {
                    Some(task) = task_rx.recv() => {
                        Some(task)
                    }

                    maybe_message = connection.as_mut().ok_or(RoutingError::ConnectionError)?.recv() => {
                        match maybe_message{
                            Some(message) => Some(IncomingRequest::Message(message)),
                            None => Some(IncomingRequest::Disconnect),
                        }
                    }

                    else => None,
                };

                let task = task.ok_or(RoutingError::ConnectionError)?;

                match task {
                    IncomingRequest::Connection(message_rx) => {
                        connection = Some(message_rx);
                    }

                    IncomingRequest::Subscribe((target, event_tx)) => {
                        subscribers
                            .entry(target.destination())
                            .or_insert_with(Vec::new)
                            .push(event_tx);
                    }

                    IncomingRequest::Message(message) => {
                        let message = message.to_text().unwrap();
                        let value = parse_single(message).unwrap();
                        let envelope = Envelope::try_from(value).unwrap();
                        let destination = envelope.destination();
                        let event = RouterEvent::Envelope(envelope);

                        if let Some(destination) = destination {
                            if subscribers.contains_key(&destination) {
                                //Todo Replace with tracing
                                println!("{:?}", event);
                                let destination_subs = subscribers
                                    .get_mut(&destination)
                                    .ok_or(RoutingError::ConnectionError)?;

                                for subscriber in destination_subs.iter_mut() {
                                    subscriber
                                        .send(event.clone())
                                        .await
                                        .map_err(|_| RoutingError::ConnectionError)?;
                                }
                            } else {
                                //Todo log the messsage
                                println!("No downlink interested in message: {:?}", event);
                            }
                        } else {
                            println!("Host messages are not supported: {:?}", event);
                        }
                    }

                    IncomingRequest::Disconnect => {
                        println!("Connection closed");
                        connection = None;

                        for (_, destination) in subscribers.iter_mut() {
                            for subscriber in destination {
                                subscriber
                                    .send(RouterEvent::ConnectionClosed)
                                    .await
                                    .map_err(|_| RoutingError::ConnectionError)?;
                            }
                        }
                    }

                    IncomingRequest::Close => {
                        println!("Closing Router");
                        for (_, destination) in subscribers.iter_mut() {
                            for subscriber in destination {
                                subscriber
                                    .send(RouterEvent::Stopping)
                                    .await
                                    .map_err(|_| RoutingError::ConnectionError)?;
                            }
                        }
                        break;
                    }

                    _ => {}
                }
            }
        }
        Ok(())
    }
}
