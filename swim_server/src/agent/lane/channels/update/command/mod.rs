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

use crate::agent::lane::channels::update::{LaneUpdate, UpdateError};
use crate::agent::lane::model::command::Command;
use crate::agent::model::command::Commander;
use futures::future::BoxFuture;
use futures::select_biased;
use futures::stream::FuturesOrdered;
use futures::{FutureExt, Stream, StreamExt};
use pin_utils::pin_mut;
use std::fmt::Debug;
use std::time::Duration;
use swim_common::routing::RoutingAddr;
use swim_runtime::time::timeout;
use tokio::sync::{mpsc, oneshot};
use tokio_stream::wrappers::ReceiverStream;
use tracing::{event, Level};

pub struct CommandLaneUpdateTask<T> {
    commander: Commander<T>,
    local_commands_rx: ReceiverStream<Command<T>>,
    feedback: mpsc::Sender<T>,
    cleanup_timeout: Duration,
}

impl<T> CommandLaneUpdateTask<T>
where
    T: Send + Sync + Debug + 'static,
{
    pub fn new(
        commander: Commander<T>,
        local_commands_rx: ReceiverStream<Command<T>>,
        feedback: mpsc::Sender<T>,
        cleanup_timeout: Duration,
    ) -> Self {
        CommandLaneUpdateTask {
            commander,
            local_commands_rx,
            feedback,
            cleanup_timeout,
        }
    }
}

enum CommandLaneUpdate<T, Err> {
    Response(Result<T, oneshot::error::RecvError>),
    RemoteMessage(Result<T, Err>),
    LocalMessage(Command<T>),
}

/// Receives a message from the command lifecycle and sends it to the requester if a transmitter
/// has been provided.
async fn receive_message<T: Clone + Send + Sync + Debug + 'static>(
    receiver: oneshot::Receiver<T>,
    response_tx: Option<oneshot::Sender<T>>,
) -> Result<T, oneshot::error::RecvError> {
    let result = receiver.await?;

    if let Some(response_tx) = response_tx {
        let _ = response_tx.send(result.clone());
    }

    Ok(result)
}

const NO_COMPLETION: &str = "Command did not complete.";
const CLEANUP_TIMEOUT: &str = "Timeout waiting for pending completions.";

impl<T> LaneUpdate for CommandLaneUpdateTask<T>
where
    T: Clone + Send + Sync + Debug + 'static,
{
    type Msg = T;

    fn run_update<Messages, Err>(
        self,
        messages: Messages,
    ) -> BoxFuture<'static, Result<(), UpdateError>>
    where
        Messages: Stream<Item = Result<(RoutingAddr, Self::Msg), Err>> + Send + 'static,
        Err: Send + Debug,
        UpdateError: From<Err>,
    {
        async move {
            let CommandLaneUpdateTask {
                mut commander,
                local_commands_rx,
                feedback: resp_tx,
                cleanup_timeout,
            } = self;


            let messages = messages.fuse();
            let mut local_commands_rx = local_commands_rx.fuse();
            pin_mut!(messages);
            let mut responses = FuturesOrdered::new();

            let result: Result<(), UpdateError> = loop {
                let next = if responses.is_empty() {
                    select_biased! {
                        remote_message = messages.next() => remote_message.map(|result| CommandLaneUpdate::RemoteMessage(result.map(|(_, message)|message))),
                        local_command = local_commands_rx.next() => local_command.map(CommandLaneUpdate::LocalMessage),
                    }
                } else {
                    select_biased! {
                        response = responses.next().fuse() => response.map(CommandLaneUpdate::Response),
                        remote_message = messages.next() => remote_message.map(|result| CommandLaneUpdate::RemoteMessage(result.map(|(_, message)|message))),
                        local_command = local_commands_rx.next() => local_command.map(CommandLaneUpdate::LocalMessage),
                    }
                };

                match next {
                    Some(CommandLaneUpdate::Response(Ok(resp))) => {
                        if resp_tx.send(resp).await.is_err() {
                            break Err(UpdateError::FeedbackChannelDropped);
                        }
                    }
                    Some(CommandLaneUpdate::Response(Err(_))) => {
                        event!(Level::WARN, NO_COMPLETION);
                    }
                    Some(CommandLaneUpdate::RemoteMessage(Ok(msg))) => {
                        if let Ok(rx) = commander.command_and_await(msg).await {
                            responses.push(receive_message(rx, None));
                        } else {
                            event!(Level::ERROR, NO_COMPLETION);
                        }
                    }
                    Some(CommandLaneUpdate::RemoteMessage(Err(e))) => {
                        event!(Level::ERROR, ?e);
                    }

                    Some(CommandLaneUpdate::LocalMessage(command)) => {
                        let Command {
                            command: msg,
                            responder
                        } = command;

                        if let Ok(rx) = commander.command_and_await(msg).await {
                            if let Some(responder) = responder {
                                responses.push(receive_message(rx, Some(responder)));
                            } else {
                                responses.push(receive_message(rx, None));
                            }
                        } else {
                            event!(Level::ERROR, NO_COMPLETION);
                        }
                    }
                    _ => {
                        break Ok(());
                    }
                }
            };
            match result {
                e @ Err(UpdateError::FeedbackChannelDropped) => e,
                ow => {
                    loop {
                        let resp =
                            timeout::timeout(cleanup_timeout, responses.next()).await;
                        match resp {
                            Ok(Some(Ok(resp))) => {
                                if resp_tx.send(resp).await.is_err() {
                                    return Err(UpdateError::FeedbackChannelDropped);
                                }
                            }
                            Ok(Some(Err(_))) => {
                                event!(Level::WARN, NO_COMPLETION);
                            }
                            Ok(_) => {
                                break;
                            }
                            Err(_) => {
                                event!(Level::ERROR, CLEANUP_TIMEOUT);
                                break;
                            }
                        }
                    }
                    ow
                }
            }
        }
            .boxed()
    }
}
