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
use crate::agent::lane::model::command::CommandLane;
use either::Either;
use futures::future::BoxFuture;
use futures::select_biased;
use futures::stream::FuturesOrdered;
use futures::{FutureExt, Stream, StreamExt};
use pin_utils::pin_mut;
use std::fmt::Debug;
use std::time::Duration;
use swim_common::routing::RoutingAddr;
use swim_runtime::time::timeout;
use tokio::sync::mpsc;
use tracing::{event, Level};

pub struct CommandLaneUpdateTask<T> {
    lane: CommandLane<T>,
    feedback: Option<mpsc::Sender<T>>,
    cleanup_timeout: Duration,
}

impl<T> CommandLaneUpdateTask<T>
where
    T: Send + Sync + Debug + 'static,
{
    pub fn new(
        lane: CommandLane<T>,
        feedback: Option<mpsc::Sender<T>>,
        cleanup_timeout: Duration,
    ) -> Self {
        CommandLaneUpdateTask {
            lane,
            feedback,
            cleanup_timeout,
        }
    }
}

const NO_COMPLETION: &str = "Command did not complete.";
const CLEANUP_TIMEOUT: &str = "Timeout waiting for pending completions.";

impl<T> LaneUpdate for CommandLaneUpdateTask<T>
where
    T: Send + Sync + Debug + 'static,
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
                lane,
                feedback,
                cleanup_timeout,
            } = self;
            let mut commander = lane.commander();
            match feedback {
                Some(resp_tx) => {
                    let messages = messages.fuse();
                    pin_mut!(messages);
                    let mut responses = FuturesOrdered::new();
                    let result: Result<(), UpdateError> = loop {
                        let resp_or_msg = if responses.is_empty() {
                            messages.next().await.map(Either::Right)
                        } else {
                            select_biased! {
                                response = responses.next().fuse() => response.map(Either::Left),
                                result = messages.next() => result.map(Either::Right),
                            }
                        };

                        match resp_or_msg {
                            Some(Either::Left(Ok(addr_and_resp))) => {
                                if resp_tx.send(addr_and_resp).await.is_err() {
                                    break Err(UpdateError::FeedbackChannelDropped);
                                }
                            }
                            Some(Either::Left(Err(_))) => {
                                event!(Level::WARN, NO_COMPLETION);
                            }
                            Some(Either::Right(Ok((addr, msg)))) => {
                                if let Ok(rx) = commander.command_and_await(msg).await {
                                    responses.push(rx);
                                } else {
                                    event!(Level::ERROR, NO_COMPLETION);
                                }
                            }
                            Some(Either::Right(Err(e))) => {
                                event!(Level::ERROR, ?e);
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
                                    Ok(Some(Ok(addr_and_resp))) => {
                                        if resp_tx.send(addr_and_resp).await.is_err() {
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
                _ => {
                    pin_mut!(messages);
                    while let Some(result) = messages.next().await {
                        let (_, msg) = result?;
                        commander.command(msg).await;
                    }
                    Ok(())
                }
            }
        }
        .boxed()
    }
}
