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
use crate::configuration::router::RouterParams;
use crate::router::retry::RetryableRequest;
use crate::router::{CloseRequestReceiver, CloseRequestSender, ConnReqSender, RoutingError};
use common::warp::path::AbsolutePath;
use futures::{stream, StreamExt, TryFutureExt};
use tokio::sync::{mpsc, oneshot};
use tokio_tungstenite::tungstenite::protocol::Message;
use utilities::future::retryable::RetryableFuture;

pub type CommandSender = mpsc::Sender<(AbsolutePath, String)>;
pub type CommandReceiver = mpsc::Receiver<(AbsolutePath, String)>;

pub struct CommandTask {
    connection_request_tx: ConnReqSender,
    command_rx: CommandReceiver,
    close_request_rx: CloseRequestReceiver,
    config: RouterParams,
}

impl CommandTask {
    pub fn new(
        connection_request_tx: ConnReqSender,
        config: RouterParams,
    ) -> (Self, CommandSender, CloseRequestSender) {
        let (command_tx, command_rx) = mpsc::channel(config.buffer_size().get());
        let (close_request_tx, close_request_rx) = mpsc::channel(config.buffer_size().get());

        (
            CommandTask {
                connection_request_tx,
                command_rx,
                close_request_rx,
                config,
            },
            command_tx,
            close_request_tx,
        )
    }

    pub async fn run(self) -> Result<(), RoutingError> {
        let CommandTask {
            connection_request_tx,
            command_rx,
            close_request_rx,
            config,
        } = self;

        let mut rx = combine_command_streams(command_rx, close_request_rx);

        loop {
            let command = rx.next().await.ok_or(RoutingError::ConnectionError)?;

            match command {
                CommandType::Send((AbsolutePath { host, node, lane }, message)) => {
                    //Todo add proper conversion
                    let command_message = format!(
                        "@command(node:\"{}\", lane:\"{}\")\"{}\"",
                        node, lane, message
                    );

                    let message = Message::Text(command_message).to_string();
                    let message = message.as_str();
                    let retryable = RetryableRequest::new(|is_retry| {
                        let mut sender = connection_request_tx.clone();
                        let host = host.clone();

                        async move {
                            let (connection_tx, connection_rx) = oneshot::channel();

                            sender
                                .send((host, connection_tx, is_retry))
                                .await
                                .map_err(|_| RoutingError::ConnectionError)?;

                            connection_rx
                                .await
                                .map_err(|_| RoutingError::ConnectionError)?
                        }
                        .and_then(|mut s| async move {
                            s.send_message(&message)
                                .map_err(|_| RoutingError::ConnectionError)
                                .await
                        })
                    });

                    let retry = RetryableFuture::new(retryable, config.retry_strategy());
                    retry.await.map_err(|_| RoutingError::ConnectionError)?;
                }

                CommandType::Close => {
                    break;
                }
            }
        }

        Ok(())
    }
}

enum CommandType {
    Send((AbsolutePath, String)),
    Close,
}

fn combine_command_streams(
    command_rx: CommandReceiver,
    close_request_rx: CloseRequestReceiver,
) -> impl stream::Stream<Item = CommandType> + Send + 'static {
    let command_request = command_rx.map(CommandType::Send);
    let close_request = close_request_rx.map(|_| CommandType::Close);

    stream::select(command_request, close_request)
}
