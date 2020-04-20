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
use crate::router::configuration::RouterConfig;
use crate::router::envelope_routing_task::retry::boxed_connection_sender::BoxedConnSender;
use crate::router::envelope_routing_task::retry::RetryableRequest;
use crate::router::{CloseRequestReceiver, CloseRequestSender, ConnReqSender, RoutingError};
use futures::{stream, StreamExt};
use tokio::sync::mpsc;
use tokio_tungstenite::tungstenite::protocol::Message;

pub type CommandSender = mpsc::Sender<((url::Url, String, String), String)>;
pub type CommandReceiver = mpsc::Receiver<((url::Url, String, String), String)>;

pub struct CommandRoutingTask {
    connection_request_tx: ConnReqSender,
    command_rx: CommandReceiver,
    close_request_rx: CloseRequestReceiver,
    config: RouterConfig,
}

impl CommandRoutingTask {
    pub fn new(
        connection_request_tx: ConnReqSender,
        config: RouterConfig,
    ) -> (Self, CommandSender, CloseRequestSender) {
        let (command_tx, command_rx) = mpsc::channel(config.buffer_size().get());
        let (close_request_tx, close_request_rx) = mpsc::channel(config.buffer_size().get());

        (
            CommandRoutingTask {
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
        let CommandRoutingTask {
            connection_request_tx,
            command_rx,
            close_request_rx,
            config,
        } = self;

        let mut rx = combine_command_requests(command_rx, close_request_rx);

        loop {
            let command_request = rx.next().await.ok_or(RoutingError::ConnectionError)?;

            match command_request {
                CommandType::Send(((host_url, node, lane), message)) => {
                    //Todo add proper conversion
                    let command_message = format!(
                        "@command(node:\"{}\", lane:\"{}\")\"{}\"",
                        node, lane, message
                    );

                    //Todo log errors
                    let _ = RetryableRequest::send(
                        BoxedConnSender::new(connection_request_tx.clone(), host_url),
                        Message::Text(command_message),
                        config.retry_strategy(),
                    )
                    .await
                    .map_err(|_| println!("Unreachable Host"));
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
    Send(((url::Url, String, String), String)),
    Close,
}

fn combine_command_requests(
    command_rx: CommandReceiver,
    close_request_rx: CloseRequestReceiver,
) -> impl stream::Stream<Item = CommandType> + Send + 'static {
    let command_request = command_rx.map(CommandType::Send);
    let close_request = close_request_rx.map(|_| CommandType::Close);

    stream::select(command_request, close_request)
}
