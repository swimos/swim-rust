// Copyright 2015-2021 Swim Inc.
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

use std::num::NonZeroUsize;
use std::time::Duration;
use std::future::Future;

use crate::compat::{ResponseMessageEncoder, ResponseMessage, Operation, AgentMessageDecoder, RequestMessage, MessageDecodeError};
use crate::routing::RoutingAddr;

use super::{AttachAction, DownlinkOptions, ValueDownlinkManagementTask};
use futures::{SinkExt, StreamExt};
use futures::future::join3;
use swim_api::error::DownlinkTaskError;
use swim_api::protocol::{
    DownlinkNotifiationDecoder, DownlinkNotification, DownlinkOperation, DownlinkOperationEncoder, DownlinkNotificationEncoder,
};
use swim_form::structural::write::write_by_ref;
use swim_form::structural::read::recognizer::RecognizerReadable;
use swim_form::Form;
use swim_model::Text;
use swim_model::path::RelativePath;
use swim_recon::printer::print_recon_compact;
use swim_utilities::algebra::non_zero_usize;
use swim_utilities::io::byte_channel::{self, ByteReader, ByteWriter};
use swim_utilities::trigger;
use tokio::sync::mpsc;
use tokio::time::timeout;
use tokio_util::codec::{FramedRead, FramedWrite};

#[derive(Debug, Form, PartialEq, Eq, Clone)]
enum Message {
    Ping,
    CurrentValue(Text),
}

const BUFFER_SIZE: NonZeroUsize = non_zero_usize!(1024);

enum State {
    Unlinked,
    Linked,
    Synced,
}

async fn run_fake_downlink(
    sub: mpsc::Sender<AttachAction>,
    options: DownlinkOptions,
) -> Result<(), DownlinkTaskError> {
    let (tx_in, rx_in) = byte_channel::byte_channel(BUFFER_SIZE);
    let (tx_out, rx_out) = byte_channel::byte_channel(BUFFER_SIZE);
    if sub
        .send(AttachAction::new(rx_out, tx_in, options))
        .await
        .is_err()
    {
        return Err(DownlinkTaskError::FailedToStart);
    }
    let mut state = State::Unlinked;
    let mut read = FramedRead::new(
        rx_in,
        DownlinkNotifiationDecoder::new(Message::make_recognizer()),
    );

    let mut write = FramedWrite::new(tx_out, DownlinkOperationEncoder);

    let mut current = Text::default();

    while let Some(message) = read.next().await.transpose()? {
        match state {
            State::Unlinked => match message {
                DownlinkNotification::Linked => {
                    state = State::Linked;
                }
                DownlinkNotification::Synced => {
                    state = State::Synced;
                }
                _ => {}
            },
            State::Linked => match message {
                DownlinkNotification::Synced => {
                    state = State::Synced;
                }
                DownlinkNotification::Unlinked => {
                    state = State::Unlinked;
                    break;
                }
                DownlinkNotification::Event {
                    body: Message::CurrentValue(v),
                } => {
                    current = v;
                }
                _ => {}
            },
            State::Synced => match message {
                DownlinkNotification::Unlinked => {
                    state = State::Unlinked;
                    break;
                }
                DownlinkNotification::Event {
                    body: Message::CurrentValue(v),
                } => {
                    current = v;
                }
                DownlinkNotification::Event {
                    body: Message::Ping,
                } => {
                    if write
                        .send(DownlinkOperation { body: write_by_ref(&current) })
                        .await
                        .is_err()
                    {
                        break;
                    }
                }
                _ => {}
            },
        }
    }
    Ok(())
}

const CHANNEL_SIZE: usize = 16;
const TEST_TIMEOUT: Duration = Duration::from_secs(5);
const REMOTE_ADDR: RoutingAddr = RoutingAddr::remote(1);
const REMOTE_NODE: &str = "/remote";
const REMOTE_LANE: &str = "remote_lane";

async fn run_test<F, Fut>(options: DownlinkOptions, test_block: F) -> Result<(), DownlinkTaskError>
where
    F: FnOnce(TestSender, TestReceiver, trigger::Sender) -> Fut,
    Fut: Future<Output = ()> + Send + 'static,
{
    let (attach_tx, attach_rx) = mpsc::channel(CHANNEL_SIZE);
    let (stop_tx, stop_rx) = trigger::trigger();
    let downlink = run_fake_downlink(attach_tx, options);

    let (in_tx, in_rx) = byte_channel::byte_channel(BUFFER_SIZE);
    let (out_tx, out_rx) = byte_channel::byte_channel(BUFFER_SIZE);

    let path = RelativePath::new("/node", "lane");
    let config = super::Config {
        empty_timeout: Duration::from_secs(5),
        flush_timout: Duration::from_millis(10),
    };

    let management_task = ValueDownlinkManagementTask::new(
        attach_rx, 
        in_rx, 
        out_tx, 
        stop_rx, 
        RoutingAddr::client(1), 
        path, 
        config).run();

    let test_task = test_block(TestSender::new(in_tx), TestReceiver::new(out_rx), stop_tx);

    let (_, _, result) = timeout(TEST_TIMEOUT, join3(management_task, test_task, downlink)).await.unwrap();
    result
}

struct TestSender(FramedWrite<ByteWriter, ResponseMessageEncoder>);

type MsgDecoder<T> = AgentMessageDecoder<T, <T as RecognizerReadable>::Rec>;
struct TestReceiver(FramedRead<ByteReader, MsgDecoder<Message>>);

impl TestSender {

    fn new(writer: ByteWriter) -> Self {
        TestSender(FramedWrite::new(writer, ResponseMessageEncoder))
    }

    async fn link(&mut self) {
        self.send(ResponseMessage::linked(REMOTE_ADDR, RelativePath::new(REMOTE_NODE, REMOTE_LANE))).await;

    }

    async fn sync(&mut self) {
        self.send(ResponseMessage::synced(REMOTE_ADDR, RelativePath::new(REMOTE_NODE, REMOTE_LANE))).await;

    }

    async fn unlink(&mut self) {
        self.send(ResponseMessage::unlinked(REMOTE_ADDR, RelativePath::new(REMOTE_NODE, REMOTE_LANE), None)).await;
    }

    async fn send(&mut self, message: ResponseMessage<Message, &[u8]>) {
        assert!(self.0.send(message).await.is_ok());
    }

    async fn update(&mut self, message: Message) {
        let recon = format!("{}", print_recon_compact(&message));
        let message = ResponseMessage::event(
            REMOTE_ADDR, RelativePath::new(REMOTE_NODE, REMOTE_LANE), message);
        self.send(message).await;
    }

}

impl TestReceiver {

    fn new(reader: ByteReader) -> Self {
        TestReceiver(FramedRead::new(reader, MsgDecoder::new(Message::make_recognizer())))
    }

    async fn recv(&mut self) -> Option<Result<RequestMessage<Message>, MessageDecodeError>> {
        self.0.next().await
    }

}

fn expect_message(result: Option<Result<RequestMessage<Message>, MessageDecodeError>>, message: Operation<Message>) {
    match result {
        Some(Ok(m)) => {
            assert_eq!(m.envelope, message);
        },
        Some(Err(e)) => {
            panic!("Unexpected error: {}", e); 
        }
        _ => {
            panic!("Unexpected termination.")
        }
    }
}

#[tokio::test]
async fn clean_shutdown() {
    assert!(run_test(DownlinkOptions::SYNC, |_tx, mut rx: TestReceiver, stop| async move {
        expect_message(rx.recv().await, Operation::Link);
        stop.trigger();
    }).await.is_ok());
}