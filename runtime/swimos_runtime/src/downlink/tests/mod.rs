// Copyright 2015-2023 Swim Inc.
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

use std::{convert::Infallible, num::NonZeroUsize, time::Duration};

use futures_util::{future::join, SinkExt, StreamExt};
use swimos_agent_protocol::encoding::downlink::{
    DownlinkOperationEncoder, ValueNotificationDecoder,
};
use swimos_agent_protocol::{DownlinkNotification, DownlinkOperation};
use swimos_messages::protocol::{
    Operation, Path, RawRequestMessageDecoder, RawResponseMessageEncoder, RequestMessage,
    ResponseMessage,
};
use swimos_model::{address::RelativeAddress, Text};
use swimos_utilities::{io::byte_channel::byte_channel, non_zero_usize};
use tokio::sync::mpsc;
use tokio_util::codec::{FramedRead, FramedWrite};
use uuid::Uuid;

use crate::{
    backpressure::ValueBackpressure,
    downlink::{failure::InfallibleStrategy, DownlinkOptions, DownlinkRuntimeConfig},
    timeout_coord::{downlink_timeout_coordinator, VoteResult},
};

use super::{interpretation::DownlinkInterpretation, read_task, write_task};

mod map;
mod value;

const BUFFER_SIZE: NonZeroUsize = non_zero_usize!(1024);
const CHANNEL_SIZE: usize = 16;
const TEST_TIMEOUT: Duration = Duration::from_secs(10);
const REMOTE_ADDR: Uuid = Uuid::from_u128(1);
const REMOTE_NODE: &str = "/remote";
const REMOTE_LANE: &str = "remote_lane";
const EMPTY_TIMEOUT: Duration = Duration::from_secs(2);
const ATT_QUEUE_SIZE: NonZeroUsize = non_zero_usize!(8);

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum State {
    Unlinked,
    Linked,
    Synced,
}

#[derive(Clone, Copy, Debug)]
struct DummyInterp;

impl DownlinkInterpretation for DummyInterp {
    type Error = Infallible;

    fn interpret_frame_data(
        &mut self,
        _frame: bytes::Bytes,
        _buffer: &mut bytes::BytesMut,
    ) -> Result<(), Self::Error> {
        Ok(())
    }
}

#[tokio::test(start_paused = true)]
async fn read_task_votes_to_stop_no_subscribers() {
    let (_msg_tx, msg_rx) = byte_channel(BUFFER_SIZE);
    let (_consumers_tx, consumers_rx) = mpsc::channel(CHANNEL_SIZE);
    let config = DownlinkRuntimeConfig {
        empty_timeout: EMPTY_TIMEOUT,
        attachment_queue_size: ATT_QUEUE_SIZE,
        abort_on_bad_frames: true,
        remote_buffer_size: BUFFER_SIZE,
        downlink_buffer_size: BUFFER_SIZE,
    };

    let (read_voter, write_voter, vote_rx) = downlink_timeout_coordinator();

    let task = read_task(
        msg_rx,
        consumers_rx,
        config,
        DummyInterp,
        InfallibleStrategy,
        read_voter,
    );

    let test_case = async move {
        write_voter.vote();
        vote_rx.await;
    };

    let (r, _) = tokio::time::timeout(2 * EMPTY_TIMEOUT, join(task, test_case))
        .await
        .expect("Test timed out.");
    assert!(r.is_ok());
}

#[tokio::test(start_paused = true)]
async fn read_task_rescinds_vote_to_stop() {
    let (msg_tx, msg_rx) = byte_channel(BUFFER_SIZE);
    let mut msg_sender = FramedWrite::new(msg_tx, RawResponseMessageEncoder);
    let (consumers_tx, consumers_rx) = mpsc::channel(CHANNEL_SIZE);
    let config = DownlinkRuntimeConfig {
        empty_timeout: EMPTY_TIMEOUT,
        attachment_queue_size: ATT_QUEUE_SIZE,
        abort_on_bad_frames: true,
        remote_buffer_size: BUFFER_SIZE,
        downlink_buffer_size: BUFFER_SIZE,
    };

    let (read_vote, write_voter, _vote_rx) = downlink_timeout_coordinator();

    let task = read_task(
        msg_rx,
        consumers_rx,
        config,
        DummyInterp,
        InfallibleStrategy,
        read_vote,
    );

    let test_case = async move {
        tokio::time::sleep(2 * EMPTY_TIMEOUT).await;
        let (tx, rx) = byte_channel(BUFFER_SIZE);
        let mut reader = FramedRead::new(rx, ValueNotificationDecoder::<String>::default());
        consumers_tx
            .send((tx, DownlinkOptions::DEFAULT))
            .await
            .expect("Send failed.");
        msg_sender
            .send(ResponseMessage::<_, String, String>::linked(
                REMOTE_ADDR,
                Path::new(REMOTE_NODE, REMOTE_LANE),
            ))
            .await
            .expect("Send failed.");

        let not = reader
            .next()
            .await
            .expect("Reader dropped.")
            .expect("Read failed.");
        assert!(matches!(not, DownlinkNotification::Linked));
        assert_eq!(write_voter.vote(), VoteResult::UnanimityPending);
    };

    let (r, _) = tokio::time::timeout(4 * EMPTY_TIMEOUT, join(task, test_case))
        .await
        .expect("Test timed out.");
    assert!(r.is_ok());
}

#[tokio::test(start_paused = true)]
async fn write_task_votes_to_stop_no_subscribers() {
    let (msg_tx, msg_rx) = byte_channel(BUFFER_SIZE);
    let mut msg_receiver = FramedRead::new(msg_rx, RawRequestMessageDecoder);
    let (_producers_tx, producers_rx) = mpsc::channel(CHANNEL_SIZE);
    let config = DownlinkRuntimeConfig {
        empty_timeout: EMPTY_TIMEOUT,
        attachment_queue_size: ATT_QUEUE_SIZE,
        abort_on_bad_frames: true,
        remote_buffer_size: BUFFER_SIZE,
        downlink_buffer_size: BUFFER_SIZE,
    };

    let (read_voter, write_voter, vote_rx) = downlink_timeout_coordinator();

    let task = write_task(
        msg_tx,
        producers_rx,
        Uuid::from_u128(2),
        RelativeAddress::new(Text::new(REMOTE_NODE), Text::new(REMOTE_LANE)),
        config,
        ValueBackpressure::default(),
        write_voter,
    );

    let test_case = async move {
        read_voter.vote();
        let RequestMessage { envelope, .. } = msg_receiver
            .next()
            .await
            .expect("Message channel dropped.")
            .expect("Receive failed.");
        assert!(matches!(envelope, Operation::Link));
        vote_rx.await;
    };

    tokio::time::timeout(2 * EMPTY_TIMEOUT, join(task, test_case))
        .await
        .expect("Test timed out.");
}

#[tokio::test(start_paused = true)]
async fn write_task_rescinds_stop_vote() {
    let (msg_tx, msg_rx) = byte_channel(BUFFER_SIZE);
    let mut msg_receiver = FramedRead::new(msg_rx, RawRequestMessageDecoder);
    let (producers_tx, producers_rx) = mpsc::channel(CHANNEL_SIZE);
    let config = DownlinkRuntimeConfig {
        empty_timeout: EMPTY_TIMEOUT,
        attachment_queue_size: ATT_QUEUE_SIZE,
        abort_on_bad_frames: true,
        remote_buffer_size: BUFFER_SIZE,
        downlink_buffer_size: BUFFER_SIZE,
    };

    let (read_voter, write_voter, _vote_rx) = downlink_timeout_coordinator();

    let task = write_task(
        msg_tx,
        producers_rx,
        Uuid::from_u128(2),
        RelativeAddress::new(Text::new(REMOTE_NODE), Text::new(REMOTE_LANE)),
        config,
        ValueBackpressure::default(),
        write_voter,
    );

    let test_case = async move {
        let RequestMessage { envelope, .. } = msg_receiver
            .next()
            .await
            .expect("Message channel dropped.")
            .expect("Receive failed.");
        assert!(matches!(envelope, Operation::Link));
        tokio::time::sleep(2 * EMPTY_TIMEOUT).await;
        let (tx, rx) = byte_channel(BUFFER_SIZE);
        producers_tx
            .send((rx, DownlinkOptions::empty()))
            .await
            .expect("Send failed.");
        let mut sender = FramedWrite::new(tx, DownlinkOperationEncoder::default());
        sender
            .send(DownlinkOperation::new("a".to_string()))
            .await
            .expect("Write failed.");
        let RequestMessage { envelope, .. } = msg_receiver
            .next()
            .await
            .expect("Message channel dropped.")
            .expect("Receive failed.");
        assert!(envelope.is_command());
        assert_eq!(read_voter.vote(), VoteResult::UnanimityPending);
    };

    tokio::time::timeout(4 * EMPTY_TIMEOUT, join(task, test_case))
        .await
        .expect("Test timed out.");
}
