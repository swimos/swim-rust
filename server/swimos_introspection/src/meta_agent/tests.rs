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

use std::{num::NonZeroUsize, pin::pin, time::Duration};

use futures::{future::join, Future, SinkExt, StreamExt};
use swimos_agent_protocol::agent::{
    LaneRequest, LaneRequestEncoder, LaneResponse, LaneResponseDecoder,
};
use swimos_form::structural::read::recognizer::RecognizerReadable;
use swimos_form::Form;
use swimos_meta::WarpUplinkPulse;
use swimos_recon::WithLenRecognizerDecoder;
use swimos_runtime::agent::reporting::{UplinkReporter, UplinkSnapshot};
use swimos_utilities::{
    encoding::WithLengthBytesCodec,
    io::byte_channel::{byte_channel, ByteReader, ByteWriter},
    non_zero_usize, trigger,
};
use tokio::{sync::mpsc, time::Instant};
use tokio_stream::wrappers::ReceiverStream;
use tokio_util::codec::{FramedRead, FramedWrite};
use uuid::Uuid;

use super::{run_pulse_lane_inner, sleep_stream};

const BUFFER_SIZE: NonZeroUsize = non_zero_usize!(4096);

struct TestContext {
    shutdown_tx: trigger::Sender,
    pulse_tx: mpsc::Sender<()>,
    reporter: UplinkReporter,
    sender: LaneSender,
    receiver: LaneReceiver,
}

async fn pulse_lane_test<F, Fut>(test_case: F) -> Fut::Output
where
    F: FnOnce(TestContext) -> Fut,
    Fut: Future,
{
    let reporter = UplinkReporter::default();
    let reader = reporter.reader();
    let (shutdown_tx, shutdown_rx) = trigger::trigger();
    let (pulse_tx, pulse_rx) = mpsc::channel(8);
    let (in_tx, in_rx) = byte_channel(BUFFER_SIZE);
    let (out_tx, out_rx) = byte_channel(BUFFER_SIZE);

    let lane_task = run_pulse_lane_inner(
        shutdown_rx,
        ReceiverStream::new(pulse_rx),
        reader,
        (out_tx, in_rx),
        PulseWithDuration::new,
    );

    let context = TestContext {
        shutdown_tx,
        reporter,
        pulse_tx,
        sender: LaneSender::new(SYNC_ID, in_tx),
        receiver: LaneReceiver::new(out_rx),
    };
    let test_task = test_case(context);

    let (result, output) = join(lane_task, test_task).await;
    assert!(result.is_ok());
    output
}

pub struct LaneSender {
    sync_id: Uuid,
    writer: FramedWrite<ByteWriter, LaneRequestEncoder<WithLengthBytesCodec>>,
}

const SYNC_ID: Uuid = Uuid::from_u128(747473);

impl LaneSender {
    pub fn new(sync_id: Uuid, writer: ByteWriter) -> Self {
        LaneSender {
            sync_id,
            writer: FramedWrite::new(writer, Default::default()),
        }
    }

    pub async fn sync(&mut self) {
        let req: LaneRequest<&[u8]> = LaneRequest::Sync(self.sync_id);
        assert!(self.writer.send(req).await.is_ok());
    }
}

type RespDecoder =
    LaneResponseDecoder<WithLenRecognizerDecoder<<PulseWithDuration as RecognizerReadable>::Rec>>;

struct LaneReceiver {
    reader: FramedRead<ByteReader, RespDecoder>,
}

#[derive(Default, Form, Clone, Copy, PartialEq, Eq, Debug)]

struct PulseWithDuration {
    sec: u64,
    nano: u32,
    pulse: WarpUplinkPulse,
}

impl PulseWithDuration {
    fn new(diff: Duration, pulse: WarpUplinkPulse) -> Self {
        let sec = diff.as_secs();
        let nano = diff.subsec_nanos();
        PulseWithDuration { sec, nano, pulse }
    }
}

impl LaneReceiver {
    fn new(reader: ByteReader) -> Self {
        LaneReceiver {
            reader: FramedRead::new(
                reader,
                LaneResponseDecoder::new(WithLenRecognizerDecoder::new(
                    PulseWithDuration::make_recognizer(),
                )),
            ),
        }
    }

    async fn expect_synced(&mut self) {
        let response = UplinkSnapshot {
            link_count: 0,
            event_count: 0,
            command_count: 0,
        };
        let record = self
            .reader
            .next()
            .await
            .expect("Expected a record.")
            .expect("Bad response.");
        match record {
            LaneResponse::SyncEvent(id, PulseWithDuration { sec, nano, pulse }) => {
                assert_eq!(id, SYNC_ID);
                let dur = Duration::new(sec, nano);
                let expected = response.make_pulse(dur);
                assert_eq!(pulse, expected);
            }
            ow => panic!("Unexpected response: {:?}", ow),
        }
        let record = self
            .reader
            .next()
            .await
            .expect("Expected a record.")
            .expect("Bad response.");
        assert_eq!(record, LaneResponse::Synced(SYNC_ID));
    }

    async fn expect_response(&mut self, response: UplinkSnapshot) {
        let record = self
            .reader
            .next()
            .await
            .expect("Expected a record.")
            .expect("Bad response.");
        match record {
            LaneResponse::StandardEvent(PulseWithDuration { sec, nano, pulse }) => {
                let dur = Duration::new(sec, nano);
                let expected = response.make_pulse(dur);
                assert_eq!(pulse, expected);
            }
            ow => panic!("Unexpected response: {:?}", ow),
        }
    }
}

#[tokio::test]
async fn sync_pulse_lane() {
    pulse_lane_test(|context| async move {
        let TestContext {
            shutdown_tx,
            pulse_tx: _pulse_tx,
            mut sender,
            mut receiver,
            reporter: _reporter,
        } = context;

        sender.sync().await;
        receiver.expect_synced().await;
        shutdown_tx.trigger();
    })
    .await;
}

#[tokio::test]
async fn receive_pulse() {
    pulse_lane_test(|context| async move {
        let TestContext {
            shutdown_tx,
            pulse_tx,
            sender: _sender,
            mut receiver,
            reporter,
        } = context;

        reporter.set_uplinks(2);
        reporter.count_events(12);
        reporter.count_commands(3);

        assert!(pulse_tx.send(()).await.is_ok());

        let expected = UplinkSnapshot {
            link_count: 2,
            event_count: 12,
            command_count: 3,
        };

        receiver.expect_response(expected).await;
        shutdown_tx.trigger();
    })
    .await;
}

#[tokio::test]
async fn receive_multiple_pulses() {
    pulse_lane_test(|context| async move {
        let TestContext {
            shutdown_tx,
            pulse_tx,
            sender: _sender,
            mut receiver,
            reporter,
        } = context;

        reporter.set_uplinks(2);
        reporter.count_events(12);
        reporter.count_commands(3);

        assert!(pulse_tx.send(()).await.is_ok());
        let expected = UplinkSnapshot {
            link_count: 2,
            event_count: 12,
            command_count: 3,
        };
        receiver.expect_response(expected).await;

        assert!(pulse_tx.send(()).await.is_ok());
        let expected = UplinkSnapshot {
            link_count: 2,
            event_count: 0,
            command_count: 0,
        };
        receiver.expect_response(expected).await;

        reporter.set_uplinks(1);
        reporter.count_events(67);
        reporter.count_commands(267);

        assert!(pulse_tx.send(()).await.is_ok());
        let expected = UplinkSnapshot {
            link_count: 1,
            event_count: 67,
            command_count: 267,
        };
        receiver.expect_response(expected).await;

        shutdown_tx.trigger();
    })
    .await;
}

const PERIOD: Duration = Duration::from_secs(1);

#[tokio::test(start_paused = true)]
async fn drive_sleep_stream() {
    let sleep = pin!(tokio::time::sleep(PERIOD));
    let mut stream = pin!(sleep_stream(PERIOD, sleep));

    let t0 = Instant::now();

    stream.next().await;

    let t1 = Instant::now();

    stream.next().await;

    let t2 = Instant::now();

    assert_eq!(t1.duration_since(t0), PERIOD);
    assert_eq!(t2.duration_since(t1), PERIOD);
}
