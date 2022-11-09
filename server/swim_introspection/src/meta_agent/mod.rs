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

use std::{pin::Pin, time::Duration};

use futures::{pin_mut, stream::unfold, SinkExt, Stream, StreamExt};
use swim_api::{
    error::FrameIoError,
    meta::uplink::WarpUplinkPulse,
    protocol::{
        agent::{LaneRequest, LaneRequestDecoder, LaneResponse, LaneResponseEncoder},
        WithLenReconEncoder, WithLengthBytesCodec,
    },
};
use swim_form::structural::write::StructuralWritable;
use swim_runtime::agent::reporting::UplinkReportReader;
use swim_utilities::{
    io::byte_channel::{ByteReader, ByteWriter},
    trigger,
};
use tokio::time::{Instant, Sleep};
use tokio_util::codec::{FramedRead, FramedWrite};

pub mod lane;
pub mod node;
#[cfg(test)]
mod test_harness;
#[cfg(test)]
mod tests;

const PULSE_LANE: &str = "pulse";

type Io = (ByteWriter, ByteReader);

fn sleep_stream(pulse_interval: Duration, sleep: Pin<&mut Sleep>) -> impl Stream<Item = ()> + '_ {
    unfold(sleep, move |mut sleep| {
        let new_timeout = Instant::now()
            .checked_add(pulse_interval)
            .expect("Timer overflow.");
        sleep.as_mut().reset(new_timeout);
        async move {
            sleep.as_mut().await;
            Some(((), sleep))
        }
    })
}

async fn run_pulse_lane<PulseType, F>(
    shutdown_rx: trigger::Receiver,
    pulse_interval: Duration,
    report_reader: UplinkReportReader,
    pulse_io: Io,
    wrap: F,
) -> Result<(), FrameIoError>
where
    PulseType: StructuralWritable,
    F: Fn(WarpUplinkPulse) -> PulseType,
{
    let sleep = tokio::time::sleep(pulse_interval);
    pin_mut!(sleep);
    let sleep_str = sleep_stream(pulse_interval, sleep);

    run_pulse_lane_inner(
        shutdown_rx,
        sleep_str,
        report_reader,
        pulse_io,
        move |_, pulse| wrap(pulse),
    )
    .await
}

async fn run_pulse_lane_inner<PulseType, F, S>(
    shutdown_rx: trigger::Receiver,
    pulses: S,
    report_reader: UplinkReportReader,
    pulse_io: Io,
    wrap: F,
) -> Result<(), FrameIoError>
where
    PulseType: StructuralWritable,
    F: Fn(Duration, WarpUplinkPulse) -> PulseType,
    S: Stream<Item = ()>,
{
    let (tx, rx) = pulse_io;

    let mut input = FramedRead::new(rx, LaneRequestDecoder::new(WithLengthBytesCodec::default()))
        .take_until(shutdown_rx);
    let mut output = FramedWrite::new(tx, LaneResponseEncoder::new(WithLenReconEncoder::default()));

    let mut previous = Instant::now();
    if report_reader.snapshot().is_none() {
        return Ok(());
    }

    pin_mut!(pulses);

    loop {
        let result = tokio::select! {
            biased;
            maybe_request = input.next() => {
                if maybe_request.is_some() {
                    maybe_request
                } else {
                    break Ok(());
                }
            }
            maybe_pulse = pulses.next() => {
                if maybe_pulse.is_some() {
                    None
                } else {
                    break Ok(());
                }
            },
        };

        match result.transpose()? {
            Some(LaneRequest::Sync(id)) => {
                let synced: LaneResponse<PulseType> = LaneResponse::Synced(id);
                output.send(synced).await?;
            }
            None => {
                if let Some(report) = report_reader.snapshot() {
                    let now = Instant::now();
                    let diff = now.duration_since(previous);
                    previous = now;
                    let uplink_pulse = report.make_pulse(diff);
                    let pulse = wrap(diff, uplink_pulse);
                    output.send(LaneResponse::StandardEvent(pulse)).await?;
                } else {
                    break Ok(());
                }
            }
            _ => {}
        }
    }
}
