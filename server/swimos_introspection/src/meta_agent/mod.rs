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

use std::{
    pin::{pin, Pin},
    time::Duration,
};

use futures::{stream::unfold, SinkExt, Stream, StreamExt};
use swimos_api::{
    error::FrameIoError,
    meta::uplink::WarpUplinkPulse,
    protocol::{
        agent::{LaneRequest, LaneRequestDecoder, LaneResponse, LaneResponseEncoder},
        WithLenReconEncoder, WithLengthBytesCodec,
    },
};
use swimos_form::structural::write::StructuralWritable;
use swimos_runtime::agent::reporting::{UplinkReportReader, UplinkSnapshot};
use swimos_utilities::{
    io::byte_channel::{ByteReader, ByteWriter},
    routing::route_uri::RouteUri,
    trigger,
};
use thiserror::Error;
use tokio::time::{Instant, Sleep};
use tokio_util::codec::{FramedRead, FramedWrite};

#[cfg(test)]
mod test_harness;
#[cfg(test)]
mod tests;

pub mod lane;
pub mod node;

const PULSE_LANE: &str = "pulse";

type Io = (ByteWriter, ByteReader);

/// Run a lane that emits events on a fixed schedule, computed from an [`UplinkReportReader`].
///
/// #Arguments
/// * `shutdown_rx` - Shutdown signal for when the agent is stopping.
/// * `pulse_interval` - Interval on which to emit events.
/// * `reporter_reader` - Reader to produce uplink statistics snapshots for the events.
/// * `pulse_io` - The input and output channels for the lane.
/// * `wrap` - Function used to wrap the snapshots as the appropriate type for the lane.
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
    let sleep = pin!(tokio::time::sleep(pulse_interval));
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

    let mut input =
        FramedRead::new(rx, LaneRequestDecoder::new(WithLengthBytesCodec)).take_until(shutdown_rx);
    let mut output = FramedWrite::new(tx, LaneResponseEncoder::new(WithLenReconEncoder));

    let mut previous = Instant::now();
    let mut accumulate = |report: UplinkSnapshot| {
        let now = Instant::now();
        let diff = now.duration_since(previous);
        previous = now;
        let uplink_pulse = report.make_pulse(diff);
        wrap(diff, uplink_pulse)
    };

    let mut last_pulse = match report_reader.snapshot() {
        Some(snapshot) => accumulate(snapshot),
        None => return Ok(()),
    };

    let mut pulses = pin!(pulses);

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
                let synced = LaneResponse::SyncEvent(id, &last_pulse);
                output.send(synced).await?;

                let synced: LaneResponse<PulseType> = LaneResponse::Synced(id);
                output.send(synced).await?;
            }
            None => {
                if let Some(report) = report_reader.snapshot() {
                    let pulse = accumulate(report);
                    output.send(LaneResponse::StandardEvent(&pulse)).await?;
                    last_pulse = pulse;
                } else {
                    break Ok(());
                }
            }
            _ => {}
        }
    }
}

#[derive(Debug, Error)]
#[error("Invalid introspection URI: {route}. Missing parameter: {missing}")]
struct MetaRouteError {
    route: RouteUri,
    missing: String,
}

impl MetaRouteError {
    pub fn new(route: RouteUri, missing: &str) -> Self {
        MetaRouteError {
            route,
            missing: missing.to_string(),
        }
    }
}
