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

use either::Either;
use std::fmt::Display;
use swimos_client_api::DownlinkConfig;

use futures::{Sink, SinkExt, StreamExt};
use tokio::sync::mpsc;
use tokio_stream::wrappers::ReceiverStream;
use tokio_util::codec::{FramedRead, FramedWrite};
use tracing::{info_span, trace, Instrument};

use swimos_agent_protocol::encoding::downlink::{
    DownlinkOperationEncoder, ValueNotificationDecoder,
};
use swimos_agent_protocol::{DownlinkNotification, DownlinkOperation};
use swimos_api::{address::Address, error::DownlinkTaskError};
use swimos_form::write::StructuralWritable;
use swimos_form::Form;
use swimos_model::Text;
use swimos_recon::print_recon;
use swimos_utilities::byte_channel::{ByteReader, ByteWriter};
use swimos_utilities::future::{immediate_or_join, race};

use crate::model::lifecycle::ValueDownlinkLifecycle;
use crate::model::ValueDownlinkSet;
use crate::ValueDownlinkModel;

/// Task to drive a value downlink, calling lifecyle events at appropriate points.
///
/// #Arguments
///
/// * `model` - The downlink model, providing the lifecycle and a stream of values to set.
/// * `path` - The path of the lane to which the downlink is attached.
/// * `config` - Configuration parameters to the downlink.
/// * `input` - Input stream for messages to the downlink from the runtime.
/// * `output` - Output stream for messages from the downlink to the runtime.
pub async fn value_downlink_task<T, LC>(
    model: ValueDownlinkModel<T, LC>,
    path: Address<Text>,
    config: DownlinkConfig,
    input: ByteReader,
    output: ByteWriter,
) -> Result<(), DownlinkTaskError>
where
    T: Form + Send + Sync + Clone + 'static,
    LC: ValueDownlinkLifecycle<T>,
{
    let ValueDownlinkModel { handle, lifecycle } = model;
    run_io(
        config,
        input,
        lifecycle,
        handle,
        FramedWrite::new(output, DownlinkOperationEncoder::default()),
    )
    .instrument(info_span!("Downlink io task.", %path))
    .await
}

enum State<T> {
    Unlinked,
    Linked(Option<T>),
    Synced(T),
}

struct ShowState<'a, T>(&'a State<T>);

impl<'a, T: StructuralWritable> Display for ShowState<'a, T> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let ShowState(inner) = self;
        match *inner {
            State::Unlinked => f.write_str("Unlinked"),
            State::Linked(Some(v)) => write!(f, "Linked({})", print_recon(v)),
            State::Linked(_) => f.write_str("Linked"),
            State::Synced(v) => write!(f, "Synced({})", print_recon(v)),
        }
    }
}

enum Mode {
    ReadWrite,
    Read,
}

async fn run_io<T, LC, S>(
    config: DownlinkConfig,
    input: ByteReader,
    mut lifecycle: LC,
    handle_rx: mpsc::Receiver<ValueDownlinkSet<T>>,
    mut framed: S,
) -> Result<(), DownlinkTaskError>
where
    LC: ValueDownlinkLifecycle<T>,
    T: Form + Send + Sync + Clone + 'static,
    S: Sink<DownlinkOperation<T>> + Unpin,
{
    let mut mode = Mode::ReadWrite;
    let mut state: State<T> = State::Unlinked;
    let mut framed_read = FramedRead::new(input, ValueNotificationDecoder::default());
    let mut set_stream = ReceiverStream::new(handle_rx);

    let DownlinkConfig {
        events_when_not_synced,
        terminate_on_unlinked,
        ..
    } = config;

    loop {
        match mode {
            Mode::ReadWrite => {
                match race(
                    immediate_or_join(set_stream.next(), framed.flush()),
                    framed_read.next(),
                )
                .await
                {
                    Either::Left((Some(ValueDownlinkSet { to }), Some(Ok(_)) | None)) => {
                        if write(&mut framed, to).await.is_err() {
                            mode = Mode::Read;
                        }
                    }
                    Either::Left(_) => mode = Mode::Read,
                    Either::Right(Some(Ok(frame))) => {
                        match on_read(
                            state,
                            &mut lifecycle,
                            frame,
                            events_when_not_synced,
                            terminate_on_unlinked,
                        )
                        .await
                        {
                            Ok(Some(new_state)) => state = new_state,
                            Ok(None) => return Ok(()),
                            Err(e) => return Err(e),
                        }
                    }
                    Either::Right(Some(Err(e))) => return Err(e.into()),
                    Either::Right(None) => return Ok(()),
                }
            }
            Mode::Read => {
                while let Some(result) = framed_read.next().await {
                    match on_read(
                        state,
                        &mut lifecycle,
                        result?,
                        events_when_not_synced,
                        terminate_on_unlinked,
                    )
                    .await
                    {
                        Ok(Some(new_state)) => state = new_state,
                        Ok(None) => return Ok(()),
                        Err(e) => return Err(e),
                    }
                }
                return Ok(());
            }
        }
    }
}

async fn write<S, T>(framed: &mut S, op: T) -> Result<(), ()>
where
    S: Sink<DownlinkOperation<T>> + Unpin,
    T: Form + Send + Sync + 'static,
{
    trace!("Sending command '{cmd}'.", cmd = print_recon(&op));
    let op = DownlinkOperation::new(op);
    if framed.feed(op).await.is_ok() {
        Ok(())
    } else {
        Err(())
    }
}

async fn on_read<T, LC>(
    mut state: State<T>,
    lifecycle: &mut LC,
    notification: DownlinkNotification<T>,
    events_when_not_synced: bool,
    terminate_on_unlinked: bool,
) -> Result<Option<State<T>>, DownlinkTaskError>
where
    T: 'static + Form + Send + Sync,
    LC: ValueDownlinkLifecycle<T>,
{
    match notification {
        DownlinkNotification::Linked => {
            trace!(
                "Received Linked in state {state}",
                state = ShowState(&state)
            );
            if matches!(&state, State::Unlinked) {
                lifecycle.on_linked().await;
                state = State::Linked(None);
            }
        }
        DownlinkNotification::Synced => {
            trace!(
                "Received Synced in state {state}",
                state = ShowState(&state)
            );
            return match state {
                State::Linked(Some(value)) => {
                    lifecycle.on_synced(&value).await;
                    Ok(Some(State::Synced(value)))
                }
                _ => Err(DownlinkTaskError::SyncedWithNoValue),
            };
        }
        DownlinkNotification::Event { body } => {
            trace!(
                "Received Event with body '{body}' in state {state}",
                body = print_recon(&body),
                state = ShowState(&state)
            );
            match state {
                State::Linked(value) => {
                    if events_when_not_synced {
                        lifecycle.on_event(&body).await;
                        lifecycle.on_set(value.as_ref(), &body).await;
                    }
                    return Ok(Some(State::Linked(Some(body))));
                }
                State::Synced(value) => {
                    lifecycle.on_event(&body).await;
                    lifecycle.on_set(Some(&value), &body).await;
                    return Ok(Some(State::Synced(body)));
                }
                _ => {}
            }
        }
        DownlinkNotification::Unlinked => {
            trace!(
                "Received Unlinked in state {state}",
                state = ShowState(&state)
            );
            lifecycle.on_unlinked().await;
            if terminate_on_unlinked {
                trace!("Terminating on Unlinked.");
                return Ok(None);
            } else {
                state = State::Unlinked;
            }
        }
    }
    Ok(Some(state))
}
