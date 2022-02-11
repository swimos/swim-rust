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

#[cfg(test)]
mod tests;

use crate::agent::lane::channels::uplink::{UplinkError, UplinkMessage, ValueLaneEvent};
use futures::future::join;
use futures::stream::unfold;
use futures::{Stream, StreamExt};
use pin_utils::pin_mut;
use std::fmt::Debug;
use std::hash::Hash;
use std::num::NonZeroUsize;
use swim_form::Form;
use swim_runtime::backpressure::keyed::map::release_pressure as release_pressure_map;
use swim_runtime::backpressure::{release_pressure, Flushable};
use swim_utilities::future::item_sink::ItemSender;
use swim_utilities::sync::circular_buffer;
use swim_utilities::trigger;
use swim_warp::map::MapUpdate;
use tokio::sync::oneshot;

/// Configuration for the value lane back-pressure release.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct SimpleBackpressureConfig {
    /// Buffer size for the channel connecting the input and output tasks.
    pub buffer_size: NonZeroUsize,
    /// Number of loop iterations after which the input and output tasks will yield.
    pub yield_after: NonZeroUsize,
}

/// Configuration for the map lane back-pressure release.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct KeyedBackpressureConfig {
    /// Buffer size for the channels connecting the input and output tasks.
    pub buffer_size: NonZeroUsize,
    /// Number of loop iterations after which the input and output tasks will yield.
    pub yield_after: NonZeroUsize,
    /// Buffer size for the communication side channel between the input and output tasks.
    pub bridge_buffer_size: NonZeroUsize,
    /// Number of keys for maintain a channel for at any one time.
    pub cache_size: NonZeroUsize,
}

/// Consume a stream of messages from a [`ValueLane`] with one task that pushes them into a
/// circular buffer. A second task then consumes the buffer and writes the values to a sink.
/// If the second tasks does not keep up with the first, some messages will be discarded.
pub async fn value_uplink_release_backpressure<T, E, Snk>(
    messages: impl Stream<Item = Result<UplinkMessage<ValueLaneEvent<T>>, UplinkError>>,
    sink: &mut Snk,
    config: SimpleBackpressureConfig,
) -> Result<(), UplinkError>
where
    T: Send + Sync + Debug,
    Snk: ItemSender<UplinkMessage<ValueLaneEvent<T>>, E>,
{
    let SimpleBackpressureConfig {
        buffer_size,
        yield_after,
    } = config;

    let (mut internal_tx, internal_rx) =
        circular_buffer::channel::<Flushable<UplinkMessage<ValueLaneEvent<T>>>>(buffer_size);

    let out_task = release_pressure(internal_rx, sink, yield_after);

    let in_task = async move {
        pin_mut!(messages);
        while let Some(msg) = messages.next().await {
            match msg? {
                event_msg @ UplinkMessage::Event(_) => {
                    internal_tx
                        .try_send(Flushable::Value(event_msg))
                        .expect(INTERNAL_ERROR);
                }
                ow => {
                    let (flush_tx, flush_rx) = trigger::trigger();
                    internal_tx
                        .try_send(Flushable::Flush(ow, flush_tx))
                        .expect(INTERNAL_ERROR);
                    flush_rx.await.map_err(|_| UplinkError::ChannelDropped)?;
                }
            }
        }
        Ok(())
    };

    match join(in_task, out_task).await {
        (Err(e), _) => Err(e),
        (_, Err(_)) => Err(UplinkError::ChannelDropped),
        _ => Ok(()),
    }
}

const INTERNAL_ERROR: &str = "Internal channel error.";

/// Consume a stream of messages from a [`MapLane`] with one task that pushes them into a
/// circular buffer (for each key). A second task then consumes the buffers and writes the messages
/// to a sink. If the second tasks does not keep up with the first, for a give key, some messages
/// will be discarded. Up to a fixed maximum number of keys are kept active at any one time.
pub async fn map_uplink_release_backpressure<K, V, E, Snk>(
    messages: impl Stream<Item = Result<UplinkMessage<MapUpdate<K, V>>, UplinkError>>,
    sink: &mut Snk,
    config: KeyedBackpressureConfig,
) -> Result<(), UplinkError>
where
    K: Form + Hash + Eq + Clone + Send + Sync + Debug,
    V: Form + Send + Sync + Debug,
    Snk: ItemSender<UplinkMessage<MapUpdate<K, V>>, E>,
{
    let (result_tx, result_rx) = oneshot::channel();
    pin_mut!(messages);
    //Filters out only valid messages and terminates the stream on an error, reserving the error to
    //be reported.
    let good_messages = unfold(
        (messages, Some(result_tx)),
        |(mut messages, mut maybe_tx)| async move {
            match messages.next().await {
                Some(Ok(msg)) => Some((msg, (messages, maybe_tx))),
                Some(Err(e)) => {
                    if let Some(tx) = maybe_tx.take() {
                        let _ = tx.send(Err(e));
                    }
                    None
                }
                _ => {
                    if let Some(tx) = maybe_tx.take() {
                        let _ = tx.send(Ok(()));
                    }
                    None
                }
            }
        },
    );

    let task_result = release_pressure_map(
        good_messages,
        sink,
        config.yield_after,
        config.bridge_buffer_size,
        config.cache_size,
        config.buffer_size,
    )
    .await;

    match (result_rx.await, task_result) {
        (Ok(r), _) => r,
        (_, Ok(_)) => Ok(()),
        _ => Err(UplinkError::ChannelDropped),
    }
}
