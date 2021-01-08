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

use crate::agent::lane::channels::uplink::UplinkMessage;
use futures::future::join;
use futures::{Stream, StreamExt};
use pin_utils::pin_mut;
use std::fmt::Debug;
use std::hash::Hash;
use std::num::NonZeroUsize;
use std::sync::Arc;
use swim_common::form::ValidatedForm;
use swim_common::sink::item::ItemSender;
use swim_warp::backpressure::map::release_pressure as release_pressure_map;
use swim_warp::backpressure::{release_pressure, Flushable};
use swim_warp::model::map::MapUpdate;
use utilities::sync::{circular_buffer, trigger};

pub async fn value_uplink_release_backpressure<T, E, Snk>(
    messages: impl Stream<Item = UplinkMessage<Arc<T>>>,
    mut sink: Snk,
    buffer_size: NonZeroUsize,
    yield_after: NonZeroUsize,
) -> Result<(), E>
where
    T: Send + Sync + Debug,
    Snk: ItemSender<UplinkMessage<Arc<T>>, E> + Clone,
{
    let (mut internal_tx, internal_rx) =
        circular_buffer::channel::<Flushable<UplinkMessage<Arc<T>>>>(buffer_size);

    let out_task = release_pressure(internal_rx, sink.clone(), yield_after);

    let in_task = async move {
        pin_mut!(messages);
        while let Some(msg) = messages.next().await {
            match msg {
                event_msg @ UplinkMessage::Event(_) => {
                    internal_tx
                        .try_send(Flushable::Value(event_msg))
                        .expect(INTERNAL_ERROR);
                }
                ow => {
                    let (flush_tx, flush_rx) = trigger::trigger();
                    internal_tx
                        .try_send(Flushable::Flush(flush_tx))
                        .expect(INTERNAL_ERROR);
                    let _ = flush_rx.await;
                    sink.send_item(ow).await?;
                }
            }
        }
        Ok(())
    };

    match join(in_task, out_task).await {
        (Err(e), _) => Err(e),
        (_, Err(e)) => Err(e),
        _ => Ok(()),
    }
}

const INTERNAL_ERROR: &str = "Internal channel error.";

//TODO Remove ValidatedForm constraint.
pub async fn map_uplink_release_backpressure<K, V, E, Snk>(
    messages: impl Stream<Item = UplinkMessage<MapUpdate<K, V>>>,
    sink: Snk,
    buffer_size: NonZeroUsize,
    bridge_buffer_size: NonZeroUsize,
    cache_size: NonZeroUsize,
    yield_after: NonZeroUsize,
) -> Result<(), E>
where
    K: ValidatedForm + Hash + Eq + Clone + Send + Sync + Debug,
    V: ValidatedForm + Send + Sync + Debug,
    Snk: ItemSender<UplinkMessage<MapUpdate<K, V>>, E> + Clone,
{
    release_pressure_map(
        messages,
        sink.clone(),
        yield_after,
        bridge_buffer_size,
        cache_size,
        buffer_size,
    )
    .await
}
