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

pub mod example {
    use either::Either;
    use futures::stream::unfold;
    use futures::{Stream, StreamExt};

    pub enum Peeled<'a, S, T> {
        Value(T),
        Completed(&'a mut S),
    }

    pub fn peel<S>(n: usize, str: &mut S) -> impl Stream<Item = Peeled<S, S::Item>>
    where
        S: Stream + Unpin,
    {
        unfold((n, Some(str)), |(n, maybe_str)| async move {
            match maybe_str {
                Some(str) => {
                    if n == 0 {
                        Some((Peeled::Completed(str), (n, None)))
                    } else {
                        match str.next().await {
                            Some(v) => Some((Peeled::Value(v), (n - 1, Some(str)))),
                            _ => Some((Peeled::Completed(str), (n - 1, None))),
                        }
                    }
                }
                _ => None,
            }
        })
    }

    pub fn borrower<'a, S, F, S2, T>(str: &'a mut S, f: &'a F) -> impl Stream<Item = Option<T>> + 'a
    where
        S: Stream + Unpin,
        F: Fn(S::Item) -> Either<S2, T> + 'a,
        S2: Stream<Item = Peeled<'a, S, T>> + 'a + Unpin,
    {
        let state: Either<&'a mut S, S2> = Either::Left(str);

        unfold(state, move |state| async move {
            match state {
                Either::Left(str) => match str.next().await.map(f) {
                    Some(Either::Left(str2)) => Some((None, Either::Right(str2))),
                    Some(Either::Right(v)) => Some((Some(v), Either::Left(str))),
                    _ => None,
                },
                Either::Right(mut str) => match str.next().await {
                    Some(Peeled::Value(v)) => Some((Some(v), Either::Right(str))),
                    Some(Peeled::Completed(str1)) => Some((None, Either::Left(str1))),
                    _ => None,
                },
            }
        })
    }
}
