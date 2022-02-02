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

use swim_utilities::future::item_sink::ItemSender;
use swim_utilities::sync::circular_buffer;
use swim_utilities::trigger;
use tokio::sync::mpsc;

pub mod keyed;

#[cfg(test)]
mod test;

#[derive(Debug)]
pub enum Flushable<T> {
    Value(T),
    Flush(T, trigger::Sender),
}

pub async fn release_pressure<T, E, Snk>(
    mut rx: circular_buffer::Receiver<Flushable<T>>,
    sink: &mut Snk,
    yield_after: NonZeroUsize,
) -> Result<(), E>
where
    T: Send + Sync,
    Snk: ItemSender<T, E>,
{
    let mut iteration_count: usize = 0;
    let yield_mod = yield_after.get();
    while let Ok(value) = rx.recv().await {
        match value {
            Flushable::Value(v) => {
                if let Err(e) = sink.send_item(v).await {
                    return Err(e);
                }
            }
            Flushable::Flush(v, tx) => {
                if let Err(e) = sink.send_item(v).await {
                    return Err(e);
                }
                tx.trigger();
            }
        }
        iteration_count = iteration_count.wrapping_add(1);
        if iteration_count % yield_mod == 0 {
            tokio::task::yield_now().await;
        }
    }
    Ok(())
}

use futures::future::{select, Either};
use pin_utils::pin_mut;

/// TODO Merge/remove the two implementations.
/// This is a variant of the abouve `release_pressure`. Currently downlinks use this version
/// and uplinks the other. They should be merged or replaced in favour of something else.
pub async fn pressure_valve<T, E>(
    mut priority: mpsc::Receiver<T>,
    mut merge: circular_buffer::Receiver<T>,
    mut sink: impl ItemSender<T, E>,
    yield_after: NonZeroUsize,
) -> Result<(), E>
where
    T: Send + Sync,
{
    let mut iteration_count: usize = 0;
    let yield_mod = yield_after.get();
    loop {
        let recv = priority.recv();
        pin_mut!(recv);
        let value = match select(recv, merge.recv()).await {
            Either::Left((Some(t), _)) => t,
            Either::Right((Ok(t), _)) => t,
            _ => {
                break;
            }
        };
        sink.send_item(value).await?;
        iteration_count = iteration_count.wrapping_add(1);
        if iteration_count % yield_mod == 0 {
            tokio::task::yield_now().await;
        }
    }
    Ok(())
}
