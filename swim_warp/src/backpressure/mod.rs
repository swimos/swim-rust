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

mod common;
pub mod keyed;
pub mod map;

#[cfg(test)]
mod test;

use std::num::NonZeroUsize;
use swim_common::sink::item::ItemSender;
use utilities::sync::{circular_buffer, trigger};

const DEFAULT_YIELD: NonZeroUsize = unsafe { NonZeroUsize::new_unchecked(256) };
const DEFAULT_BUFFER: NonZeroUsize = unsafe { NonZeroUsize::new_unchecked(64) };

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

impl Default for KeyedBackpressureConfig {
    fn default() -> Self {
        KeyedBackpressureConfig {
            buffer_size: DEFAULT_BUFFER,
            yield_after: DEFAULT_YIELD,
            bridge_buffer_size: NonZeroUsize::new(32).unwrap(),
            cache_size: NonZeroUsize::new(4).unwrap(),
        }
    }
}

#[derive(Debug)]
pub enum Flushable<T> {
    Value(T),
    Flush(trigger::Sender),
}

impl<T> From<T> for Flushable<T> {
    fn from(v: T) -> Self {
        Flushable::Value(v)
    }
}

pub async fn release_pressure<T, M, E, Snk>(
    mut rx: circular_buffer::Receiver<M>,
    mut sink: Snk,
    yield_after: NonZeroUsize,
) -> Result<(), E>
where
    M: Into<Flushable<T>> + Send + Sync,
    Snk: ItemSender<T, E>,
{
    let mut iteration_count: usize = 0;
    let yield_mod = yield_after.get();
    while let Ok(value) = rx.recv().await {
        match value.into() {
            Flushable::Value(v) => {
                if let Err(e) = sink.send_item(v).await {
                    return Err(e);
                }
            }
            Flushable::Flush(tx) => {
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
