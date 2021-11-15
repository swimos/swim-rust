// Copyright 2015-2021 SWIM.AI inc.
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

pub mod keyed;

#[cfg(test)]
mod test;

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
