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

pub mod map;
#[cfg(test)]
mod test;

use futures::StreamExt;
use std::num::NonZeroUsize;
use swim_common::sink::item::ItemSender;
use utilities::sync::circular_buffer;

pub async fn release_pressure<T, E, Snk>(
    mut rx: circular_buffer::Receiver<T>,
    mut sink: Snk,
    yield_after: NonZeroUsize,
) -> Result<(), E>
where
    T: Send + Sync,
    Snk: ItemSender<T, E>,
{
    let mut iteration_count: usize = 0;
    let yield_mod = yield_after.get();
    while let Some(value) = rx.next().await {
        if let Err(e) = sink.send_item(value).await {
            return Err(e);
        } else {
            iteration_count = iteration_count.wrapping_add(1);
            if iteration_count % yield_mod == 0 {
                tokio::task::yield_now().await;
            }
        }
    }
    Ok(())
}
