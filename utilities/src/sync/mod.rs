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

use futures::future::ready;
use futures::stream::unfold;
use futures::{Stream, StreamExt};
use tokio::sync::{broadcast, watch};

pub mod promise;
pub mod rwlock;
pub mod trigger;

pub fn watch_option_rx_to_stream<T>(
    rx: watch::Receiver<Option<T>>,
) -> impl Stream<Item = T> + Send + 'static
where
    T: Send + Sync + Clone + 'static,
{
    unfold(rx, |mut rx| async move {
        loop {
            if rx.changed().await.is_err() {
                break None;
            } else {
                let current = (*rx.borrow()).clone();
                if let Some(v) = current {
                    break Some((v, rx));
                }
            }
        }
    })
}

pub fn broadcast_rx_to_stream<T>(
    rx: broadcast::Receiver<T>,
) -> impl Stream<Item = T> + Send + 'static
where
    T: Clone + Send + 'static,
{
    rx.into_stream().filter_map(|r| ready(r.ok()))
}
