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

use crate::agent::lane::channels::UpdateError;
use crate::agent::lane::model::map::{MapLane, MapUpdate};
use futures::{Stream, StreamExt};
use pin_utils::pin_mut;
use std::any::Any;
use std::fmt::Debug;
use stm::transaction::{RetryManager, TransactionRunner};
use swim_form::Form;

pub struct MapLaneUpdateTask<K, V, F> {
    lane: MapLane<K, V>,
    retries: F,
}

impl<K, V, F> MapLaneUpdateTask<K, V, F>
where
    F: Fn(),
    F::Output: RetryManager,
{
    pub fn new(lane: MapLane<K, V>, retries: F) -> Self {
        MapLaneUpdateTask { lane, retries }
    }
}

impl<K, V, F, Ret> MapLaneUpdateTask<K, V, F>
where
    K: Form + Send + Sync + Debug,
    V: Any + Send + Sync + Debug,
    F: Fn() -> Ret,
    Ret: RetryManager,
{
    pub async fn run<Updates>(self, updates: Updates) -> Result<(), UpdateError>
    where
        Updates: Stream<Item = MapUpdate<K, V>>,
    {
        let MapLaneUpdateTask { lane, retries } = self;

        pin_mut!(updates);

        let mut runner = TransactionRunner::new(1, retries);
        while let Some(update) = updates.next().await {
            match update {
                MapUpdate::Update(key, value) => {
                    lane.update_direct(key, value)
                        .apply_with(&mut runner)
                        .await?;
                }
                MapUpdate::Remove(key) => {
                    lane.remove_direct(key).apply_with(&mut runner).await?;
                }
                MapUpdate::Clear => {
                    lane.clear_direct().apply_with(&mut runner).await?;
                }
            }
        }
        Ok(())
    }
}
