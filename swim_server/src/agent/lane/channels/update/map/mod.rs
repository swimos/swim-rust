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

use crate::agent::lane::channels::update::{LaneUpdate, UpdateError};
use crate::agent::lane::model::map::MapLane;
use crate::routing::RoutingAddr;
use futures::future::BoxFuture;
use futures::{FutureExt, Stream, StreamExt};
use pin_utils::pin_mut;
use std::any::Any;
use std::fmt::Debug;
use stm::transaction::{RetryManager, TransactionRunner};
use swim_form::Form;
use swim_warp::map::MapUpdate;
use tracing::{event, Level};

#[cfg(test)]
mod tests;

/// Asynchronous task to apply a stream of [`MapUpdate`]s to a [`MapLane`].
pub struct MapLaneUpdateTask<K, V, F> {
    lane: MapLane<K, V>,
    retries: F,
}

impl<K, V, F, Ret> MapLaneUpdateTask<K, V, F>
where
    F: Fn() -> Ret,
    Ret: RetryManager,
{
    pub fn new(lane: MapLane<K, V>, retries: F) -> Self {
        MapLaneUpdateTask { lane, retries }
    }
}

const APPLYING_UPDATE: &str = "Applying map update.";

impl<K, V, F, Ret> LaneUpdate for MapLaneUpdateTask<K, V, F>
where
    K: Form + Any + Send + Sync + Debug,
    V: Any + Form + Send + Sync + Debug,
    F: Fn() -> Ret + Send + Sync + 'static,
    Ret: RetryManager + Send,
{
    type Msg = MapUpdate<K, V>;

    fn run_update<Messages, Err>(
        self,
        messages: Messages,
    ) -> BoxFuture<'static, Result<(), UpdateError>>
    where
        Messages: Stream<Item = Result<(RoutingAddr, Self::Msg), Err>> + Send + 'static,
        Err: Send + Debug,
        UpdateError: From<Err>,
    {
        let MapLaneUpdateTask { lane, retries } = self;
        async move {
            pin_mut!(messages);

            let mut runner = TransactionRunner::new(1, retries);
            while let Some(update_result) = messages.next().await {
                match update_result {
                    Ok((_, update)) => {
                        event!(Level::TRACE, message = APPLYING_UPDATE, ?update);
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
                            _ => {
                                panic!("Take and drop not yet supported.")
                            }
                        }
                    }
                    Err(err) => {
                        event!(Level::ERROR, ?err);
                    }
                }
            }
            Ok(())
        }
        .boxed()
    }
}
