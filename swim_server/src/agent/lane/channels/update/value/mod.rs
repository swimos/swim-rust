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

#[cfg(test)]
mod tests;

use crate::agent::lane::channels::update::{LaneUpdate, UpdateError};
use crate::agent::lane::model::value::ValueLane;
use futures::future::BoxFuture;
use futures::{FutureExt, Stream, StreamExt};
use pin_utils::pin_mut;
use std::any::Any;
use std::fmt::Debug;
use tracing::{event, Level};

/// Asynchronous task to set a stream of values into a [`ValueLane`].
pub struct ValueLaneUpdateTask<T> {
    lane: ValueLane<T>,
}

impl<T> ValueLaneUpdateTask<T> {
    pub fn new(lane: ValueLane<T>) -> Self {
        ValueLaneUpdateTask { lane }
    }
}

const APPLYING_UPDATE: &str = "Applying value update.";

impl<T> LaneUpdate for ValueLaneUpdateTask<T>
where
    T: Any + Send + Sync + Debug,
{
    type Msg = T;

    fn run_update<Messages, Err>(
        self,
        messages: Messages,
    ) -> BoxFuture<'static, Result<(), UpdateError>>
    where
        Messages: Stream<Item = Result<Self::Msg, Err>> + Send + 'static,
        Err: Send,
        UpdateError: From<Err>,
    {
        let ValueLaneUpdateTask { lane } = self;
        async move {
            pin_mut!(messages);
            while let Some(msg_result) = messages.next().await {
                let msg = msg_result?;
                event!(Level::TRACE, message = APPLYING_UPDATE, value = ?msg);
                lane.store(msg).await;
            }
            Ok(())
        }
        .boxed()
    }
}
