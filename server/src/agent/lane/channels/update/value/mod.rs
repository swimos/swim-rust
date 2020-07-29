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

use crate::agent::lane::model::value::ValueLane;
use futures::{Stream, StreamExt};
use std::any::Any;

/// Asynchronous task to set a stream of values into a [`ValueLane`].
pub struct ValueLaneUpdateTask<T> {
    lane: ValueLane<T>,
}

impl<T> ValueLaneUpdateTask<T> {
    pub fn new(lane: ValueLane<T>) -> Self {
        ValueLaneUpdateTask { lane }
    }
}

impl<T> ValueLaneUpdateTask<T>
where
    T: Any + Send + Sync,
{
    pub async fn run<Updates>(self, updates: Updates)
    where
        Updates: Stream<Item = T>,
    {
        updates.for_each(|value| self.lane.store(value)).await;
    }
}
