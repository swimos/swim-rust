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
use crate::agent::model::command::Commander;
use futures::future::BoxFuture;
use futures::{FutureExt, Stream, StreamExt};
use pin_utils::pin_mut;
use std::fmt::Debug;
use swim_runtime::routing::RoutingAddr;
use tracing::{event, Level};

pub struct CommandLaneUpdateTask<T> {
    commander: Commander<T>,
}

impl<T> CommandLaneUpdateTask<T>
where
    T: Send + Sync + Debug + 'static,
{
    pub fn new(commander: Commander<T>) -> Self {
        CommandLaneUpdateTask { commander }
    }
}

impl<T> LaneUpdate for CommandLaneUpdateTask<T>
where
    T: Clone + Send + Sync + Debug + 'static,
{
    type Msg = T;

    fn run_update<Messages, Err>(
        self,
        messages: Messages,
    ) -> BoxFuture<'static, Result<(), UpdateError>>
    where
        Messages: Stream<Item = Result<(RoutingAddr, Self::Msg), Err>> + Send + 'static,
        Err: Send + Debug,
        UpdateError: From<Err>,
    {
        async move {
            let CommandLaneUpdateTask { mut commander } = self;

            let messages = messages.fuse();
            pin_mut!(messages);

            while let Some(msg_result) = messages
                .next()
                .await
                .map(|result| result.map(|(_, msg)| msg))
            {
                match msg_result {
                    Ok(msg) => {
                        commander.command(msg).await;
                    }
                    Err(e) => {
                        event!(Level::ERROR, ?e);
                    }
                }
            }

            Ok(())
        }
        .boxed()
    }
}
