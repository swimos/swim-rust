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

use std::num::NonZeroUsize;
use tokio::sync::mpsc;
use futures::future::join;
use futures::{FutureExt, StreamExt};
use crate::agent::EffStream;
use std::future::Future;

pub struct ScheduleTask {
    receiver: mpsc::Receiver<EffStream>,
    buffer_size: NonZeroUsize,
}

impl ScheduleTask {

    pub fn new(receiver: mpsc::Receiver<EffStream>, buffer_size: NonZeroUsize) -> Self {
        ScheduleTask {
            receiver, buffer_size,
        }
    }

    pub fn tasks(self) -> (impl Future<Output = ()> + Send + 'static, impl Future<Output = ()> + Send + 'static) {
        let ScheduleTask {
            mut receiver,
            buffer_size,
        } = self;

        let (mut tx, rx) = mpsc::channel(buffer_size.get());

        let accept_streams = async move {
            loop {
                if let Some(str) = receiver.next().await {
                    if tx.send(str.for_each(|eff| eff)).await.is_err() {
                        break;
                    }
                } else {
                    break;
                }
            }
        };

        let run_effects = rx.for_each_concurrent(None, |eff| eff);

        (accept_streams, run_effects)
    }

    pub async fn run(self) {
        let (acc, run_eff) = self.tasks();
        join(acc, run_eff).map(|_| ()).await
    }
}
