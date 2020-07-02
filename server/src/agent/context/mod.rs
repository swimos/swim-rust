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

use crate::agent::{AgentContext, Eff};
use futures::future::BoxFuture;
use futures::sink::drain;
use futures::{FutureExt, Stream, StreamExt};
use std::future::Future;
use std::sync::Arc;
use swim_runtime::time::delay;
use tokio::sync::mpsc;
use tokio::time::Duration;
use url::Url;
use utilities::future::SwimStreamExt;

#[derive(Debug)]
pub struct ContextImpl<Agent> {
    agent_ref: Arc<Agent>,
    url: Url,
    scheduler: mpsc::Sender<Eff>,
}

impl<Agent> Clone for ContextImpl<Agent> {
    fn clone(&self) -> Self {
        ContextImpl {
            agent_ref: self.agent_ref.clone(),
            url: self.url.clone(),
            scheduler: self.scheduler.clone(),
        }
    }
}

impl<Agent> ContextImpl<Agent> {
    pub fn new(agent_ref: Arc<Agent>, url: Url, scheduler: mpsc::Sender<Eff>) -> Self {
        ContextImpl {
            agent_ref,
            url,
            scheduler,
        }
    }
}

impl<Agent> AgentContext<Agent> for ContextImpl<Agent>
where
    Agent: Send + Sync + 'static,
{
    fn schedule<Effect, Str, Sch>(&self, effects: Str, schedule: Sch) -> BoxFuture<()>
    where
        Effect: Future<Output = ()> + Send + 'static,
        Str: Stream<Item = Effect> + Send + 'static,
        Sch: Stream<Item = Duration> + Send + 'static,
    {
        let schedule_effect = schedule
            .zip(effects)
            .then(|(dur, eff)| async move {
                delay::delay_for(dur).await;
                eff.await;
            })
            .never_error()
            .forward(drain())
            .map(|_| ()) //Infallible is an empty type so we can drop the errors.
            .boxed();

        let mut sender = self.scheduler.clone();
        Box::pin(async move {
            //TODO Handle this.
            let _ = sender.send(schedule_effect).await;
        })
    }

    fn agent(&self) -> &Agent {
        self.agent_ref.as_ref()
    }

    fn node_url(&self) -> &Url {
        &self.url
    }
}
