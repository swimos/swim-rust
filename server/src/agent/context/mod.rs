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

use crate::agent::{AgentContext, EffStream, Eff};
use futures::{Stream, StreamExt};
use futures::stream::once;
use std::sync::Arc;
use tokio::sync::mpsc;
use tokio::time::Duration;
use url::Url;
use swim_runtime::time::delay;
use futures::future::BoxFuture;

#[derive(Debug)]
pub struct ContextImpl<Agent> {
    agent_ref: Arc<Agent>,
    url: Url,
    scheduler: mpsc::Sender<EffStream>,
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
    pub fn new(
        agent_ref: Arc<Agent>,
        url: Url,
        scheduler: mpsc::Sender<EffStream>,
    ) -> Self {
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
    fn schedule<Str, Sch>(&self, effects: Str, schedule: Sch) -> BoxFuture<()>
    where
        Str: Stream<Item = Eff> + Send + 'static,
        Sch: Stream<Item = Duration> + Send + 'static,
    {
        let effects = StreamExt::boxed(schedule.flat_map(|dur| once(delay::delay_for(dur)))
            .zip(effects)
            .map(|(_, eff)| eff));

        let mut sender = self.scheduler.clone();
        Box::pin(async move {
            //TODO Handle this.
            let _ = sender.send(effects).await;
            ()
        })
    }

    fn agent(&self) -> &Agent {
        self.agent_ref.as_ref()
    }

    fn node_url(&self) -> &Url {
        &self.url
    }
}
