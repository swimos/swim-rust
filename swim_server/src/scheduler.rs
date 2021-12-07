// Copyright 2015-2021 Swim Inc.
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

use crate::agent::Eff;
use futures::future::BoxFuture;
use futures::sink::drain;
use futures::FutureExt;
use futures::{Stream, StreamExt};
use std::future::Future;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::Duration;
use swim_async_runtime::time::clock::Clock;
use swim_utilities::future::SwimStreamExt;
use swim_utilities::trigger;
use tokio::sync::mpsc;
use tracing::{event, span, Level};
use tracing_futures::Instrument;

const SCHEDULE: &str = "Schedule";
const SCHED_TRIGGERED: &str = "Schedule triggered";
const SCHED_STOPPED: &str = "Scheduler unexpectedly stopped";
const WAITING: &str = "Schedule waiting";

#[derive(Debug)]
pub struct SchedulerContext<Clk> {
    scheduler: mpsc::Sender<Eff>,
    schedule_count: Arc<AtomicU64>,
    clock: Clk,
    stop_signal: trigger::Receiver,
}

impl<Clk: Clock> SchedulerContext<Clk> {
    pub(super) fn new(
        scheduler: mpsc::Sender<Eff>,
        clock: Clk,
        stop_signal: trigger::Receiver,
    ) -> Self {
        SchedulerContext {
            scheduler,
            schedule_count: Default::default(),
            clock,
            stop_signal,
        }
    }

    pub fn schedule<Effect, Str, Sch>(&self, effects: Str, schedule: Sch) -> BoxFuture<()>
    where
        Effect: Future<Output = ()> + Send + 'static,
        Str: Stream<Item = Effect> + Send + 'static,
        Sch: Stream<Item = Duration> + Send + 'static,
    {
        let index = self.schedule_count.fetch_add(1, Ordering::Relaxed);

        let clock = self.clock.clone();
        let schedule_effect = schedule
            .zip(effects)
            .then(move |(dur, eff)| {
                event!(Level::TRACE, WAITING, ?dur);
                let delay_fut = clock.delay(dur);
                async move {
                    delay_fut.await;
                    event!(Level::TRACE, SCHED_TRIGGERED);
                    eff.await;
                }
            })
            .take_until(self.stop_signal.clone())
            .never_error()
            .forward(drain())
            .map(|_| ()) //Never is an empty type so we can drop the errors.
            .instrument(span!(Level::DEBUG, SCHEDULE, index))
            .boxed();

        let sender = self.scheduler.clone();
        Box::pin(async move {
            //TODO Handle this.
            if sender.send(schedule_effect).await.is_err() {
                event!(Level::ERROR, SCHED_STOPPED)
            }
        })
    }
}

impl<Clk> SchedulerContext<Clk> {
    pub fn stop_rx(&self) -> trigger::Receiver {
        self.stop_signal.clone()
    }

    pub fn schedule_tx(&self) -> mpsc::Sender<Eff> {
        self.scheduler.clone()
    }
}

impl<Clk: Clone> Clone for SchedulerContext<Clk> {
    fn clone(&self) -> Self {
        SchedulerContext {
            scheduler: self.scheduler.clone(),
            schedule_count: self.schedule_count.clone(),
            clock: self.clock.clone(),
            stop_signal: self.stop_signal.clone(),
        }
    }
}
