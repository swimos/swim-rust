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

use crate::agent::context::ContextImpl;
use crate::agent::lane::lifecycle::LaneLifecycle;
use crate::agent::lane::model;
use crate::agent::lane::model::map::MapLaneEvent;
use crate::agent::lane::model::map::{MapLane, MapLaneWatch};
use crate::agent::lane::model::value::{ValueLane, ValueLaneWatch};
use futures::future::{ready, BoxFuture};
use futures::sink::drain;
use futures::stream::{once, repeat, unfold, BoxStream};
use futures::{FutureExt, Stream, StreamExt};
use futures_util::stream::FuturesUnordered;
use pin_utils::pin_mut;
use std::any::Any;
use std::future::Future;
use std::sync::Arc;
use std::time::Duration;
use swim_form::Form;
use tokio::sync::mpsc;
use url::Url;
use utilities::future::SwimStreamExt;
use std::num::NonZeroUsize;

mod context;
pub mod lane;
pub mod lifecycle;

pub trait SwimAgent: Sized {
    fn instantiate<Context: AgentContext<Self>>() -> (Self, Vec<Box<dyn LaneTasks<Self, Context>>>);
}

pub async fn run_agent<Agent>(url: Url, scheduler_buffer_size: NonZeroUsize)
where
    Agent: SwimAgent + Send + Sync + 'static,
{
    let (agent, tasks) = Agent::instantiate::<ContextImpl<Agent>>();
    let agent_ref = Arc::new(agent);
    let (tx, rx) = mpsc::channel(scheduler_buffer_size.get());
    let context = ContextImpl::new(agent_ref, url, tx);

    for lane_task in tasks.iter() {
        (**lane_task).start(&context).await;
    }

    let task_manager: FuturesUnordered<Eff> = FuturesUnordered::new();

    task_manager.push(rx.for_each_concurrent(None, |eff| eff).boxed());

    for lane_task in tasks.into_iter() {
        task_manager.push(lane_task.events(context.clone()));
    }

    task_manager
        .never_error()
        .forward(drain())
        .map(|_| ()) //Never is an empty type so we can discard the errors.
        .await
}

pub type Eff = BoxFuture<'static, ()>;
pub type EffStream = BoxStream<'static, ()>;

pub trait AgentContext<Agent> {
    fn schedule<Effect, Str, Sch>(&self, effects: Str, schedule: Sch) -> BoxFuture<()>
    where
        Effect: Future<Output = ()> + Send + 'static,
        Str: Stream<Item = Effect> + Send + 'static,
        Sch: Stream<Item = Duration> + Send + 'static;

    fn periodically<Fut, F>(&self, mut effect: F, interval: Duration) -> BoxFuture<()>
    where
        Fut: Future<Output = ()> + Send + 'static,
        F: FnMut() -> Fut + Send + 'static,
    {
        let effects = unfold((), move |_| ready(Some((effect(), ()))));
        let sch = repeat(interval);
        self.schedule(effects, sch)
    }

    fn after<Fut>(&self, effect: Fut, duration: Duration) -> BoxFuture<()>
    where
        Fut: Future<Output = ()> + Send + 'static,
    {
        self.schedule(once(ready(effect)), once(ready(duration)))
    }

    fn agent(&self) -> &Agent;

    fn node_url(&self) -> &Url;
}

pub trait LaneTasks<Agent, Context: AgentContext<Agent> + Sized + Send + Sync + 'static> {
    fn start<'a>(&'a self, context: &'a Context) -> BoxFuture<'a, ()>;

    fn events(self: Box<Self>, context: Context) -> BoxFuture<'static, ()>;
}

struct LifecycleTasks<L, S, P> {
    lifecycle: L,
    event_stream: S,
    projection: P,
}

struct ValueLifecycleTasks<L, S, P>(LifecycleTasks<L, S, P>);
struct MapLifecycleTasks<L, S, P>(LifecycleTasks<L, S, P>);

impl<Agent, Context, T, L, S, P> LaneTasks<Agent, Context> for ValueLifecycleTasks<L, S, P>
where
    Agent: 'static,
    Context: AgentContext<Agent> + Send + Sync + 'static,
    S: Stream<Item = Arc<T>> + Send + Sync + 'static,
    T: Any + Send + Sync,
    L: LaneLifecycle<ValueLane<T>, Agent>,
    L::WatchStrategy: ValueLaneWatch<T, View = S>,
    P: Fn(&Agent) -> &ValueLane<T> + Send + Sync + 'static,
{
    fn start<'a>(&'a self, context: &'a Context) -> BoxFuture<'a, ()> {
        let ValueLifecycleTasks(LifecycleTasks {
            lifecycle,
            projection,
            ..
        }) = self;
        let model = projection(context.agent());
        lifecycle.on_start(model, context)
    }

    fn events(self: Box<Self>, context: Context) -> BoxFuture<'static, ()> {
        async move {
            let ValueLifecycleTasks(LifecycleTasks {
                lifecycle,
                event_stream,
                projection,
            }) = *self;
            let model = projection(context.agent());
            pin_mut!(event_stream);
            while let Some(event) = event_stream.next().await {
                lifecycle.on_event(&event, &model, &context).await
            }
        }
        .boxed()
    }
}

pub fn make_value_lane<Agent, Context, T, L>(
    init: T,
    lifecycle: L,
    projection: impl Fn(&Agent) -> &ValueLane<T> + Send + Sync + 'static,
) -> (ValueLane<T>, impl LaneTasks<Agent, Context>)
where
    Agent: 'static,
    Context: AgentContext<Agent> + Send + Sync + 'static,
    T: Any + Send + Sync,
    L: LaneLifecycle<ValueLane<T>, Agent>,
    L::WatchStrategy: ValueLaneWatch<T>,
{
    let (lane, event_stream) = model::value::make_lane_model(init, lifecycle.create_strategy());

    let tasks = ValueLifecycleTasks(LifecycleTasks {
        lifecycle,
        event_stream,
        projection,
    });
    (lane, tasks)
}

impl<Agent, Context, K, V, L, S, P> LaneTasks<Agent, Context> for MapLifecycleTasks<L, S, P>
where
    Agent: 'static,
    Context: AgentContext<Agent> + Send + Sync + 'static,
    S: Stream<Item = MapLaneEvent<K, V>> + Send + Sync + 'static,
    K: Any + Form + Send + Sync,
    V: Any + Send + Sync,
    L: LaneLifecycle<MapLane<K, V>, Agent>,
    L::WatchStrategy: MapLaneWatch<K, V, View = S>,
    P: Fn(&Agent) -> &MapLane<K, V> + Send + Sync + 'static,
{
    fn start<'a>(&'a self, context: &'a Context) -> BoxFuture<'a, ()> {
        let MapLifecycleTasks(LifecycleTasks {
            lifecycle,
            projection,
            ..
        }) = self;
        let model = projection(context.agent());
        lifecycle.on_start(model, context)
    }

    fn events(self: Box<Self>, context: Context) -> BoxFuture<'static, ()> {
        async move {
            let MapLifecycleTasks(LifecycleTasks {
                lifecycle,
                event_stream,
                projection,
            }) = *self;
            let model = projection(context.agent()).clone();
            pin_mut!(event_stream);
            while let Some(event) = event_stream.next().await {
                lifecycle.on_event(&event, &model, &context).await
            }
        }
        .boxed()
    }
}

pub fn make_map_lane<Agent, Context, K, V, L, P>(
    lifecycle: L,
    projection: impl Fn(&Agent) -> &MapLane<K, V> + Send + Sync + 'static,
) -> (MapLane<K, V>, impl LaneTasks<Agent, Context>)
where
    Agent: 'static,
    Context: AgentContext<Agent> + Send + Sync + 'static,
    K: Any + Form + Send + Sync,
    V: Any + Send + Sync,
    L: LaneLifecycle<MapLane<K, V>, Agent>,
    L::WatchStrategy: MapLaneWatch<K, V>,
    P: Fn(&Agent) -> &MapLane<K, V>,
{
    let (lane, event_stream) = model::map::make_lane_model(lifecycle.create_strategy());

    let tasks = MapLifecycleTasks(LifecycleTasks {
        lifecycle,
        event_stream,
        projection,
    });
    (lane, tasks)
}
