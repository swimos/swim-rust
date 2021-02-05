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

use crate::agent::lane::channels::AgentExecutionConfig;
use futures::future::{ready, BoxFuture};
use futures::{FutureExt, TryFutureExt};
use std::num::NonZeroUsize;
use swim_common::topic::{MpscTopic, Topic, WatchTopic};
use tokio::sync::{mpsc, watch};
use utilities::errors::SwimResultExt;

#[derive(Debug, PartialEq, Eq, Clone, Default)]
pub struct Dropping;

#[derive(Debug, PartialEq, Eq, Clone)]
pub struct Queue(pub NonZeroUsize);

const DEFAULT_BUFFER: usize = 10;

fn default_buffer() -> NonZeroUsize {
    NonZeroUsize::new(DEFAULT_BUFFER).unwrap()
}

impl Default for Queue {
    fn default() -> Self {
        Queue(default_buffer())
    }
}

pub type BoxSupplier<T> = Box<dyn Supplier<T> + Send + Sync + 'static>;

pub trait SupplyLaneWatch<T: Clone>
where
    T: Send + Sync + 'static,
{
    type Sender: Supplier<T> + Send + Sync + 'static;
    type Topic: Topic<T> + Send + Sync + 'static;

    fn make_watch(&self, config: &AgentExecutionConfig) -> (Self::Sender, Self::Topic);
}

pub trait Supplier<T>
where
    T: Send + Sync + 'static,
{
    fn try_supply(&self, item: T) -> Result<(), ()>;

    fn supply(&self, item: T) -> BoxFuture<Result<(), ()>>;
}

impl<T> Supplier<T> for mpsc::Sender<T>
where
    T: Send + Sync + 'static,
{
    fn try_supply(&self, item: T) -> Result<(), ()> {
        self.try_send(item).discard_err()
    }

    fn supply(&self, item: T) -> BoxFuture<Result<(), ()>> {
        self.send(item).map_err(|_| ()).boxed()
    }
}

impl<T> SupplyLaneWatch<T> for Queue
where
    T: Clone + Send + Sync + 'static,
{
    type Sender = mpsc::Sender<T>;
    type Topic = MpscTopic<T>;

    fn make_watch(&self, config: &AgentExecutionConfig) -> (Self::Sender, Self::Topic) {
        let Queue(size) = self;
        let (tx, rx) = mpsc::channel(size.get());
        let (topic, _rec) = MpscTopic::new(rx, *size, config.yield_after);

        (tx, topic)
    }
}

impl<T> Supplier<T> for WatchSupplier<T>
where
    T: Send + Sync + 'static,
{
    fn try_supply(&self, item: T) -> Result<(), ()> {
        self.0.send(Some(item)).discard_err()
    }

    fn supply(&self, item: T) -> BoxFuture<Result<(), ()>> {
        ready(self.try_supply(item)).boxed()
    }
}

pub struct WatchSupplier<T>(watch::Sender<Option<T>>);

impl<T> SupplyLaneWatch<T> for Dropping
where
    T: Clone + Send + Sync + 'static,
{
    type Sender = WatchSupplier<T>;
    type Topic = WatchTopic<T>;

    fn make_watch(&self, _config: &AgentExecutionConfig) -> (Self::Sender, Self::Topic) {
        let (tx, rx) = watch::channel(None);
        let (topic, _rec) = WatchTopic::new(rx);

        (WatchSupplier(tx), topic)
    }
}
