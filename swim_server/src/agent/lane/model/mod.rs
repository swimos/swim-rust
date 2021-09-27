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

use futures::Stream;
use std::any::type_name;
use std::fmt::{Debug, Formatter};
use std::marker::PhantomData;
use std::sync::Arc;
use stm::var::observer::{Observer, ObserverStream, ObserverSubscriber};
use swim_utilities::sync::topic::{self, ReceiverStream};

pub mod action;
pub mod command;
pub mod demand;
pub mod demand_map;
pub mod map;
pub mod supply;
pub mod value;

const COMMANDED_AFTER_STOP: &str = "Lane commanded after the agent stopped.";

pub trait DeferredSubscription<T>: Send + Sync + 'static {
    type View: Stream<Item = T> + Send + 'static;

    fn subscribe(&self) -> Option<Self::View>;
}

impl<T: Send + Sync + 'static> DeferredSubscription<Arc<T>> for ObserverSubscriber<T> {
    type View = ObserverStream<T>;

    fn subscribe(&self) -> Option<Self::View> {
        ObserverSubscriber::subscribe(self)
            .ok()
            .map(Observer::into_stream)
    }
}

impl<T: Clone + Send + Sync + 'static> DeferredSubscription<T> for topic::Subscriber<T> {
    type View = ReceiverStream<T>;

    fn subscribe(&self) -> Option<Self::View> {
        self.subscribe().ok().map(topic::Receiver::into_stream)
    }
}

struct TypeOf<T: ?Sized>(PhantomData<T>);

fn type_of<T: ?Sized>() -> TypeOf<T> {
    TypeOf(PhantomData)
}

impl<T: ?Sized> Debug for TypeOf<T> {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", type_name::<T>())
    }
}
