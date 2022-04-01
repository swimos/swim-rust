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

use std::marker::PhantomData;

use tokio::sync::mpsc;

use lifecycle::{
    for_event_downlink, for_map_downlink, for_value_downlink, BasicEventDownlinkLifecycle,
    BasicMapDownlinkLifecycle, BasicValueDownlinkLifecycle, EventDownlinkLifecycle,
    MapDownlinkLifecycle, ValueDownlinkLifecycle,
};
use swim_api::handlers::NoHandler;
use swim_api::protocol::map::MapMessage;

pub mod lifecycle;

pub struct ValueDownlinkModel<T, LC> {
    pub set_value: mpsc::Receiver<T>,
    pub lifecycle: LC,
}

impl<T, LC> ValueDownlinkModel<T, LC> {
    pub fn new(set_value: mpsc::Receiver<T>, lifecycle: LC) -> Self {
        ValueDownlinkModel {
            set_value,
            lifecycle,
        }
    }
}

pub struct EventDownlinkModel<T, LC> {
    _type: PhantomData<T>,
    pub lifecycle: LC,
}

impl<T, LC> EventDownlinkModel<T, LC> {
    pub fn new(lifecycle: LC) -> Self {
        EventDownlinkModel {
            _type: PhantomData,
            lifecycle,
        }
    }
}

pub struct MapDownlinkModel<K, V, LC> {
    pub event_stream: mpsc::Receiver<MapMessage<K, V>>,
    pub lifecycle: LC,
}

impl<K, V, LC> MapDownlinkModel<K, V, LC> {
    pub fn new(event_stream: mpsc::Receiver<MapMessage<K, V>>, lifecycle: LC) -> Self {
        MapDownlinkModel {
            event_stream,
            lifecycle,
        }
    }
}

pub type DefaultValueDownlinkModel<T> = ValueDownlinkModel<
    T,
    BasicValueDownlinkLifecycle<T, NoHandler, NoHandler, NoHandler, NoHandler, NoHandler>,
>;

pub type DefaultEventDownlinkModel<T> =
    EventDownlinkModel<T, BasicEventDownlinkLifecycle<T, NoHandler, NoHandler, NoHandler>>;

pub type DefaultMapDownlinkModel<K, V> = MapDownlinkModel<
    K,
    V,
    BasicMapDownlinkLifecycle<
        K,
        V,
        NoHandler,
        NoHandler,
        NoHandler,
        NoHandler,
        NoHandler,
        NoHandler,
        NoHandler,
    >,
>;

pub fn value_downlink<T>(set_value: mpsc::Receiver<T>) -> DefaultValueDownlinkModel<T> {
    ValueDownlinkModel {
        set_value,
        lifecycle: for_value_downlink::<T>(),
    }
}

pub fn event_downlink<T>() -> DefaultEventDownlinkModel<T> {
    EventDownlinkModel {
        _type: PhantomData,
        lifecycle: for_event_downlink::<T>(),
    }
}

pub fn map_downlink<K, V>(
    event_stream: mpsc::Receiver<MapMessage<K, V>>,
) -> DefaultMapDownlinkModel<K, V> {
    MapDownlinkModel {
        event_stream,
        lifecycle: for_map_downlink::<K, V>(),
    }
}

impl<T, LC> ValueDownlinkModel<T, LC>
where
    LC: ValueDownlinkLifecycle<T>,
{
    pub fn with_lifecycle<F, LC2>(self, f: F) -> ValueDownlinkModel<T, LC2>
    where
        F: Fn(LC) -> LC2,
        LC2: ValueDownlinkLifecycle<T>,
    {
        let ValueDownlinkModel {
            set_value,
            lifecycle,
        } = self;

        ValueDownlinkModel {
            set_value,
            lifecycle: f(lifecycle),
        }
    }
}

impl<T, LC> EventDownlinkModel<T, LC>
where
    LC: EventDownlinkLifecycle<T>,
{
    pub fn with_lifecycle<F, LC2>(self, f: F) -> EventDownlinkModel<T, LC2>
    where
        F: Fn(LC) -> LC2,
        LC2: EventDownlinkLifecycle<T>,
    {
        let EventDownlinkModel { lifecycle, .. } = self;

        EventDownlinkModel {
            _type: PhantomData,
            lifecycle: f(lifecycle),
        }
    }
}

impl<K, V, LC> MapDownlinkModel<K, V, LC>
where
    LC: MapDownlinkLifecycle<K, V>,
{
    pub fn with_lifecycle<F, LC2>(self, f: F) -> MapDownlinkModel<K, V, LC2>
    where
        F: Fn(LC) -> LC2,
        LC2: MapDownlinkLifecycle<K, V>,
    {
        let MapDownlinkModel {
            event_stream,
            lifecycle,
        } = self;

        MapDownlinkModel {
            event_stream,
            lifecycle: f(lifecycle),
        }
    }
}
