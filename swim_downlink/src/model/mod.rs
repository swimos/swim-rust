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
use std::sync::Arc;

use tokio::sync::{mpsc, watch};

use crate::model::lifecycle::BasicMapDownlinkLifecycle;
use lifecycle::{
    BasicEventDownlinkLifecycle, BasicValueDownlinkLifecycle, EventDownlinkLifecycle,
    ValueDownlinkLifecycle,
};
use swim_api::protocol::map::MapMessage;

pub mod lifecycle;

pub struct ValueDownlinkModel<T, LC> {
    pub set_value: mpsc::Receiver<T>,
    pub get_value: watch::Sender<Arc<T>>,
    pub lifecycle: LC,
}

pub struct EventDownlinkModel<T, LC> {
    _type: PhantomData<T>,
    pub lifecycle: LC,
}

impl<T, LC> ValueDownlinkModel<T, LC> {
    pub fn new(
        set_value: mpsc::Receiver<T>,
        get_value: watch::Sender<Arc<T>>,
        lifecycle: LC,
    ) -> Self {
        ValueDownlinkModel {
            set_value,
            get_value,
            lifecycle,
        }
    }
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
    pub actions: mpsc::Receiver<MapMessage<K, V>>,
    pub lifecycle: LC,
}

impl<K, V, LC> MapDownlinkModel<K, V, LC> {
    pub fn new(
        actions: mpsc::Receiver<MapMessage<K, V>>,
        lifecycle: LC,
    ) -> MapDownlinkModel<K, V, LC> {
        MapDownlinkModel { actions, lifecycle }
    }
}

pub type DefaultValueDownlinkModel<T> = ValueDownlinkModel<T, BasicValueDownlinkLifecycle<T>>;

pub type DefaultEventDownlinkModel<T> = EventDownlinkModel<T, BasicEventDownlinkLifecycle<T>>;

pub type DefaultMapDownlinkModel<K, V> = MapDownlinkModel<K, V, BasicMapDownlinkLifecycle<K, V>>;

pub fn value_downlink<T>(
    set_value: mpsc::Receiver<T>,
    get_value: watch::Sender<Arc<T>>,
) -> DefaultValueDownlinkModel<T> {
    ValueDownlinkModel {
        set_value,
        get_value,
        lifecycle: Default::default(),
    }
}

pub fn event_downlink<T>() -> DefaultEventDownlinkModel<T> {
    EventDownlinkModel {
        _type: PhantomData,
        lifecycle: Default::default(),
    }
}

pub fn map_downlink<K, V>(
    actions: mpsc::Receiver<MapMessage<K, V>>,
) -> DefaultMapDownlinkModel<K, V> {
    MapDownlinkModel::new(actions, Default::default())
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
            get_value,
            lifecycle,
        } = self;

        ValueDownlinkModel {
            set_value,
            get_value,
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
