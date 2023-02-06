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
    BasicEventDownlinkLifecycle, BasicValueDownlinkLifecycle, EventDownlinkLifecycle,
    ValueDownlinkLifecycle,
};

pub mod lifecycle;

pub struct ValueDownlinkModel<T, LC> {
    pub set_value: mpsc::Receiver<T>,
    pub lifecycle: LC,
}

pub struct EventDownlinkModel<T, LC> {
    _type: PhantomData<T>,
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

impl<T, LC> EventDownlinkModel<T, LC> {
    pub fn new(lifecycle: LC) -> Self {
        EventDownlinkModel {
            _type: PhantomData,
            lifecycle,
        }
    }
}

pub type DefaultValueDownlinkModel<T> = ValueDownlinkModel<T, BasicValueDownlinkLifecycle<T>>;

pub type DefaultEventDownlinkModel<T> = EventDownlinkModel<T, BasicEventDownlinkLifecycle<T>>;

pub fn value_downlink<T>(set_value: mpsc::Receiver<T>) -> DefaultValueDownlinkModel<T> {
    ValueDownlinkModel {
        set_value,
        lifecycle: Default::default(),
    }
}

pub fn event_downlink<T>() -> DefaultEventDownlinkModel<T> {
    EventDownlinkModel {
        _type: PhantomData,
        lifecycle: Default::default(),
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
