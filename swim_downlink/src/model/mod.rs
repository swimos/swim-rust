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

use tokio::sync::mpsc;

use swim_api::lifecycle::downlink::{
    for_value_downlink, StatelessValueDownlinkLifecycle, ValueDownlinkLifecycle,
};
use swim_api::lifecycle::NoHandler;

pub struct ValueDownlinkModel<T, LC> {
    pub set_value: mpsc::Receiver<T>,
    pub lifecycle: LC,
}

pub type DefaultValueDownlinkModel<T> = ValueDownlinkModel<
    T,
    StatelessValueDownlinkLifecycle<T, NoHandler, NoHandler, NoHandler, NoHandler, NoHandler>,
>;

pub fn value_downlink<T>(set_value: mpsc::Receiver<T>) -> DefaultValueDownlinkModel<T> {
    ValueDownlinkModel {
        set_value,
        lifecycle: for_value_downlink::<T>(),
    }
}

impl<T, LC> ValueDownlinkModel<T, LC>
where
    LC: for<'a> ValueDownlinkLifecycle<'a, T>,
{
    pub fn with_lifecycle<F, LC2>(self, f: F) -> ValueDownlinkModel<T, LC2>
    where
        F: Fn(LC) -> LC2,
        LC2: for<'a> ValueDownlinkLifecycle<'a, T>,
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
