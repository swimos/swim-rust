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

use std::collections::HashMap;

use crate::lanes::map::MapLaneEvent;

pub trait AgentItem {
    fn id(&self) -> u64;
}

pub trait ValueItem<T>: AgentItem {
    fn read_with_prev<F, R>(&self, f: F) -> R
    where
        F: FnOnce(Option<T>, &T) -> R;

    fn init(&self, value: T);
}

pub trait MapItem<K, V>: AgentItem {
    fn init(&self, map: HashMap<K, V>);

    fn read_with_prev<F, R>(&self, f: F) -> R
    where
        F: FnOnce(Option<MapLaneEvent<K, V>>, &HashMap<K, V>) -> R;
}
