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
use std::hash::Hash;

/// Enumeration of the possible inputs to a map lane event handler.
#[derive(Debug, Clone)]
pub enum MapLaneEvent<K, V> {
    Clear(HashMap<K, V>),
    Update(K, Option<V>),
    Remove(K, V),
}

impl<K, V> PartialEq for MapLaneEvent<K, V>
where
    K: Eq + Hash,
    V: PartialEq,
{
    fn eq(&self, other: &Self) -> bool {
        match (self, other) {
            (Self::Clear(l0), Self::Clear(r0)) => l0 == r0,
            (Self::Update(l0, l1), Self::Update(r0, r1)) => l0 == r0 && l1 == r1,
            (Self::Remove(l0, l1), Self::Remove(r0, r1)) => l0 == r0 && l1 == r1,
            _ => false,
        }
    }
}

impl<K, V> Eq for MapLaneEvent<K, V>
where
    K: Eq + Hash,
    V: Eq,
{
    fn assert_receiver_is_total_eq(&self) {}
}
