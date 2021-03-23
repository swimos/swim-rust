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

use crate::stores::lane::LaneStore;
use crate::StoreError;

pub trait MapStore<'a> {
    type Key: 'a;
    type Value: 'a;

    fn put(&self, key: Self::Key, value: Self::Value) -> Result<(), StoreError>;

    fn get(&self, key: Self::Key) -> Result<Option<Vec<u8>>, StoreError>;

    fn delete(&self, key: Self::Key) -> Result<bool, StoreError>;
}

pub struct MapLaneStore<D> {
    lane_uri: String,
    delegate: D,
}

impl<'a, D> MapStore<'a> for MapLaneStore<D>
where
    D: LaneStore<'a>,
{
    type Key = ();
    type Value = ();

    fn put(&self, _key: Self::Key, _value: Self::Value) -> Result<(), StoreError> {
        unimplemented!()
    }

    fn get(&self, _key: Self::Key) -> Result<Option<Vec<u8>>, StoreError> {
        unimplemented!()
    }

    fn delete(&self, _key: Self::Key) -> Result<bool, StoreError> {
        unimplemented!()
    }
}
