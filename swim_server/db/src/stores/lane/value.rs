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

pub trait ValueStore<'a> {
    type Value: 'a;

    fn store(&self, value: Self::Value) -> Result<(), StoreError>;

    fn load(&self) -> Result<Self::Value, StoreError>;

    fn clear(&self) -> Result<(), StoreError>;
}

pub struct ValueLaneStore<D> {
    lane_uri: String,
    delegate: D,
}

impl<'a, D> ValueStore<'a> for ValueLaneStore<D>
where
    D: LaneStore<'a>,
{
    type Value = ();

    fn store(&self, _value: Self::Value) -> Result<(), StoreError> {
        unimplemented!()
    }

    fn load(&self) -> Result<Self::Value, StoreError> {
        unimplemented!()
    }

    fn clear(&self) -> Result<(), StoreError> {
        unimplemented!()
    }
}
