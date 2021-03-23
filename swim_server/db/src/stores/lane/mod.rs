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

use crate::stores::lane::map::MapStore;
use crate::stores::lane::value::ValueStore;
use crate::{StoreEngine, StoreError};
use serde::Serialize;
use std::sync::Arc;

pub mod map;
pub mod value;

pub trait LaneStore<'a> {
    fn map_lane_store<I, S, K, V>(&self, lane: I) -> S
    where
        S: MapStore<'a, K, V> + FromDelegate<'a, Delegate = Self>,
        I: ToString,
        K: Serialize + 'a,
        V: Serialize + 'a;

    fn value_lane_store<I, S, V>(&self, lane: I) -> S
    where
        S: ValueStore<'a, V> + FromDelegate<'a, Delegate = Self>,
        I: ToString,
        V: Serialize + 'a;
}

pub enum LaneKey<'a> {
    Map { lane_uri: &'a String, key: Vec<u8> },
    Value { lane_uri: String },
}

pub struct SwimLaneStore<D> {
    delegate: Arc<D>,
}

impl<D> Clone for SwimLaneStore<D> {
    fn clone(&self) -> Self {
        SwimLaneStore {
            delegate: self.delegate.clone(),
        }
    }
}

impl<D> SwimLaneStore<D> {
    pub fn new(delegate: D) -> SwimLaneStore<D> {
        SwimLaneStore {
            delegate: Arc::new(delegate),
        }
    }
}

pub trait FromDelegate<'a> {
    type Delegate: LaneStore<'a>;

    fn from_delegate(delegate: Self::Delegate, address: String) -> Self;
}

impl<'a, D> LaneStore<'a> for SwimLaneStore<D>
where
    D: StoreEngine<'a>,
{
    fn map_lane_store<I, S, K, V>(&self, lane: I) -> S
    where
        S: MapStore<'a, K, V> + FromDelegate<'a, Delegate = Self>,
        I: ToString,
        K: Serialize + 'a,
        V: Serialize + 'a,
    {
        S::from_delegate((*self).clone(), lane.to_string())
    }

    fn value_lane_store<I, S, V>(&self, lane: I) -> S
    where
        S: ValueStore<'a, V> + FromDelegate<'a, Delegate = Self>,
        I: ToString,
        V: Serialize + 'a,
    {
        S::from_delegate((*self).clone(), lane.to_string())
    }
}

impl<'a, D> StoreEngine<'a> for SwimLaneStore<D>
where
    D: StoreEngine<'a>,
{
    type Key = &'a LaneKey<'a>;
    type Value = &'a [u8];
    type Error = StoreError;

    fn put(&self, key: Self::Key, value: Self::Value) -> Result<(), Self::Error> {
        unimplemented!()
    }

    fn get(&self, _key: Self::Key) -> Result<Option<Vec<u8>>, Self::Error> {
        unimplemented!()
    }

    fn delete(&self, _key: Self::Key) -> Result<bool, Self::Error> {
        unimplemented!()
    }
}

pub fn serialize_into_slice<'a, S, F, O, E>(engine: &E, obj: &S, f: F) -> Result<O, StoreError>
where
    S: Serialize,
    E: StoreEngine<'a>,
    F: Fn(&E, &[u8]) -> Result<O, StoreError>,
{
    Ok(f(engine, serialize(obj)?.as_slice())?)
}

pub fn serialize_into_vec<'a, S, F, O, E>(engine: &E, obj: &S, f: F) -> Result<O, StoreError>
where
    S: Serialize,
    E: StoreEngine<'a>,
    F: Fn(&E, Vec<u8>) -> Result<O, StoreError>,
{
    Ok(f(engine, serialize(obj)?)?)
}

pub fn serialize<S: Serialize>(obj: &S) -> Result<Vec<u8>, StoreError> {
    bincode::serialize(obj).map_err(StoreError::from)
}
