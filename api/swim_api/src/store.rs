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

use std::fmt::{Display, Formatter};

use bytes::BytesMut;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum StoreKind {
    Value,
    Map,
}

impl Display for StoreKind {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            StoreKind::Value => write!(f, "Value"),
            StoreKind::Map => write!(f, "Map"),
        }
    }
}

pub struct PersistenceError;

pub trait ReadMapIterator {
    fn next<F>(&mut self, f: F) -> Result<(), PersistenceError>
    where
        F: FnOnce(Option<(&[u8], &[u8])>);
}

pub trait MapPersistence<'a> {
    type MapIt: ReadMapIterator + 'a;

    fn read_map(&'a self, name: &str) -> Result<Self::MapIt, PersistenceError>;
}

pub trait NodePersistence: for<'a> MapPersistence<'a> {
    fn get_value(&self, name: &str, buffer: &mut BytesMut) -> Result<(), PersistenceError>;

    fn put_value(&self, name: &str, value: &[u8]) -> Result<(), PersistenceError>;

    fn update_map(&self, name: &str, key: &[u8], value: &[u8]) -> Result<(), PersistenceError>;
    fn remove_map(&self, name: &str, key: &[u8], value: &[u8]) -> Result<(), PersistenceError>;

    fn clear(&self, name: &str) -> Result<(), PersistenceError>;
}

pub trait PlanePersistence {
    type Node: NodePersistence;

    fn node_store(&self, node_uri: &str) -> Result<Self::Node, PersistenceError>;
}
