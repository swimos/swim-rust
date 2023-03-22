// Copyright 2015-2023 Swim Inc.
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

use std::collections::{BTreeMap, HashMap};

use crate::model::Endpoint;

pub struct SharedState {
    count: usize,
    links: HashMap<Endpoint, usize>,
    rev: BTreeMap<usize, Endpoint>,
}

impl Default for SharedState {
    fn default() -> Self {
        Self {
            count: 1,
            links: Default::default(),
            rev: Default::default(),
        }
    }
}

impl SharedState {
    pub fn insert(&mut self, endpoint: Endpoint) -> usize {
        let SharedState { count, links, rev } = self;
        let n = *count;
        *count += 1;
        links.insert(endpoint.clone(), n);
        rev.insert(n, endpoint);
        n
    }

    pub fn remove(&mut self, id: usize) {
        let SharedState { links, rev, .. } = self;
        if let Some(endpoint) = rev.remove(&id) {
            links.remove(&endpoint);
        }
    }

    pub fn list(&self) -> Vec<(usize, Endpoint)> {
        self.rev.iter().map(|(k, v)| (*k, v.clone())).collect()
    }

    pub fn has_id(&self, id: usize) -> bool {
        self.rev.contains_key(&id)
    }

    pub fn get_id(&self, endpoint: &Endpoint) -> Option<usize> {
        self.links.get(endpoint).copied()
    }
}
