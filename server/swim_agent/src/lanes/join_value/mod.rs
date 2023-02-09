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

use std::hash::Hash;
use std::{cell::RefCell, collections::HashMap};

use bytes::BytesMut;
use swim_form::structural::write::StructuralWritable;

use crate::{
    agent_model::WriteResult,
    event_handler::{ActionContext, HandlerAction, StepResult},
    item::{AgentItem, MapItem},
    meta::AgentMetadata,
};

use super::{map::MapLaneEvent, Lane, MapLane};

mod downlink;
pub mod lifecycle;

enum DownlinkStatus {
    Pending,
    Linked,
}

pub struct JoinValueLane<K, V> {
    inner: MapLane<K, V>,
    keys: RefCell<HashMap<K, DownlinkStatus>>,
}

impl<K, V> JoinValueLane<K, V> {
    pub fn new(id: u64) -> Self {
        JoinValueLane {
            inner: MapLane::new(id, HashMap::new()),
            keys: RefCell::new(HashMap::new()),
        }
    }
}

impl<K, V> AgentItem for JoinValueLane<K, V> {
    fn id(&self) -> u64 {
        self.inner.id()
    }
}

impl<K, V> Lane for JoinValueLane<K, V>
where
    K: Clone + Eq + Hash + StructuralWritable,
    V: StructuralWritable,
{
    fn write_to_buffer(&self, buffer: &mut BytesMut) -> WriteResult {
        self.inner.write_to_buffer(buffer)
    }
}

/// [`HandlerAction`] that attempts to add a new downlink to a [`JoinValueLane`].
pub struct AddDownlinkAction<Context, K, V> {
    projection: fn(&Context) -> &JoinValueLane<K, V>,
    key: K,
}

impl<Context, K, V> HandlerAction<Context> for AddDownlinkAction<Context, K, V>
where
    K: Clone + Eq + Hash + StructuralWritable,
    V: StructuralWritable,
{
    type Completion = ();

    fn step(
        &mut self,
        action_context: ActionContext<Context>,
        meta: AgentMetadata,
        context: &Context,
    ) -> StepResult<Self::Completion> {
        todo!()
    }
}

pub struct RemoteLane<'a> {
    pub host: Option<&'a str>,
    pub node: &'a str,
    pub lane: &'a str,
}

impl<K, V> MapItem<K, V> for JoinValueLane<K, V>
where
    K: Eq + Hash + Clone,
{
    fn init(&self, map: HashMap<K, V>) {
        self.inner.init(map)
    }

    fn read_with_prev<F, R>(&self, f: F) -> R
    where
        F: FnOnce(Option<MapLaneEvent<K, V>>, &HashMap<K, V>) -> R,
    {
        self.inner.read_with_prev(f)
    }
}

#[derive(Clone, Copy, PartialEq, Eq, Default)]
pub enum LinkClosedResponse {
    Retry,
    #[default]
    Abandon,
    Delete,
}
