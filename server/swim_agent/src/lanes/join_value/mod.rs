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
use swim_form::Form;
use swim_model::address::Address;
use swim_model::Text;

use crate::agent_model::downlink::OpenValueDownlinkAction;
use crate::{
    agent_model::WriteResult,
    event_handler::{ActionContext, HandlerAction, StepResult},
    item::{AgentItem, MapItem},
    meta::AgentMetadata,
};

use self::downlink::JoinValueDownlink;
use self::lifecycle::JoinValueLaneLifecycle;

use super::{map::MapLaneEvent, Lane, MapLane};

mod default_lifecycle;
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
pub struct AddDownlinkAction<Context, K, V, LC> {
    projection: fn(&Context) -> &JoinValueLane<K, V>,
    key: Option<K>,
    inner: Option<OpenValueDownlinkAction<V, JoinValueDownlink<K, V, LC, Context>>>,
}

impl<Context, K, V, LC> AddDownlinkAction<Context, K, V, LC>
where
    K: Clone,
{
    pub fn new(
        projection: fn(&Context) -> &JoinValueLane<K, V>,
        key: K,
        lane: Address<Text>,
        lifecycle: LC,
    ) -> Self {
        let dl_lifecycle = JoinValueDownlink::new(projection, key.clone(), lane.clone(), lifecycle);
        let inner = OpenValueDownlinkAction::new(lane, dl_lifecycle, Default::default());
        AddDownlinkAction {
            projection,
            key: Some(key),
            inner: Some(inner),
        }
    }
}

impl<Context, K, V, LC> HandlerAction<Context> for AddDownlinkAction<Context, K, V, LC>
where
    Context: 'static,
    K: Clone + Eq + Hash + Send + 'static,
    V: Form + Send + Sync + 'static,
    V::Rec: Send,
    LC: JoinValueLaneLifecycle<K, V, Context> + Send + 'static,
{
    type Completion = ();

    fn step(
        &mut self,
        action_context: ActionContext<Context>,
        meta: AgentMetadata,
        context: &Context,
    ) -> StepResult<Self::Completion> {
        let AddDownlinkAction {
            projection,
            key,
            inner,
        } = self;
        if let Some(inner) = inner {
            if let Some(key) = key.take() {
                let lane = projection(context);
                let mut guard = lane.keys.borrow_mut();
                if guard.contains_key(&key) {
                    self.inner = None;
                    StepResult::done(())
                } else {
                    guard.insert(key, DownlinkStatus::Pending);
                    inner.step(action_context, meta, context).map(|_| ())
                }
            } else {
                inner.step(action_context, meta, context).map(|_| ())
            }
        } else {
            StepResult::after_done()
        }
    }
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
