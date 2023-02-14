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

use std::any::Any;
use std::hash::Hash;
use std::{cell::RefCell, collections::HashMap};

use bytes::BytesMut;
use swim_form::structural::write::StructuralWritable;
use swim_form::Form;
use swim_model::address::Address;
use swim_model::Text;
use uuid::Uuid;

use crate::agent_model::downlink::OpenValueDownlinkAction;
use crate::event_handler::{EventHandler, JoinValueInitializer, KeyDowncastError, Modification};
use crate::{
    agent_model::WriteResult,
    event_handler::{ActionContext, HandlerAction, StepResult},
    item::{AgentItem, MapItem},
    meta::AgentMetadata,
};

use self::default_lifecycle::DefaultJoinValueLifecycle;
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
struct AddDownlinkAction<Context, K, V, LC> {
    projection: fn(&Context) -> &JoinValueLane<K, V>,
    key: Option<K>,
    inner: Option<OpenValueDownlinkAction<V, JoinValueDownlink<K, V, LC, Context>>>,
}

impl<Context, K, V, LC> AddDownlinkAction<Context, K, V, LC>
where
    K: Clone,
{
    fn new(
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
    V: Form + Send + 'static,
    V::Rec: Send,
    LC: JoinValueLaneLifecycle<K, V, Context> + Send + 'static,
{
    type Completion = ();

    fn step(
        &mut self,
        action_context: &mut ActionContext<Context>,
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
                if let std::collections::hash_map::Entry::Vacant(e) = guard.entry(key) {
                    e.insert(DownlinkStatus::Pending);
                    inner.step(action_context, meta, context).map(|_| ())
                } else {
                    self.inner = None;
                    StepResult::done(())
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

/// An [`EventHandler`] that will get an entry from the map.
pub struct JoinValueLaneGet<C, K, V> {
    projection: for<'a> fn(&'a C) -> &'a JoinValueLane<K, V>,
    key: K,
    done: bool,
}

impl<C, K, V> JoinValueLaneGet<C, K, V> {
    pub fn new(projection: for<'a> fn(&'a C) -> &'a JoinValueLane<K, V>, key: K) -> Self {
        JoinValueLaneGet {
            projection,
            key,
            done: false,
        }
    }
}

impl<C, K, V> HandlerAction<C> for JoinValueLaneGet<C, K, V>
where
    K: Clone + Eq + Hash,
    V: Clone,
{
    type Completion = Option<V>;

    fn step(
        &mut self,
        _action_context: &mut ActionContext<C>,
        _meta: AgentMetadata,
        context: &C,
    ) -> StepResult<Self::Completion> {
        let JoinValueLaneGet {
            projection,
            key,
            done,
        } = self;
        if !*done {
            *done = true;
            let lane = projection(context);
            StepResult::done(lane.inner.get(key, |v| v.cloned()))
        } else {
            StepResult::after_done()
        }
    }
}

/// An [`EventHandler`] that will get an entry from the map.
pub struct JoinValueLaneGetMap<C, K, V> {
    projection: for<'a> fn(&'a C) -> &'a JoinValueLane<K, V>,
    done: bool,
}

impl<C, K, V> JoinValueLaneGetMap<C, K, V> {
    pub fn new(projection: for<'a> fn(&'a C) -> &'a JoinValueLane<K, V>) -> Self {
        JoinValueLaneGetMap {
            projection,
            done: false,
        }
    }
}

impl<C, K, V> HandlerAction<C> for JoinValueLaneGetMap<C, K, V>
where
    K: Clone + Eq + Hash,
    V: Clone,
{
    type Completion = HashMap<K, V>;

    fn step(
        &mut self,
        _action_context: &mut ActionContext<C>,
        _meta: AgentMetadata,
        context: &C,
    ) -> StepResult<Self::Completion> {
        let JoinValueLaneGetMap { projection, done } = self;
        if !*done {
            *done = true;
            let lane = projection(context);
            StepResult::done(lane.inner.get_map(Clone::clone))
        } else {
            StepResult::after_done()
        }
    }
}

/// An [`EventHandler`] that will request a sync from the lane.
pub struct JoinValueLaneSync<C, K, V> {
    projection: for<'a> fn(&'a C) -> &'a JoinValueLane<K, V>,
    id: Option<Uuid>,
}

impl<C, K, V> JoinValueLaneSync<C, K, V> {
    pub fn new(projection: for<'a> fn(&'a C) -> &'a JoinValueLane<K, V>, id: Uuid) -> Self {
        JoinValueLaneSync {
            projection,
            id: Some(id),
        }
    }
}

impl<C, K, V> HandlerAction<C> for JoinValueLaneSync<C, K, V>
where
    K: Clone + Eq + Hash,
{
    type Completion = ();

    fn step(
        &mut self,
        _action_context: &mut ActionContext<C>,
        _meta: AgentMetadata,
        context: &C,
    ) -> StepResult<Self::Completion> {
        let JoinValueLaneSync { projection, id } = self;
        if let Some(id) = id.take() {
            let lane = &projection(context).inner;
            lane.sync(id);
            StepResult::Complete {
                modified_item: Some(Modification::no_trigger(lane.id())),
                result: (),
            }
        } else {
            StepResult::after_done()
        }
    }
}

enum OpenDownlinkState<C, K, V> {
    Init {
        projection: fn(&C) -> &JoinValueLane<K, V>,
        key: K,
        address: Address<Text>,
    },
    Running {
        handler: Box<dyn EventHandler<C> + Send + 'static>,
    },
    Done,
}

impl<C, K, V> Default for OpenDownlinkState<C, K, V> {
    fn default() -> Self {
        OpenDownlinkState::Done
    }
}

pub struct LifecycleInitializer<Context, K, V, F> {
    projection: fn(&Context) -> &JoinValueLane<K, V>,
    lifecycle_factory: F,
}

impl<Context, K, V, F, LC> JoinValueInitializer<Context> for LifecycleInitializer<Context, K, V, F>
where
    Context: 'static,
    K: Any + Clone + Eq + Hash + Send + 'static,
    V: Form + Send + Sync + 'static,
    V::Rec: Send,
    F: Fn() -> LC + Send,
    LC: JoinValueLaneLifecycle<K, V, Context> + Send + 'static,
{
    fn try_create_action(
        &self,
        key: Box<dyn Any + Send>,
        address: Address<Text>,
    ) -> Result<Box<dyn EventHandler<Context> + Send + 'static>, KeyDowncastError> {
        let LifecycleInitializer {
            projection,
            lifecycle_factory,
        } = self;

        match key.downcast::<K>() {
            Ok(key) => {
                let lifecycle = lifecycle_factory();
                let action = AddDownlinkAction::new(*projection, *key, address, lifecycle);
                Ok(Box::new(action))
            }
            Err(bad_key) => Err(KeyDowncastError {
                key: bad_key,
                expected: std::any::TypeId::of::<K>(),
            }),
        }
    }
}

pub struct JoinValueAddDownlink<C, K, V> {
    state: OpenDownlinkState<C, K, V>,
}

impl<C, K, V> HandlerAction<C> for JoinValueAddDownlink<C, K, V>
where
    C: 'static,
    K: Any + Clone + Eq + Hash + Send + 'static,
    V: Form + Send + 'static,
    V::Rec: Send,
{
    type Completion = ();

    fn step(
        &mut self,
        action_context: &mut ActionContext<C>,
        meta: AgentMetadata,
        context: &C,
    ) -> StepResult<Self::Completion> {
        let JoinValueAddDownlink { state } = self;
        loop {
            match std::mem::take(state) {
                OpenDownlinkState::Init {
                    projection,
                    key,
                    address,
                } => {
                    let lane_id = projection(context).id();
                    let handler = if let Some(init) = action_context.join_value_initializer(lane_id)
                    {
                        match init.try_create_action(Box::new(key), address) {
                            Ok(boxed) => boxed,
                            Err(err) => break StepResult::Fail(err.into()),
                        }
                    } else {
                        let action = AddDownlinkAction::new(
                            projection,
                            key,
                            address,
                            DefaultJoinValueLifecycle,
                        );
                        let boxed: Box<dyn EventHandler<C> + Send + 'static> = Box::new(action);
                        boxed
                    };
                    *state = OpenDownlinkState::Running { handler };
                }
                OpenDownlinkState::Running { mut handler } => {
                    let result = handler.step(action_context, meta, context);
                    if result.is_cont() {
                        *state = OpenDownlinkState::Running { handler };
                    }
                    break result;
                }
                OpenDownlinkState::Done => break StepResult::after_done(),
            }
        }
    }
}

impl<C, K, V> JoinValueAddDownlink<C, K, V> {
    pub(crate) fn new(
        projection: fn(&C) -> &JoinValueLane<K, V>,
        key: K,
        address: Address<Text>,
    ) -> Self {
        JoinValueAddDownlink {
            state: OpenDownlinkState::Init {
                projection,
                key,
                address,
            },
        }
    }
}
