// Copyright 2015-2024 Swim Inc.
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

use std::{
    any::{type_name, Any, TypeId},
    borrow::Borrow,
    cell::RefCell,
    collections::{hash_map::Entry, BTreeSet, HashMap, HashSet},
    hash::Hash,
    marker::PhantomData,
};

use bytes::BytesMut;
use uuid::Uuid;

pub use downlink::{AfterClosed, JoinMapLaneUpdate};
pub use init::LifecycleInitializer;
use swimos_agent_protocol::MapMessage;
use swimos_api::address::Address;
use swimos_form::{write::StructuralWritable, Form};
use swimos_model::Text;
use swimos_utilities::trigger;

use crate::{agent_model::AgentDescription, event_handler::Described, item::JoinLikeItem};
use crate::{
    agent_model::{downlink::OpenEventDownlinkAction, WriteResult},
    config::SimpleDownlinkConfig,
    event_handler::{
        ActionContext, EventHandler, EventHandlerError, HandlerAction, Modification, StepResult,
    },
    item::{AgentItem, InspectableMapLikeItem, MapItem, MapLikeItem},
    lanes::{
        join_map::default_lifecycle::DefaultJoinMapLifecycle, map::MapLaneEvent, LaneItem, MapLane,
    },
    meta::AgentMetadata,
};

use super::DownlinkStatus;

use self::{downlink::JoinMapDownlink, lifecycle::JoinMapLaneLifecycle};

mod default_lifecycle;
mod downlink;
mod init;
pub mod lifecycle;
#[cfg(test)]
mod tests;

/// Model of a join map lane. This is conceptually similar to a [map lane](`super::super::MapLane`) only, rather than
/// the state being modified directly, it is populated through a series of map downlinks Each map downlink is
/// identified by a link key of type `L`.
///
/// Each entry in the map is 'owned' by the link that most recently updated it. When a 'clear' message is
/// received on a link, all keys owned by that link will be removed from the map. In all other respects,
/// it behaves as a read only map lane, having the same set of event handlers.
///
/// Join lanes provide views of the state of other remote lanes and so do not persist their state and are
/// always considered to be transient.
#[derive(Debug)]
pub struct JoinMapLane<L, K, V> {
    inner: MapLane<K, V>,
    link_tracker: RefCell<Links<L, K>>,
}

#[derive(Debug)]
struct Link<K> {
    status: DownlinkStatus,
    keys: HashSet<K>,
    stop_tx: Option<trigger::Sender>,
}

impl<K> Link<K> {
    fn set_stop_tx(&mut self, stop_tx: Option<trigger::Sender>) {
        self.stop_tx = stop_tx;
    }
}

impl<K> Default for Link<K> {
    fn default() -> Self {
        Self {
            status: DownlinkStatus::Pending,
            keys: Default::default(),
            stop_tx: None,
        }
    }
}

#[derive(Debug)]
struct Links<L, K> {
    links: HashMap<L, Link<K>>,
    ownership: HashMap<K, L>,
}

impl<L, K> Links<L, K>
where
    L: Hash + Eq,
    K: Clone + Hash + Eq,
{
    fn insert(&mut self, link: L, key: K) -> bool {
        let Links { links, ownership } = self;
        if let Some(Link { status, keys, .. }) = links.get_mut(&link) {
            if *status == DownlinkStatus::Linked {
                keys.insert(key.clone());
                if let Some(old_link) = ownership.remove(&key) {
                    if let Some(l) = links.get_mut(&old_link) {
                        l.keys.remove(&key);
                    }
                }
                ownership.insert(key, link);
                true
            } else {
                false
            }
        } else {
            false
        }
    }

    fn remove(&mut self, link: &L, key: &K) -> bool {
        let Links { links, ownership } = self;
        if let Some(l) = links.get_mut(link) {
            if l.status == DownlinkStatus::Linked {
                l.keys.remove(key);
                ownership.remove(key);
                true
            } else {
                false
            }
        } else {
            false
        }
    }

    fn clear(&mut self, link: &L) -> Option<HashSet<K>> {
        let Links { links, ownership } = self;
        links.get_mut(link).and_then(|l| {
            if l.status == DownlinkStatus::Linked {
                let keys = std::mem::take(&mut l.keys);
                for k in &keys {
                    ownership.remove(k);
                }
                Some(keys)
            } else {
                None
            }
        })
    }

    fn remove_link(&mut self, link: &L) {
        let Links { links, ownership } = self;
        if let Some(Link { keys, .. }) = links.remove(link) {
            for k in keys {
                ownership.remove(&k);
            }
        }
    }
}

impl<L, K> Links<L, K>
where
    L: Clone + Hash + Eq,
    K: Clone + Hash + Eq + Ord,
{
    fn take(&mut self, link: &L, n: usize) -> Option<Vec<K>> {
        let Links { links, ownership } = self;
        links.get_mut(link).and_then(|l| {
            if l.status == DownlinkStatus::Linked {
                let sorted = l.keys.iter().collect::<BTreeSet<_>>();
                let to_remove = sorted
                    .iter()
                    .skip(n)
                    .map(|k| (*k).clone())
                    .collect::<Vec<_>>();
                for k in &to_remove {
                    l.keys.remove(k);
                    ownership.remove(k);
                }
                Some(to_remove)
            } else {
                None
            }
        })
    }

    fn drop(&mut self, link: &L, n: usize) -> Option<Vec<K>> {
        let Links { links, ownership } = self;
        links.get_mut(link).and_then(|l| {
            if l.status == DownlinkStatus::Linked {
                let sorted = l.keys.iter().collect::<BTreeSet<_>>();
                let to_remove = sorted
                    .iter()
                    .take(n)
                    .map(|k| (*k).clone())
                    .collect::<Vec<_>>();
                for k in &to_remove {
                    l.keys.remove(k);
                    ownership.remove(k);
                }
                Some(to_remove)
            } else {
                None
            }
        })
    }
}

impl<L, K> Default for Links<L, K> {
    fn default() -> Self {
        Self {
            links: Default::default(),
            ownership: Default::default(),
        }
    }
}

impl<L, K, V> JoinMapLane<L, K, V> {
    pub fn new(id: u64) -> Self {
        JoinMapLane {
            inner: MapLane::new(id, HashMap::new()),
            link_tracker: Default::default(),
        }
    }
}

impl<L, K, V> JoinMapLane<L, K, V>
where
    L: Clone + Hash + Eq,
    K: Clone + Hash + Eq + Ord,
{
    pub(crate) fn update(&self, link_key: L, message: MapMessage<K, V>, add_link: bool) -> bool {
        let JoinMapLane {
            inner,
            link_tracker,
        } = self;
        let mut guard = link_tracker.borrow_mut();
        if add_link {
            guard.links.entry(link_key.clone()).or_default().status = DownlinkStatus::Linked;
        }
        match message {
            MapMessage::Update { key, value } => {
                if guard.insert(link_key, key.clone()) {
                    inner.update(key, value);
                    true
                } else {
                    false
                }
            }
            MapMessage::Remove { key } => {
                if guard.remove(&link_key, &key) {
                    inner.remove(&key);
                    true
                } else {
                    false
                }
            }
            MapMessage::Clear => {
                if let Some(keys) = guard.clear(&link_key) {
                    for k in keys {
                        inner.remove(&k);
                    }
                    true
                } else {
                    false
                }
            }
            MapMessage::Take(n) => {
                if let Some(keys) = usize::try_from(n)
                    .ok()
                    .and_then(|n| guard.take(&link_key, n))
                {
                    for k in keys {
                        inner.remove(&k);
                    }
                    true
                } else {
                    false
                }
            }
            MapMessage::Drop(n) => {
                if let Ok(n) = usize::try_from(n) {
                    if let Some(keys) = guard.drop(&link_key, n) {
                        for k in keys {
                            inner.remove(&k);
                        }
                        true
                    } else {
                        false
                    }
                } else if let Some(keys) = guard.clear(&link_key) {
                    for k in keys {
                        inner.remove(&k);
                    }
                    true
                } else {
                    false
                }
            }
        }
    }
}

impl<L, K, V> JoinMapLane<L, K, V>
where
    K: Clone + Eq + Hash,
{
    /// Read a value from the map, if it exists.
    pub fn get<Q, F, R>(&self, key: &Q, f: F) -> R
    where
        K: Borrow<Q>,
        Q: Hash + Eq,
        F: FnOnce(Option<&V>) -> R,
    {
        self.inner.get(key, f)
    }

    /// Read the complete state of the map.
    pub fn get_map<F, R>(&self, f: F) -> R
    where
        F: FnOnce(&HashMap<K, V>) -> R,
    {
        self.inner.get_map(f)
    }
}

impl<L, K, V> AgentItem for JoinMapLane<L, K, V> {
    fn id(&self) -> u64 {
        self.inner.id()
    }
}

impl<L, K, V> LaneItem for JoinMapLane<L, K, V>
where
    K: Clone + Eq + Hash + StructuralWritable,
    V: StructuralWritable,
{
    fn write_to_buffer(&self, buffer: &mut BytesMut) -> WriteResult {
        self.inner.write_to_buffer(buffer)
    }
}

type JoinMapStartAction<Context, L, K, V, LC> =
    OpenEventDownlinkAction<MapMessage<K, V>, JoinMapDownlink<L, K, V, LC, Context>>;

/// [`HandlerAction`] that attempts to add a new downlink to a [`JoinMapLane`].
struct AddDownlinkAction<Context, L, K, V, LC> {
    projection: fn(&Context) -> &JoinMapLane<L, K, V>,
    link_key: L,
    started: bool,
    inner: Option<JoinMapStartAction<Context, L, K, V, LC>>,
}

impl<Context, L, K, V, LC> AddDownlinkAction<Context, L, K, V, LC>
where
    L: Clone,
{
    fn new(
        projection: fn(&Context) -> &JoinMapLane<L, K, V>,
        link_key: L,
        lane: Address<Text>,
        lifecycle: LC,
    ) -> Self {
        let dl_lifecycle =
            JoinMapDownlink::new(projection, link_key.clone(), lane.clone(), lifecycle);
        let inner = OpenEventDownlinkAction::new(
            lane,
            dl_lifecycle,
            SimpleDownlinkConfig {
                events_when_not_synced: true,
                terminate_on_unlinked: true,
            },
            true,
        );
        AddDownlinkAction {
            projection,
            link_key,
            started: false,
            inner: Some(inner),
        }
    }
}

impl<Context, L, K, V, LC> HandlerAction<Context> for AddDownlinkAction<Context, L, K, V, LC>
where
    Context: AgentDescription + 'static,
    L: Clone + Eq + Hash + Send + 'static,
    K: Form + Clone + Eq + Hash + Ord + Send + 'static,
    V: Form + Send + 'static,
    K::Rec: Send,
    V::BodyRec: Send,
    LC: JoinMapLaneLifecycle<L, K, Context> + Send + 'static,
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
            link_key,
            started,
            inner,
        } = self;

        if let Some(inner) = inner {
            let lane = projection(context);
            let mut guard = lane.link_tracker.borrow_mut();

            if !*started {
                match guard.links.entry(link_key.clone()) {
                    Entry::Vacant(entry) => {
                        *started = true;

                        let link = entry.insert(Link::default());
                        inner.step(action_context, meta, context).map(|handle| {
                            link.set_stop_tx(handle.into_stop_rx());
                        })
                    }
                    Entry::Occupied(_) => {
                        self.inner = None;
                        StepResult::done(())
                    }
                }
            } else {
                inner.step(action_context, meta, context).map(|handle| {
                    if let Some(link) = guard.links.get_mut(link_key) {
                        link.set_stop_tx(handle.into_stop_rx())
                    }
                })
            }
        } else {
            StepResult::after_done()
        }
    }

    fn describe(
        &self,
        context: &Context,
        f: &mut std::fmt::Formatter<'_>,
    ) -> Result<(), std::fmt::Error> {
        let AddDownlinkAction {
            projection,
            started,
            inner,
            ..
        } = self;
        let lane = (projection)(context);
        let name = context.item_name(lane.id());
        match inner {
            Some(open_downlink) => f
                .debug_struct("AddDownlinkAction")
                .field("id", &lane.id())
                .field("lane_name", &name.as_ref().map(|s| s.as_ref()))
                .field("started", started)
                .field("consumed", &false)
                .field("open_downlink", &Described::new(context, open_downlink))
                .finish(),
            None => f
                .debug_struct("AddDownlinkAction")
                .field("id", &lane.id())
                .field("lane_name", &name.as_ref().map(|s| s.as_ref()))
                .field("started", started)
                .field("consumed", &true)
                .finish(),
        }
    }
}

impl<L, K, V> MapItem<K, V> for JoinMapLane<L, K, V>
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

#[derive(Default)]
enum OpenDownlinkState<C, L> {
    Init {
        link_key: L,
        address: Address<Text>,
    },
    Running {
        handler: Box<dyn EventHandler<C> + Send + 'static>,
    },
    #[default]
    Done,
}

pub struct JoinMapAddDownlink<C, L, K, V> {
    projection: fn(&C) -> &JoinMapLane<L, K, V>,
    state: OpenDownlinkState<C, L>,
}

impl<C, L, K, V> HandlerAction<C> for JoinMapAddDownlink<C, L, K, V>
where
    C: AgentDescription + 'static,
    L: Any + Clone + Eq + Hash + Send + 'static,
    K: Any + Form + Clone + Eq + Hash + Ord + Send + 'static,
    V: Any + Form + Send + 'static,
    K::Rec: Send,
    V::BodyRec: Send,
{
    type Completion = ();

    fn step(
        &mut self,
        action_context: &mut ActionContext<C>,
        meta: AgentMetadata,
        context: &C,
    ) -> StepResult<Self::Completion> {
        let JoinMapAddDownlink { projection, state } = self;
        loop {
            match std::mem::take(state) {
                OpenDownlinkState::Init { link_key, address } => {
                    let lane_id = projection(context).id();
                    let handler = if let Some(init) = action_context.join_lane_initializer(lane_id)
                    {
                        match init.try_create_action(
                            Box::new(link_key),
                            TypeId::of::<K>(),
                            TypeId::of::<V>(),
                            address,
                        ) {
                            Ok(boxed) => boxed,
                            Err(err) => {
                                break StepResult::Fail(EventHandlerError::BadJoinLifecycle(err))
                            }
                        }
                    } else {
                        let action = AddDownlinkAction::new(
                            *projection,
                            link_key,
                            address,
                            DefaultJoinMapLifecycle,
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

    fn describe(
        &self,
        context: &C,
        f: &mut std::fmt::Formatter<'_>,
    ) -> Result<(), std::fmt::Error> {
        let JoinMapAddDownlink { projection, state } = self;
        let lane = (projection)(context);
        let name = context.item_name(lane.id());
        match state {
            OpenDownlinkState::Init { address, .. } => f
                .debug_struct("JoinMapAddDownlink")
                .field("id", &lane.id())
                .field("lane_name", &name.as_ref().map(|s| s.as_ref()))
                .field("address", address)
                .field("state", &"Init")
                .finish(),
            OpenDownlinkState::Running { handler } => f
                .debug_struct("JoinMapAddDownlink")
                .field("id", &lane.id())
                .field("lane_name", &name.as_ref().map(|s| s.as_ref()))
                .field("handler", &Described::new(context, handler))
                .field("state", &"Running")
                .finish(),
            OpenDownlinkState::Done => f
                .debug_struct("JoinMapAddDownlink")
                .field("id", &lane.id())
                .field("lane_name", &name.as_ref().map(|s| s.as_ref()))
                .field("state", &"Done")
                .finish(),
        }
    }
}

impl<C, L, K, V> JoinMapAddDownlink<C, L, K, V> {
    pub(crate) fn new(
        projection: fn(&C) -> &JoinMapLane<L, K, V>,
        link_key: L,
        address: Address<Text>,
    ) -> Self {
        JoinMapAddDownlink {
            projection,
            state: OpenDownlinkState::Init { link_key, address },
        }
    }
}

///  An [event handler](crate::event_handler::EventHandler) that will get an entry from the map.
pub struct JoinMapLaneGet<C, L, K, V> {
    projection: fn(&C) -> &JoinMapLane<L, K, V>,
    key: K,
    done: bool,
}

impl<C, L, K, V> JoinMapLaneGet<C, L, K, V> {
    pub fn new(projection: fn(&C) -> &JoinMapLane<L, K, V>, key: K) -> Self {
        JoinMapLaneGet {
            projection,
            key,
            done: false,
        }
    }
}

impl<C, L, K, V> HandlerAction<C> for JoinMapLaneGet<C, L, K, V>
where
    C: AgentDescription,
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
        let JoinMapLaneGet {
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

    fn describe(
        &self,
        context: &C,
        f: &mut std::fmt::Formatter<'_>,
    ) -> Result<(), std::fmt::Error> {
        let lane = (self.projection)(context);
        let name = context.item_name(lane.id());
        f.debug_struct("JoinMapLaneGet")
            .field("id", &lane.id())
            .field("lane_name", &name.as_ref().map(|s| s.as_ref()))
            .field("consumed", &self.done)
            .finish()
    }
}

/// An [event handler](crate::event_handler::EventHandler)`] that will get an entry from the map.
pub struct JoinMapLaneGetMap<C, L, K, V> {
    projection: fn(&C) -> &JoinMapLane<L, K, V>,
    done: bool,
}

impl<C, L, K, V> JoinMapLaneGetMap<C, L, K, V> {
    pub fn new(projection: fn(&C) -> &JoinMapLane<L, K, V>) -> Self {
        JoinMapLaneGetMap {
            projection,
            done: false,
        }
    }
}

impl<C, L, K, V> HandlerAction<C> for JoinMapLaneGetMap<C, L, K, V>
where
    C: AgentDescription,
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
        let JoinMapLaneGetMap { projection, done } = self;
        if !*done {
            *done = true;
            let lane = projection(context);
            StepResult::done(lane.inner.get_map(Clone::clone))
        } else {
            StepResult::after_done()
        }
    }

    fn describe(
        &self,
        context: &C,
        f: &mut std::fmt::Formatter<'_>,
    ) -> Result<(), std::fmt::Error> {
        let lane = (self.projection)(context);
        let name = context.item_name(lane.id());
        f.debug_struct("JoinMapLaneGetMap")
            .field("id", &lane.id())
            .field("lane_name", &name.as_ref().map(|s| s.as_ref()))
            .field("consumed", &self.done)
            .finish()
    }
}

///  An [event handler](crate::event_handler::EventHandler)`] that will request a sync from the lane.
pub struct JoinMapLaneSync<C, L, K, V> {
    projection: fn(&C) -> &JoinMapLane<L, K, V>,
    id: Option<Uuid>,
}

impl<C, L, K, V> JoinMapLaneSync<C, L, K, V> {
    pub fn new(projection: fn(&C) -> &JoinMapLane<L, K, V>, id: Uuid) -> Self {
        JoinMapLaneSync {
            projection,
            id: Some(id),
        }
    }
}

impl<C, L, K, V> HandlerAction<C> for JoinMapLaneSync<C, L, K, V>
where
    C: AgentDescription,
    K: Clone + Eq + Hash,
{
    type Completion = ();

    fn step(
        &mut self,
        _action_context: &mut ActionContext<C>,
        _meta: AgentMetadata,
        context: &C,
    ) -> StepResult<Self::Completion> {
        let JoinMapLaneSync { projection, id } = self;
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

    fn describe(
        &self,
        context: &C,
        f: &mut std::fmt::Formatter<'_>,
    ) -> Result<(), std::fmt::Error> {
        let JoinMapLaneSync { projection, id } = self;
        let lane = (projection)(context);
        let name = context.item_name(lane.id());
        f.debug_struct("JoinMapLaneSync")
            .field("id", &lane.id())
            .field("lane_name", &name.as_ref().map(|s| s.as_ref()))
            .field("sync_id", &id)
            .finish()
    }
}

/// A [`HandlerAction`] that will produce a value by applying a closure to a reference to
/// and entry in the lane.
pub struct JoinMapLaneWithEntry<C, L, K, V, F, B: ?Sized> {
    projection: for<'a> fn(&'a C) -> &'a JoinMapLane<L, K, V>,
    key: K,
    f: Option<F>,
    _type: PhantomData<fn(&B)>,
}

impl<C, L, K, V, F, B: ?Sized> JoinMapLaneWithEntry<C, L, K, V, F, B> {
    /// #Arguments
    /// * `projection` - Projection from the agent context to the lane.
    /// * `key` - Key of the entry.
    /// * `f` - The closure to apply to the entry.
    pub fn new(projection: for<'a> fn(&'a C) -> &'a JoinMapLane<L, K, V>, key: K, f: F) -> Self {
        JoinMapLaneWithEntry {
            projection,
            key,
            f: Some(f),
            _type: PhantomData,
        }
    }
}

impl<'a, C, L, K, V, F, B, U> HandlerAction<C> for JoinMapLaneWithEntry<C, L, K, V, F, B>
where
    C: AgentDescription,
    K: Eq + Hash + 'static,
    C: 'a,
    B: ?Sized + 'static,
    V: Borrow<B>,
    F: FnOnce(Option<&B>) -> U + Send + 'a,
{
    type Completion = U;

    fn step(
        &mut self,
        _action_context: &mut ActionContext<C>,
        _meta: AgentMetadata,
        context: &C,
    ) -> StepResult<Self::Completion> {
        if let Some(f) = self.f.take() {
            let item = (self.projection)(context);
            StepResult::done(item.inner.with_entry(&self.key, f))
        } else {
            StepResult::after_done()
        }
    }

    fn describe(
        &self,
        context: &C,
        f: &mut std::fmt::Formatter<'_>,
    ) -> Result<(), std::fmt::Error> {
        let lane = (self.projection)(context);
        let name = context.item_name(lane.id());
        f.debug_struct("JoinMapLaneWithEntry")
            .field("id", &lane.id())
            .field("lane_name", &name.as_ref().map(|s| s.as_ref()))
            .field("result_type", &type_name::<U>())
            .field("consumed", &self.f.is_none())
            .finish()
    }
}

impl<L, K, V> MapLikeItem<K, V> for JoinMapLane<L, K, V>
where
    L: Send + 'static,
    K: Clone + Eq + Hash + Send + 'static,
    V: Clone + Send + 'static,
{
    type GetHandler<C> = JoinMapLaneGet<C, L, K, V>
    where
        C: AgentDescription + 'static;

    type GetMapHandler<C> = JoinMapLaneGetMap<C, L, K, V>
    where
        C: AgentDescription + 'static;

    fn get_handler<C: AgentDescription + 'static>(
        projection: fn(&C) -> &Self,
        key: K,
    ) -> Self::GetHandler<C> {
        JoinMapLaneGet::new(projection, key)
    }

    fn get_map_handler<C: AgentDescription + 'static>(
        projection: fn(&C) -> &Self,
    ) -> Self::GetMapHandler<C> {
        JoinMapLaneGetMap::new(projection)
    }
}

impl<L, K, V, B> InspectableMapLikeItem<K, V, B> for JoinMapLane<L, K, V>
where
    L: Send + 'static,
    K: Clone + Eq + Hash + Send + 'static,
    V: Borrow<B> + Send + 'static,
    B: ?Sized + 'static,
{
    type WithEntryHandler<'a, C, F, U> = JoinMapLaneWithEntry<C, L, K, V, F, B>
    where
        Self: 'static,
        C: AgentDescription + 'a,
        F: FnOnce(Option<&B>) -> U + Send + 'a;

    fn with_entry_handler<'a, C, F, U>(
        projection: fn(&C) -> &Self,
        key: K,
        f: F,
    ) -> Self::WithEntryHandler<'a, C, F, U>
    where
        Self: 'static,
        C: AgentDescription + 'a,
        F: FnOnce(Option<&B>) -> U + Send + 'a,
    {
        JoinMapLaneWithEntry::new(projection, key, f)
    }
}

/// An [`EventHandler`] that will remove a downlink from the lane.
pub struct JoinMapRemoveDownlink<L, C, K, V> {
    projection: fn(&C) -> &JoinMapLane<L, K, V>,
    key: Option<L>,
}

impl<L, C, K, V> JoinMapRemoveDownlink<L, C, K, V> {
    pub fn new(
        projection: fn(&C) -> &JoinMapLane<L, K, V>,
        key: L,
    ) -> JoinMapRemoveDownlink<L, C, K, V> {
        JoinMapRemoveDownlink {
            projection,
            key: Some(key),
        }
    }
}

impl<L, C, K, V> HandlerAction<C> for JoinMapRemoveDownlink<L, C, K, V>
where
    C: AgentDescription + 'static,
    L: Send + Eq + PartialEq + Hash + 'static,
    K: Eq + Clone + Hash + 'static,
    V: 'static,
{
    type Completion = ();

    fn step(
        &mut self,
        _action_context: &mut ActionContext<C>,
        _meta: AgentMetadata,
        context: &C,
    ) -> StepResult<Self::Completion> {
        let JoinMapRemoveDownlink { projection, key } = self;

        match key.take() {
            Some(key) => {
                let lane = projection(context);
                let mut key_guard = lane.link_tracker.borrow_mut();
                let trigger = key_guard.links.remove(&key).and_then(|link| {
                    for k in link.keys {
                        lane.inner.remove(&k)
                    }
                    link.stop_tx
                });

                if let Some(trigger) = trigger {
                    trigger.trigger();
                }
                StepResult::done(())
            }
            None => StepResult::after_done(),
        }
    }

    fn describe(
        &self,
        context: &C,
        f: &mut std::fmt::Formatter<'_>,
    ) -> Result<(), std::fmt::Error> {
        let JoinMapRemoveDownlink { projection, key } = self;
        let lane = (projection)(context);
        let name = context.item_name(lane.id());
        f.debug_struct("JoinMapRemoveDownlink")
            .field("id", &lane.id())
            .field("lane_name", &name.as_ref().map(|s| s.as_ref()))
            .field("consumed", &key.is_none())
            .finish()
    }
}

impl<L, K, V> JoinLikeItem<L> for JoinMapLane<L, K, V>
where
    L: Send + Eq + PartialEq + Hash + 'static,
    K: Eq + Clone + Hash + 'static,
    V: 'static,
{
    type RemoveDownlinkHandler<C> = JoinMapRemoveDownlink<L, C, K, V>
    where
        C: AgentDescription + 'static;

    fn remove_downlink_handler<C: AgentDescription + 'static>(
        projection: fn(&C) -> &Self,
        link_key: L,
    ) -> Self::RemoveDownlinkHandler<C> {
        JoinMapRemoveDownlink::new(projection, link_key)
    }
}
