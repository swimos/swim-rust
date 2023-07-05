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

use std::{
    cell::RefCell,
    collections::{HashSet, VecDeque},
    hash::Hash,
};

use bytes::BytesMut;
use swim_api::protocol::{
    agent::{LaneResponse, MapLaneResponseEncoder},
    map::MapOperation,
};
use swim_form::structural::write::StructuralWritable;
use tokio_util::codec::Encoder;
use uuid::Uuid;

use crate::{
    agent_model::WriteResult,
    event_handler::{ActionContext, HandlerAction, Modification, StepResult},
    item::AgentItem,
    meta::AgentMetadata,
};

use self::lifecycle::{DemandMapLaneLifecycle, DemandMapLaneLifecycleShared};

use super::{
    queues::{Action, ToWrite, WriteQueues},
    LaneItem,
};

pub mod lifecycle;
#[cfg(test)]
mod tests;

#[derive(Debug)]
struct DemandMapLaneInner<K, V> {
    queues: WriteQueues<K>,
    pending: Option<Pending<K, V>>,
    sync_requests: HashSet<Uuid>,
}

#[derive(Debug)]
enum Pending<K, V> {
    Event(K, Option<V>),
    SyncEvent(Uuid, K, Option<V>),
    Synced(Uuid),
}

impl<K, V> Default for DemandMapLaneInner<K, V> {
    fn default() -> Self {
        Self {
            queues: Default::default(),
            pending: Default::default(),
            sync_requests: Default::default(),
        }
    }
}

impl<K, V> DemandMapLaneInner<K, V>
where
    K: Eq + Clone + Hash,
{
    pub fn pop(&mut self) -> Option<LaneResponse<MapOperation<K, V>>> {
        let DemandMapLaneInner { pending, .. } = self;
        loop {
            match pending.take() {
                Some(Pending::Event(key, Some(value))) => {
                    break Some(LaneResponse::StandardEvent(MapOperation::Update {
                        key,
                        value,
                    }));
                }
                Some(Pending::Event(key, _)) => {
                    break Some(LaneResponse::StandardEvent(MapOperation::Remove { key }));
                }
                Some(Pending::SyncEvent(id, key, Some(value))) => {
                    break Some(LaneResponse::SyncEvent(
                        id,
                        MapOperation::Update { key, value },
                    ));
                }
                Some(Pending::Synced(id)) => {
                    break Some(LaneResponse::Synced(id));
                }
                None => break None,
                _ => {}
            }
        }
    }

    pub fn is_empty(&self) -> bool {
        self.queues.is_empty() && self.sync_requests.is_empty()
    }
}

#[derive(Debug)]
pub struct DemandMapLane<K, V> {
    id: u64,
    inner: RefCell<DemandMapLaneInner<K, V>>,
}

impl<K, V> DemandMapLane<K, V> {
    pub fn new(lane_id: u64) -> Self {
        DemandMapLane {
            id: lane_id,
            inner: Default::default(),
        }
    }

    pub(crate) fn sync(&self, sync_id: Uuid) {
        let mut guard = self.inner.borrow_mut();
        let DemandMapLaneInner { sync_requests, .. } = &mut *guard;
        sync_requests.insert(sync_id);
    }
}

impl<K, V> AgentItem for DemandMapLane<K, V> {
    fn id(&self) -> u64 {
        self.id
    }
}

const INFALLIBLE_SER: &str = "Serializing lane responses to recon should be infallible.";

impl<K, V> LaneItem for DemandMapLane<K, V>
where
    K: StructuralWritable + Eq + Clone + Hash,
    V: StructuralWritable,
{
    fn write_to_buffer(&self, buffer: &mut BytesMut) -> WriteResult {
        let DemandMapLane { inner, .. } = self;
        let mut guard = inner.borrow_mut();

        let mut encoder = MapLaneResponseEncoder::default();
        if let Some(message) = guard.pop() {
            encoder.encode(message, buffer).expect(INFALLIBLE_SER);
            if guard.is_empty() {
                WriteResult::Done
            } else {
                WriteResult::RequiresEvent
            }
        } else {
            WriteResult::NoData
        }
    }
}

pub struct DemandMapLaneCueKey<Context, K, V> {
    projection: fn(&Context) -> &DemandMapLane<K, V>,
    key: Option<K>,
}

impl<Context, K, V> DemandMapLaneCueKey<Context, K, V> {
    pub fn new(projection: fn(&Context) -> &DemandMapLane<K, V>, key: K) -> Self {
        DemandMapLaneCueKey {
            projection,
            key: Some(key),
        }
    }
}

impl<Context, K, V> HandlerAction<Context> for DemandMapLaneCueKey<Context, K, V>
where
    K: Eq + Clone + Hash,
{
    type Completion = ();

    fn step(
        &mut self,
        _action_context: &mut ActionContext<Context>,
        _meta: AgentMetadata,
        context: &Context,
    ) -> StepResult<Self::Completion> {
        let DemandMapLaneCueKey { projection, key } = self;
        if let Some(key) = key.take() {
            let lane = projection(context);
            let mut guard = lane.inner.borrow_mut();
            let DemandMapLaneInner { queues, .. } = &mut *guard;
            queues.push_operation(MapOperation::Update { key, value: () });
            StepResult::Complete {
                modified_item: Some(Modification::of(lane.id)),
                result: (),
            }
        } else {
            StepResult::after_done()
        }
    }
}

pub struct DemandMapLaneSync<Context, K, V> {
    projection: fn(&Context) -> &DemandMapLane<K, V>,
    sync_id: Option<Uuid>,
}

impl<Context, K, V> DemandMapLaneSync<Context, K, V> {
    pub fn new(projection: fn(&Context) -> &DemandMapLane<K, V>, sync_id: Uuid) -> Self {
        DemandMapLaneSync {
            projection,
            sync_id: Some(sync_id),
        }
    }
}

impl<Context, K, V> HandlerAction<Context> for DemandMapLaneSync<Context, K, V> {
    type Completion = ();

    fn step(
        &mut self,
        _action_context: &mut ActionContext<Context>,
        _meta: AgentMetadata,
        context: &Context,
    ) -> StepResult<Self::Completion> {
        let DemandMapLaneSync {
            projection,
            sync_id,
        } = self;
        if let Some(sync_id) = sync_id.take() {
            let lane = projection(context);
            lane.sync(sync_id);
            StepResult::Complete {
                modified_item: Some(Modification::of(lane.id)),
                result: (),
            }
        } else {
            StepResult::after_done()
        }
    }
}

fn demand_map_handler_inner<Context, K, V, F, Keys, G, OnCueKey>(
    context: &Context,
    projection: fn(&Context) -> &DemandMapLane<K, V>,
    keys: F,
    on_cue_key: G,
) -> DemandMap<Context, K, V, Keys, OnCueKey>
where
    K: Clone + Eq + Hash,
    F: FnOnce() -> Keys,
    G: FnOnce(K) -> OnCueKey,
{
    let lane = projection(context);
    let mut guard = lane.inner.borrow_mut();
    let DemandMapLaneInner {
        queues,
        sync_requests,
        pending,
    } = &mut *guard;
    if pending.is_some() {
        DemandMap::no_handler(projection)
    } else if !sync_requests.is_empty() {
        DemandMap::keys(projection, keys())
    } else {
        loop {
            match queues.pop() {
                Some(ToWrite::Event(Action::Update { key, .. })) => {
                    break DemandMap::cue_key(projection, key.clone(), on_cue_key(key), None);
                }
                Some(ToWrite::Event(Action::Remove { key })) => {
                    *pending = Some(Pending::Event(key, None));
                    break DemandMap::complete(projection);
                }
                Some(ToWrite::SyncEvent(id, key)) => {
                    break DemandMap::cue_key(projection, key.clone(), on_cue_key(key), Some(id));
                }
                Some(ToWrite::Synced(id)) => {
                    *pending = Some(Pending::Synced(id));
                    break DemandMap::complete(projection);
                }
                None => break DemandMap::no_handler(projection),
                _ => {}
            }
        }
    }
}

pub fn demand_map_handler<'a, Context, K, V, LC>(
    context: &Context,
    projection: fn(&Context) -> &DemandMapLane<K, V>,
    lifecycle: &'a LC,
) -> DemandMap<Context, K, V, LC::KeysHandler<'a>, LC::OnCueKeyHandler<'a>>
where
    K: Clone + Eq + Hash,
    LC: DemandMapLaneLifecycle<K, V, Context>,
{
    demand_map_handler_inner(
        context,
        projection,
        || lifecycle.keys(),
        |key| lifecycle.on_cue_key(key),
    )
}

pub fn demand_map_handler_shared<'a, Context, Shared, K, V, LC>(
    context: &Context,
    shared: &'a Shared,
    projection: fn(&Context) -> &DemandMapLane<K, V>,
    lifecycle: &'a LC,
) -> DemandMap<Context, K, V, LC::KeysHandler<'a>, LC::OnCueKeyHandler<'a>>
where
    K: Clone + Eq + Hash,
    LC: DemandMapLaneLifecycleShared<K, V, Context, Shared>,
{
    demand_map_handler_inner(
        context,
        projection,
        || lifecycle.keys(shared, Default::default()),
        |key| lifecycle.on_cue_key(shared, Default::default(), key),
    )
}

enum DemandMapInner<K, Keys, OnCueK> {
    GettingKeys(Keys),
    CueingKey(K, OnCueK, Option<Uuid>),
    NoHandler,
    Complete(bool),
    Done,
}

pub struct DemandMap<Context, K, V, Keys, OnCueK> {
    projection: fn(&Context) -> &DemandMapLane<K, V>,
    inner: DemandMapInner<K, Keys, OnCueK>,
}

impl<Context, K, V, Keys, OnCueK> DemandMap<Context, K, V, Keys, OnCueK> {
    fn keys(projection: fn(&Context) -> &DemandMapLane<K, V>, keys: Keys) -> Self {
        DemandMap {
            projection,
            inner: DemandMapInner::GettingKeys(keys),
        }
    }

    fn cue_key(
        projection: fn(&Context) -> &DemandMapLane<K, V>,
        key: K,
        cue_key: OnCueK,
        sync: Option<Uuid>,
    ) -> Self {
        DemandMap {
            projection,
            inner: DemandMapInner::CueingKey(key, cue_key, sync),
        }
    }

    fn complete(projection: fn(&Context) -> &DemandMapLane<K, V>) -> Self {
        DemandMap {
            projection,
            inner: DemandMapInner::Complete(false),
        }
    }

    fn no_handler(projection: fn(&Context) -> &DemandMapLane<K, V>) -> Self {
        DemandMap {
            projection,
            inner: DemandMapInner::NoHandler,
        }
    }
}

impl<Context, K, V, H1, H2> HandlerAction<Context> for DemandMap<Context, K, V, H1, H2>
where
    K: Clone + Eq + Hash,
    H1: HandlerAction<Context, Completion = HashSet<K>>,
    H2: HandlerAction<Context, Completion = Option<V>>,
{
    type Completion = ();

    fn step(
        &mut self,
        action_context: &mut ActionContext<Context>,
        meta: AgentMetadata,
        context: &Context,
    ) -> StepResult<Self::Completion> {
        let DemandMap { projection, inner } = self;
        match inner {
            DemandMapInner::GettingKeys(keys_handler) => {
                match keys_handler.step(action_context, meta, context) {
                    StepResult::Continue { modified_item } => {
                        StepResult::Continue { modified_item }
                    }
                    StepResult::Fail(err) => {
                        *inner = DemandMapInner::Done;
                        StepResult::Fail(err)
                    }
                    StepResult::Complete {
                        modified_item,
                        result,
                    } => {
                        let lane = projection(context);
                        let mut guard = lane.inner.borrow_mut();
                        let DemandMapLaneInner {
                            queues,
                            sync_requests,
                            ..
                        } = &mut *guard;

                        let key_queue: VecDeque<_> = result.iter().cloned().collect();
                        for sync_id in sync_requests.drain() {
                            queues.sync(sync_id, key_queue.clone());
                        }

                        if modified_item.is_none() {
                            *inner = DemandMapInner::Done;
                            StepResult::Complete {
                                modified_item: Some(Modification::of(lane.id)),
                                result: (),
                            }
                        } else {
                            *inner = DemandMapInner::Complete(true);
                            StepResult::Continue { modified_item }
                        }
                    }
                }
            }
            DemandMapInner::CueingKey(key, cue_key_handler, sync) => {
                match cue_key_handler.step(action_context, meta, context) {
                    StepResult::Continue { modified_item } => {
                        StepResult::Continue { modified_item }
                    }
                    StepResult::Fail(err) => {
                        *inner = DemandMapInner::Done;
                        StepResult::Fail(err)
                    }
                    StepResult::Complete {
                        modified_item,
                        result,
                    } => {
                        let lane = projection(context);
                        let mut guard = lane.inner.borrow_mut();
                        let DemandMapLaneInner { pending, .. } = &mut *guard;

                        *pending = Some(if let Some(id) = sync.take() {
                            Pending::SyncEvent(id, key.clone(), result)
                        } else {
                            Pending::Event(key.clone(), result)
                        });

                        if modified_item.is_none() {
                            *inner = DemandMapInner::Done;
                            StepResult::Complete {
                                modified_item: Some(Modification::no_trigger(lane.id)),
                                result: (),
                            }
                        } else {
                            *inner = DemandMapInner::Complete(false);
                            StepResult::Continue { modified_item }
                        }
                    }
                }
            }
            DemandMapInner::NoHandler => {
                *inner = DemandMapInner::Done;
                StepResult::done(())
            }
            DemandMapInner::Complete(trigger) => {
                let lane = projection(context);
                let modified_item = Some(if *trigger {
                    Modification::of(lane.id)
                } else {
                    Modification::no_trigger(lane.id)
                });
                *inner = DemandMapInner::Done;
                StepResult::Complete {
                    modified_item,
                    result: (),
                }
            }
            DemandMapInner::Done => StepResult::after_done(),
        }
    }
}
