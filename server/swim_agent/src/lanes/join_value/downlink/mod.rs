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

use crate::{
    agent_lifecycle::utility::HandlerContext,
    downlink_lifecycle::{
        on_failed::OnFailed, on_linked::OnLinked, on_synced::OnSynced, on_unlinked::OnUnlinked,
        value::on_event::OnDownlinkEvent,
    },
    event_handler::{
        ActionContext, AndThen, FollowedBy, HandlerAction, HandlerActionExt, HandlerTrans,
        Modification, StepResult,
    },
    item::AgentItem,
    meta::AgentMetadata,
};

use super::{
    lifecycle::{
        on_failed::OnJoinValueFailed, on_linked::OnJoinValueLinked, on_synced::OnJoinValueSynced,
        on_unlinked::OnJoinValueUnlinked,
    },
    DownlinkStatus, JoinValueLane, LinkClosedResponse, RemoteLane,
};

pub struct JoinValueDownlink<K, V, LC, Context> {
    projection: fn(&Context) -> &JoinValueLane<K, V>,
    key: K,
    host: Option<String>,
    node: String,
    lane: String,
    lifecycle: LC,
}

impl<K, V, LC, Context> OnLinked<Context> for JoinValueDownlink<K, V, LC, Context>
where
    K: Clone + Hash + Eq + Send,
    LC: OnJoinValueLinked<K, Context>,
{
    type OnLinkedHandler<'a> = FollowedBy<AlterKeyState<K, V, Context>, LC::OnJoinValueLinkedHandler<'a>>
    where
        Self: 'a;

    fn on_linked(&self) -> Self::OnLinkedHandler<'_> {
        let JoinValueDownlink {
            projection,
            key,
            host,
            node,
            lane,
            lifecycle,
        } = self;
        let remote = RemoteLane {
            host: host.as_ref().map(String::as_str),
            node: &node,
            lane: &lane,
        };
        let alter_state =
            AlterKeyState::new(*projection, key.clone(), Some(DownlinkStatus::Linked));
        alter_state.followed_by(lifecycle.on_linked(HandlerContext::default(), key.clone(), remote))
    }
}

impl<K, V, LC, Context> OnSynced<V, Context> for JoinValueDownlink<K, V, LC, Context>
where
    K: Clone + Hash + Eq + Send,
    LC: OnJoinValueSynced<K, V, Context>,
{
    type OnSyncedHandler<'a> = LC::OnJoinValueSyncedHandler<'a>
    where
        Self: 'a;

    fn on_synced<'a>(&'a self, value: &V) -> Self::OnSyncedHandler<'a> {
        let JoinValueDownlink {
            key,
            host,
            node,
            lane,
            lifecycle,
            ..
        } = self;
        let remote = RemoteLane {
            host: host.as_ref().map(String::as_str),
            node: &node,
            lane: &lane,
        };
        lifecycle.on_synced(HandlerContext::default(), key.clone(), remote, Some(value))
    }
}

impl<K, V, LC, Context> OnUnlinked<Context> for JoinValueDownlink<K, V, LC, Context>
where
    K: Clone + Hash + Eq + Send,
    LC: OnJoinValueUnlinked<K, Context>,
{
    type OnUnlinkedHandler<'a> = AndThen<LC::OnJoinValueUnlinkedHandler<'a>, AfterClosed<K, V, Context>, AfterClosedTrans<K, V, Context>>
    where
        Self: 'a;

    fn on_unlinked(&self) -> Self::OnUnlinkedHandler<'_> {
        let JoinValueDownlink {
            projection,
            key,
            host,
            node,
            lane,
            lifecycle,
        } = self;
        let remote = RemoteLane {
            host: host.as_ref().map(String::as_str),
            node: &node,
            lane: &lane,
        };
        lifecycle
            .on_unlinked(HandlerContext::default(), key.clone(), remote)
            .and_then(AfterClosedTrans::new(*projection, key.clone()))
    }
}

impl<K, V, LC, Context> OnFailed<Context> for JoinValueDownlink<K, V, LC, Context>
where
    K: Clone + Hash + Eq + Send,
    LC: OnJoinValueFailed<K, Context>,
{
    type OnFailedHandler<'a> = AndThen<LC::OnJoinValueFailedHandler<'a>, AfterClosed<K, V, Context>, AfterClosedTrans<K, V, Context>>
    where
        Self: 'a;

    fn on_failed(&self) -> Self::OnFailedHandler<'_> {
        let JoinValueDownlink {
            projection,
            key,
            host,
            node,
            lane,
            lifecycle,
        } = self;
        let remote = RemoteLane {
            host: host.as_ref().map(String::as_str),
            node: &node,
            lane: &lane,
        };
        lifecycle
            .on_failed(HandlerContext::default(), key.clone(), remote)
            .and_then(AfterClosedTrans::new(*projection, key.clone()))
    }
}

impl<K, V, LC, Context> OnDownlinkEvent<V, Context> for JoinValueDownlink<K, V, LC, Context>
where
    LC: Send,
    K: Clone + Hash + Eq + Send,
{
    type OnEventHandler<'a> = JoinValueLaneUpdate<Context, K, V>
    where
        Self: 'a;

    fn on_event<'a>(&'a self, value: V) -> Self::OnEventHandler<'a> {
        let JoinValueDownlink {
            projection, key, ..
        } = self;
        JoinValueLaneUpdate::new(*projection, key.clone(), value)
    }
}

pub struct AlterKeyState<K, V, Context> {
    projection: fn(&Context) -> &JoinValueLane<K, V>,
    key: Option<K>,
    state: Option<DownlinkStatus>,
}

impl<K, V, Context> AlterKeyState<K, V, Context> {
    fn new(
        projection: fn(&Context) -> &JoinValueLane<K, V>,
        key: K,
        state: Option<DownlinkStatus>,
    ) -> Self {
        AlterKeyState {
            projection,
            key: Some(key),
            state,
        }
    }
}

impl<K, V, Context> HandlerAction<Context> for AlterKeyState<K, V, Context>
where
    K: Hash + Eq,
{
    type Completion = ();

    fn step(
        &mut self,
        _action_context: ActionContext<Context>,
        _meta: AgentMetadata,
        context: &Context,
    ) -> StepResult<Self::Completion> {
        let AlterKeyState {
            projection,
            key,
            state,
        } = self;
        if let Some(key) = key.take() {
            let lane = projection(context);
            let mut guard = lane.keys.borrow_mut();
            if let Some(state) = state.take() {
                guard.insert(key, state);
            } else {
                guard.remove(&key);
            }
            StepResult::done(())
        } else {
            StepResult::after_done()
        }
    }
}

pub struct AfterClosed<K, V, Context> {
    projection: fn(&Context) -> &JoinValueLane<K, V>,
    key: K,
    response: Option<LinkClosedResponse>,
}

impl<K, V, Context> AfterClosed<K, V, Context> {
    fn new(
        projection: fn(&Context) -> &JoinValueLane<K, V>,
        key: K,
        response: LinkClosedResponse,
    ) -> Self {
        AfterClosed {
            projection,
            key,
            response: Some(response),
        }
    }
}

impl<K, V, Context> HandlerAction<Context> for AfterClosed<K, V, Context>
where
    K: Hash + Eq,
{
    type Completion = ();

    fn step(
        &mut self,
        _action_context: ActionContext<Context>,
        _meta: AgentMetadata,
        context: &Context,
    ) -> StepResult<Self::Completion> {
        let AfterClosed {
            projection,
            key,
            response,
        } = self;
        match response.take() {
            Some(LinkClosedResponse::Abandon) => StepResult::done(()),
            Some(LinkClosedResponse::Delete) => {
                let lane = projection(context);
                let mut guard = lane.keys.borrow_mut();
                guard.remove(&key);
                StepResult::done(())
            }
            Some(LinkClosedResponse::Retry) => todo!(),
            _ => StepResult::after_done(),
        }
    }
}

pub struct AfterClosedTrans<K, V, Context> {
    projection: fn(&Context) -> &JoinValueLane<K, V>,
    key: K,
}

impl<K, V, Context> AfterClosedTrans<K, V, Context> {
    pub fn new(projection: fn(&Context) -> &JoinValueLane<K, V>, key: K) -> Self {
        AfterClosedTrans { projection, key }
    }
}

impl<K, V, Context> HandlerTrans<LinkClosedResponse> for AfterClosedTrans<K, V, Context> {
    type Out = AfterClosed<K, V, Context>;

    fn transform(self, input: LinkClosedResponse) -> Self::Out {
        let AfterClosedTrans { projection, key } = self;
        AfterClosed::new(projection, key, input)
    }
}

pub struct JoinValueLaneUpdate<C, K, V> {
    projection: for<'a> fn(&'a C) -> &'a JoinValueLane<K, V>,
    key_value: Option<(K, V)>,
}

impl<C, K, V> JoinValueLaneUpdate<C, K, V> {
    pub fn new(projection: for<'a> fn(&'a C) -> &'a JoinValueLane<K, V>, key: K, value: V) -> Self {
        JoinValueLaneUpdate {
            projection,
            key_value: Some((key, value)),
        }
    }
}

impl<C, K, V> HandlerAction<C> for JoinValueLaneUpdate<C, K, V>
where
    K: Clone + Eq + Hash,
{
    type Completion = ();

    fn step(
        &mut self,
        _action_context: ActionContext<C>,
        _meta: AgentMetadata,
        context: &C,
    ) -> StepResult<Self::Completion> {
        let JoinValueLaneUpdate {
            projection,
            key_value,
        } = self;
        if let Some((key, value)) = key_value.take() {
            let lane = &projection(context).inner;
            lane.update(key, value);
            StepResult::Complete {
                modified_item: Some(Modification::of(lane.id())),
                result: (),
            }
        } else {
            StepResult::after_done()
        }
    }
}
