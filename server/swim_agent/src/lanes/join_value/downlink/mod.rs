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

use swim_model::{address::Address, Text};

use crate::{
    downlink_lifecycle::{
        event::on_event::OnConsumeEvent, on_failed::OnFailed, on_linked::OnLinked,
        on_synced::OnSynced, on_unlinked::OnUnlinked,
    },
    event_handler::{
        ActionContext, AndThen, AndThenContextual, ConstHandler, ContextualTrans, FollowedBy,
        HandlerAction, HandlerActionExt, HandlerTrans, Modification, StepResult,
    },
    item::AgentItem,
    meta::AgentMetadata,
};

use super::{
    lifecycle::{
        on_failed::OnJoinValueFailed, on_linked::OnJoinValueLinked, on_synced::OnJoinValueSynced,
        on_unlinked::OnJoinValueUnlinked,
    },
    DownlinkStatus, JoinValueLane, LinkClosedResponse,
};

#[cfg(test)]
mod tests;

/// Wraps a [`crate::lanes::join_value::JoinValueLaneLifecycle`] as an [`crate::downlink_lifecycle::event::EventDownlinkLifecycle`] to
/// allow it to be executed on a downlink.
pub struct JoinValueDownlink<K, V, LC, Context> {
    projection: fn(&Context) -> &JoinValueLane<K, V>,
    key: K,
    lane: Address<Text>,
    lifecycle: LC,
}

impl<K, V, LC, Context> JoinValueDownlink<K, V, LC, Context> {
    
    /// #Arguments
    /// * `projection` - Projection from the agent to the join value lane.
    /// * `key` - The key in the join value lane associated with the downlink.
    /// * `lane` - Address of the remote lane to which the downlink will be attached.
    /// * `lifecycle` - The join value lifecycle.
    pub fn new(
        projection: fn(&Context) -> &JoinValueLane<K, V>,
        key: K,
        lane: Address<Text>,
        lifecycle: LC,
    ) -> Self {
        JoinValueDownlink {
            projection,
            key,
            lane,
            lifecycle,
        }
    }
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
            lane,
            lifecycle,
        } = self;
        let remote = lane.borrow_parts();
        let alter_state =
            AlterKeyState::new(*projection, key.clone(), Some(DownlinkStatus::Linked));
        alter_state.followed_by(lifecycle.on_linked(key.clone(), remote))
    }
}

impl<K, V, LC, Context> OnSynced<(), Context> for JoinValueDownlink<K, V, LC, Context>
where
    K: Clone + Hash + Eq + Send,
    LC: OnJoinValueSynced<K, V, Context>,
{
    type OnSyncedHandler<'a> = AndThenContextual<
        ConstHandler<K>,
        LC::OnJoinValueSyncedHandler<'a>,
        RetrieveSynced<'a, Context, K, V, LC>
    >
    where
        Self: 'a;

    fn on_synced<'a>(&'a self, _: &()) -> Self::OnSyncedHandler<'a> {
        let JoinValueDownlink {
            projection,
            key,
            lane,
            lifecycle,
            ..
        } = self;
        let remote = lane.borrow_parts();
        let transform = RetrieveSynced::new(*projection, remote, lifecycle);
        ConstHandler::from(key.clone()).and_then_contextual(transform)
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
            lane,
            lifecycle,
        } = self;
        let remote = lane.borrow_parts();
        lifecycle
            .on_unlinked(key.clone(), remote)
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
            lane,
            lifecycle,
        } = self;
        let remote = lane.borrow_parts();
        lifecycle
            .on_failed(key.clone(), remote)
            .and_then(AfterClosedTrans::new(*projection, key.clone()))
    }
}

impl<K, V, LC, Context> OnConsumeEvent<V, Context> for JoinValueDownlink<K, V, LC, Context>
where
    LC: Send,
    K: Clone + Hash + Eq + Send,
{
    type OnEventHandler<'a> = JoinValueLaneUpdate<Context, K, V>
    where
        Self: 'a;

    fn on_event(&self, value: V) -> Self::OnEventHandler<'_> {
        let JoinValueDownlink {
            projection, key, ..
        } = self;
        JoinValueLaneUpdate::new(*projection, key.clone(), value)
    }
}

/// An action that will alter the state of a key in the join value lane (to indicate whether
/// it has an active downlink associated with it or not).
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
        _action_context: &mut ActionContext<Context>,
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

/// An event handler that cleans up after a downlink unlinks or fails.
pub struct AfterClosed<K, V, Context> {
    projection: fn(&Context) -> &JoinValueLane<K, V>,
    key: K,
    response: Option<LinkClosedResponse>,
}

impl<K, V, Context> AfterClosed<K, V, Context> {
    pub fn new(
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
    K: Hash + Eq + Clone,
{
    type Completion = ();

    fn step(
        &mut self,
        _action_context: &mut ActionContext<Context>,
        _meta: AgentMetadata,
        context: &Context,
    ) -> StepResult<Self::Completion> {
        let AfterClosed {
            projection,
            key,
            response,
        } = self;
        let lane = projection(context);
        let mut guard = lane.keys.borrow_mut();
        guard.remove(key);
        drop(guard);
        match response.take() {
            Some(LinkClosedResponse::Abandon) => StepResult::done(()),
            Some(LinkClosedResponse::Delete) => {
                lane.inner.remove(key);
                StepResult::Complete {
                    modified_item: Some(Modification::of(lane.id())),
                    result: (),
                }
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

/// An event handler that performs an update on the underlying map of a join value lane
/// when a value is received on one of its downlinks.
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
        _action_context: &mut ActionContext<C>,
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

/// An event handler that gets the value associated with a key in the map and feeds it to
/// the `on_synced` handler.
pub struct RetrieveSynced<'a, Context, K, V, LC> {
    projection: fn(&Context) -> &JoinValueLane<K, V>,
    lane: Address<&'a str>,
    lifecycle: &'a LC,
}

impl<'a, Context, K, V, LC> RetrieveSynced<'a, Context, K, V, LC> {
    fn new(
        projection: fn(&Context) -> &JoinValueLane<K, V>,
        lane: Address<&'a str>,
        lifecycle: &'a LC,
    ) -> Self {
        RetrieveSynced {
            projection,
            lane,
            lifecycle,
        }
    }
}

impl<'a, Context, K, V, LC> ContextualTrans<Context, K> for RetrieveSynced<'a, Context, K, V, LC>
where
    K: Eq + Hash + Clone,
    LC: OnJoinValueSynced<K, V, Context>,
{
    type Out = LC::OnJoinValueSyncedHandler<'a>;

    fn transform(self, context: &Context, input: K) -> Self::Out {
        let RetrieveSynced {
            projection,
            lane,
            lifecycle,
        } = self;
        let join_lane = projection(context);
        let key = input.clone();
        join_lane.inner.get(&input, |maybe_value| {
            lifecycle.on_synced(key, lane, maybe_value)
        })
    }
}
