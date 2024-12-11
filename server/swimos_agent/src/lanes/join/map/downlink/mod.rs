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

use std::{collections::HashSet, hash::Hash};
use swimos_agent_protocol::MapMessage;
use swimos_api::address::Address;
use swimos_form::Form;
use swimos_model::Text;

use crate::{
    agent_model::AgentDescription,
    downlink_lifecycle::{OnConsumeEvent, OnFailed, OnLinked, OnSynced, OnUnlinked},
    event_handler::{
        ActionContext, AndThen, AndThenContextual, ConstHandler, ContextualTrans, Described,
        FollowedBy, HandlerAction, HandlerActionExt, HandlerTrans, Modification, StepResult,
    },
    item::AgentItem,
    lanes::{join::DownlinkStatus, LinkClosedResponse},
    meta::AgentMetadata,
};

use super::{
    lifecycle::{
        on_failed::OnJoinMapFailed, on_linked::OnJoinMapLinked, on_synced::OnJoinMapSynced,
        on_unlinked::OnJoinMapUnlinked,
    },
    JoinMapAddDownlink, JoinMapLane,
};

#[cfg(test)]
mod tests;

/// Wraps a [`crate::lanes::join_map::lifecycle::JoinMapLaneLifecycle`] as an [`crate::downlink_lifecycle::event::EventDownlinkLifecycle`] to
/// allow it to be executed on a downlink.
pub struct JoinMapDownlink<L, K, V, LC, Context> {
    projection: fn(&Context) -> &JoinMapLane<L, K, V>,
    link_key: L,
    lane: Address<Text>,
    lifecycle: LC,
}

impl<L, K, V, LC, Context> JoinMapDownlink<L, K, V, LC, Context> {
    /// # Arguments
    /// * `projection` - Projection from the agent to the join value lane.
    /// * `link_key` - A key to identify the link.
    /// * `lane` - Address of the remote lane to which the downlink will be attached.
    /// * `lifecycle` - The join map lifecycle.
    pub fn new(
        projection: fn(&Context) -> &JoinMapLane<L, K, V>,
        link_key: L,
        lane: Address<Text>,
        lifecycle: LC,
    ) -> Self {
        JoinMapDownlink {
            projection,
            link_key,
            lane,
            lifecycle,
        }
    }
}

impl<L, K, V, LC, Context> OnLinked<Context> for JoinMapDownlink<L, K, V, LC, Context>
where
    Context: AgentDescription,
    L: Clone + Hash + Eq + Send,
    LC: OnJoinMapLinked<L, Context>,
{
    type OnLinkedHandler<'a>
        = FollowedBy<AlterLinkState<L, K, V, Context>, LC::OnJoinMapLinkedHandler<'a>>
    where
        Self: 'a;

    fn on_linked(&self) -> Self::OnLinkedHandler<'_> {
        let JoinMapDownlink {
            projection,
            link_key,
            lane,
            lifecycle,
        } = self;
        let remote = lane.borrow_parts();
        let alter_state =
            AlterLinkState::new(*projection, link_key.clone(), DownlinkStatus::Linked);
        alter_state.followed_by(lifecycle.on_linked(link_key.clone(), remote))
    }
}

/// An action that will alter the state of a link in the join map lane.
pub struct AlterLinkState<L, K, V, Context> {
    projection: fn(&Context) -> &JoinMapLane<L, K, V>,
    link_key: Option<L>,
    state: DownlinkStatus,
}

impl<L, K, V, Context> AlterLinkState<L, K, V, Context> {
    fn new(
        projection: fn(&Context) -> &JoinMapLane<L, K, V>,
        link_key: L,
        state: DownlinkStatus,
    ) -> Self {
        AlterLinkState {
            projection,
            link_key: Some(link_key),
            state,
        }
    }
}

impl<L, K, V, Context> HandlerAction<Context> for AlterLinkState<L, K, V, Context>
where
    Context: AgentDescription,
    L: Hash + Eq,
{
    type Completion = ();

    fn step(
        &mut self,
        _action_context: &mut ActionContext<Context>,
        _meta: AgentMetadata,
        context: &Context,
    ) -> StepResult<Self::Completion> {
        let AlterLinkState {
            projection,
            link_key,
            state,
        } = self;
        if let Some(link_key) = link_key.take() {
            let lane = projection(context);
            let mut guard = lane.link_tracker.borrow_mut();
            let l = guard.links.entry(link_key).or_default();
            l.status = *state;
            StepResult::done(())
        } else {
            StepResult::after_done()
        }
    }

    fn describe(
        &self,
        context: &Context,
        f: &mut std::fmt::Formatter<'_>,
    ) -> Result<(), std::fmt::Error> {
        let AlterLinkState {
            projection,
            link_key,
            state,
        } = self;
        let lane = (projection)(context);
        let name = context.item_name(lane.id());
        f.debug_struct("AlterLinkState")
            .field("id", &lane.id())
            .field("lane_name", &name.as_ref().map(|s| s.as_ref()))
            .field("consumed", &link_key.is_none())
            .field("state", state)
            .finish()
    }
}

impl<L, K, V, LC, Context> OnConsumeEvent<MapMessage<K, V>, Context>
    for JoinMapDownlink<L, K, V, LC, Context>
where
    Context: AgentDescription,
    L: Clone + Hash + Eq + Send,
    LC: Send,
    K: Clone + Hash + Eq + Ord + Send,
{
    type OnEventHandler<'a>
        = JoinMapLaneUpdate<Context, L, K, V>
    where
        Self: 'a;

    fn on_event(&self, message: MapMessage<K, V>) -> Self::OnEventHandler<'_> {
        let JoinMapDownlink {
            projection,
            link_key,
            ..
        } = self;
        JoinMapLaneUpdate::new(*projection, link_key.clone(), message, false)
    }
}

/// An event handler that performs an update on the underlying map of a join map lane
/// when a value is received on one of its downlinks.
pub struct JoinMapLaneUpdate<C, L, K, V> {
    projection: fn(&C) -> &JoinMapLane<L, K, V>,
    key_message: Option<(L, MapMessage<K, V>)>,
    add_link: bool,
}

impl<C, L, K, V> JoinMapLaneUpdate<C, L, K, V> {
    pub fn new(
        projection: fn(&C) -> &JoinMapLane<L, K, V>,
        link_key: L,
        message: MapMessage<K, V>,
        add_link: bool,
    ) -> Self {
        JoinMapLaneUpdate {
            projection,
            key_message: Some((link_key, message)),
            add_link,
        }
    }
}

impl<C, L, K, V> HandlerAction<C> for JoinMapLaneUpdate<C, L, K, V>
where
    C: AgentDescription,
    L: Clone + Hash + Eq,
    K: Clone + Hash + Eq + Ord,
{
    type Completion = ();

    fn step(
        &mut self,
        _action_context: &mut ActionContext<C>,
        _meta: AgentMetadata,
        context: &C,
    ) -> StepResult<Self::Completion> {
        let JoinMapLaneUpdate {
            projection,
            key_message,
            add_link,
        } = self;
        if let Some((link_key, message)) = key_message.take() {
            let lane = projection(context);
            if lane.update(link_key, message, *add_link) {
                StepResult::Complete {
                    modified_item: Some(Modification::of(lane.id())),
                    result: (),
                }
            } else {
                StepResult::done(())
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
        let JoinMapLaneUpdate {
            projection,
            key_message,
            add_link,
        } = self;
        let lane = (projection)(context);
        let name = context.item_name(lane.id());
        f.debug_struct("JoinMapLaneUpdate")
            .field("id", &lane.id())
            .field("lane_name", &name.as_ref().map(|s| s.as_ref()))
            .field("add_link", add_link)
            .field("consumed", &key_message.is_none())
            .finish()
    }
}

impl<L, K, V, LC, Context> OnSynced<(), Context> for JoinMapDownlink<L, K, V, LC, Context>
where
    L: Clone + Hash + Eq + Send,
    LC: OnJoinMapSynced<L, K, Context>,
{
    type OnSyncedHandler<'a>
        = AndThenContextual<
        ConstHandler<L>,
        LC::OnJoinMapSyncedHandler<'a>,
        RetrieveKeys<'a, Context, L, K, V, LC>,
    >
    where
        Self: 'a;

    fn on_synced<'a>(&'a self, _: &()) -> Self::OnSyncedHandler<'a> {
        let JoinMapDownlink {
            projection,
            link_key,
            lane,
            lifecycle,
            ..
        } = self;
        let remote = lane.borrow_parts();
        let transform = RetrieveKeys::new(*projection, remote, lifecycle);
        ConstHandler::from(link_key.clone()).and_then_contextual(transform)
    }
}

/// An event handler that gets the value associated with a key in the map and feeds it to
/// the `on_synced` handler.
pub struct RetrieveKeys<'a, Context, L, K, V, LC> {
    projection: fn(&Context) -> &JoinMapLane<L, K, V>,
    lane: Address<&'a str>,
    lifecycle: &'a LC,
}

impl<'a, Context, L, K, V, LC> RetrieveKeys<'a, Context, L, K, V, LC> {
    fn new(
        projection: fn(&Context) -> &JoinMapLane<L, K, V>,
        lane: Address<&'a str>,
        lifecycle: &'a LC,
    ) -> Self {
        RetrieveKeys {
            projection,
            lane,
            lifecycle,
        }
    }
}

impl<'a, Context, L, K, V, LC> ContextualTrans<Context, L>
    for RetrieveKeys<'a, Context, L, K, V, LC>
where
    L: Eq + Hash + Clone,
    LC: OnJoinMapSynced<L, K, Context>,
{
    type Out = LC::OnJoinMapSyncedHandler<'a>;

    fn transform(self, context: &Context, input: L) -> Self::Out {
        let RetrieveKeys {
            projection,
            lane,
            lifecycle,
        } = self;
        let join_lane = projection(context);
        let empty = HashSet::new();
        let guard = join_lane.link_tracker.borrow();
        let keys = guard.links.get(&input).map(|l| &l.keys).unwrap_or(&empty);
        lifecycle.on_synced(input, lane, keys)
    }
}

type OnUnlinkedWithCleanup<'a, L, K, V, Context, LC> = AndThen<
    <LC as OnJoinMapUnlinked<L, K, Context>>::OnJoinMapUnlinkedHandler<'a>,
    AfterClosed<'a, L, K, V, Context>,
    AfterClosedTrans<'a, L, K, V, Context>,
>;
type OnFailedWithCleanup<'a, L, K, V, Context, LC> = AndThen<
    <LC as OnJoinMapFailed<L, K, Context>>::OnJoinMapFailedHandler<'a>,
    AfterClosed<'a, L, K, V, Context>,
    AfterClosedTrans<'a, L, K, V, Context>,
>;

type JoinMapOnUnlinked<'a, L, K, V, Context, LC> = AndThenContextual<
    ConstHandler<L>,
    OnUnlinkedWithCleanup<'a, L, K, V, Context, LC>,
    RunOnUnlinkedTrans<'a, L, K, V, Context, LC>,
>;

type JoinMapOnFailed<'a, L, K, V, Context, LC> = AndThenContextual<
    ConstHandler<L>,
    OnFailedWithCleanup<'a, L, K, V, Context, LC>,
    RunOnFailedTrans<'a, L, K, V, Context, LC>,
>;

impl<L, K, V, LC, Context> OnUnlinked<Context> for JoinMapDownlink<L, K, V, LC, Context>
where
    Context: AgentDescription + 'static,
    L: Clone + Hash + Eq + Send + 'static,
    K: Form + Clone + Hash + Ord + Eq + Send + 'static,
    K::Rec: Send,
    V: Form + Send + 'static,
    V::BodyRec: Send,
    LC: OnJoinMapUnlinked<L, K, Context>,
{
    type OnUnlinkedHandler<'a>
        = JoinMapOnUnlinked<'a, L, K, V, Context, LC>
    where
        Self: 'a;

    fn on_unlinked(&self) -> Self::OnUnlinkedHandler<'_> {
        let JoinMapDownlink {
            projection,
            link_key,
            lane,
            lifecycle,
        } = self;

        ConstHandler::from(link_key.clone()).and_then_contextual(RunOnUnlinkedTrans::new(
            *projection,
            lane,
            lifecycle,
        ))
    }
}

impl<L, K, V, LC, Context> OnFailed<Context> for JoinMapDownlink<L, K, V, LC, Context>
where
    Context: AgentDescription + 'static,
    L: Clone + Hash + Eq + Send + 'static,
    K: Form + Clone + Hash + Ord + Eq + Send + 'static,
    K::Rec: Send,
    V: Form + Send + 'static,
    V::BodyRec: Send,
    LC: OnJoinMapFailed<L, K, Context>,
{
    type OnFailedHandler<'a>
        = JoinMapOnFailed<'a, L, K, V, Context, LC>
    where
        Self: 'a;

    fn on_failed(&self) -> Self::OnFailedHandler<'_> {
        let JoinMapDownlink {
            projection,
            link_key,
            lane,
            lifecycle,
        } = self;

        ConstHandler::from(link_key.clone()).and_then_contextual(RunOnFailedTrans::new(
            *projection,
            lane,
            lifecycle,
        ))
    }
}

pub struct RunOnUnlinkedTrans<'a, L, K, V, Context, LC> {
    projection: fn(&Context) -> &JoinMapLane<L, K, V>,
    remote_lane: &'a Address<Text>,
    lifecycle: &'a LC,
}

pub struct RunOnFailedTrans<'a, L, K, V, Context, LC> {
    projection: fn(&Context) -> &JoinMapLane<L, K, V>,
    remote_lane: &'a Address<Text>,
    lifecycle: &'a LC,
}

impl<'a, L, K, V, Context, LC> RunOnUnlinkedTrans<'a, L, K, V, Context, LC> {
    pub fn new(
        projection: fn(&Context) -> &JoinMapLane<L, K, V>,
        remote_lane: &'a Address<Text>,
        lifecycle: &'a LC,
    ) -> Self {
        RunOnUnlinkedTrans {
            projection,
            remote_lane,
            lifecycle,
        }
    }
}

impl<'a, L, K, V, Context, LC> RunOnFailedTrans<'a, L, K, V, Context, LC> {
    pub fn new(
        projection: fn(&Context) -> &JoinMapLane<L, K, V>,
        remote_lane: &'a Address<Text>,
        lifecycle: &'a LC,
    ) -> Self {
        RunOnFailedTrans {
            projection,
            remote_lane,
            lifecycle,
        }
    }
}

impl<'a, L, K, V, Context, LC> ContextualTrans<Context, L>
    for RunOnUnlinkedTrans<'a, L, K, V, Context, LC>
where
    Context: AgentDescription + 'static,
    LC: OnJoinMapUnlinked<L, K, Context>,
    L: Clone + Hash + Eq + Send + 'static,
    K: Form + Clone + Hash + Ord + Eq + Send + 'static,
    K::Rec: Send,
    V: Form + Send + 'static,
    V::BodyRec: Send,
{
    type Out = OnUnlinkedWithCleanup<'a, L, K, V, Context, LC>;

    fn transform(self, context: &Context, link_key: L) -> Self::Out {
        let RunOnUnlinkedTrans {
            projection,
            remote_lane,
            lifecycle,
        } = self;
        let lane = projection(context);
        let keys = lane
            .link_tracker
            .borrow_mut()
            .clear(&link_key)
            .unwrap_or_default();
        let remote = remote_lane.borrow_parts();
        lifecycle
            .on_unlinked(link_key.clone(), remote, keys.clone())
            .and_then(AfterClosedTrans {
                projection,
                address: remote_lane,
                link_key,
                keys,
            })
    }
}

impl<'a, L, K, V, Context, LC> ContextualTrans<Context, L>
    for RunOnFailedTrans<'a, L, K, V, Context, LC>
where
    Context: AgentDescription + 'static,
    LC: OnJoinMapFailed<L, K, Context>,
    L: Clone + Hash + Eq + Send + 'static,
    K: Form + Clone + Hash + Ord + Eq + Send + 'static,
    K::Rec: Send,
    V: Form + Send + 'static,
    V::BodyRec: Send,
{
    type Out = OnFailedWithCleanup<'a, L, K, V, Context, LC>;

    fn transform(self, context: &Context, link_key: L) -> Self::Out {
        let RunOnFailedTrans {
            projection,
            remote_lane,
            lifecycle,
        } = self;
        let lane = projection(context);
        let keys = lane
            .link_tracker
            .borrow_mut()
            .clear(&link_key)
            .unwrap_or_default();
        let remote = remote_lane.borrow_parts();
        lifecycle
            .on_failed(link_key.clone(), remote, keys.clone())
            .and_then(AfterClosedTrans {
                projection,
                address: remote_lane,
                link_key,
                keys,
            })
    }
}

pub struct AfterClosedTrans<'a, L, K, V, Context> {
    projection: fn(&Context) -> &JoinMapLane<L, K, V>,
    address: &'a Address<Text>,
    link_key: L,
    keys: HashSet<K>,
}

impl<'a, L, K, V, Context> HandlerTrans<LinkClosedResponse>
    for AfterClosedTrans<'a, L, K, V, Context>
{
    type Out = AfterClosed<'a, L, K, V, Context>;

    fn transform(self, response: LinkClosedResponse) -> Self::Out {
        let AfterClosedTrans {
            projection,
            address,
            link_key,
            keys,
        } = self;
        AfterClosed::Cleanup {
            projection,
            address,
            link_key,
            response,
            keys,
        }
    }
}

/// An event handler that cleans up after a downlink unlinks or fails.
#[derive(Default)]
pub enum AfterClosed<'a, L, K, V, Context> {
    Cleanup {
        link_key: L,
        projection: fn(&Context) -> &JoinMapLane<L, K, V>,
        address: &'a Address<Text>,
        response: LinkClosedResponse,
        keys: HashSet<K>,
    },
    Restarting(JoinMapAddDownlink<Context, L, K, V>),
    #[default]
    Done,
}

impl<L, K, V, Context> HandlerAction<Context> for AfterClosed<'_, L, K, V, Context>
where
    Context: AgentDescription + 'static,
    L: Clone + Hash + Eq + Send + 'static,
    K: Form + Clone + Hash + Ord + Eq + Send + 'static,
    K::Rec: Send,
    V: Form + Send + 'static,
    V::BodyRec: Send,
{
    type Completion = ();

    fn step(
        &mut self,
        action_context: &mut ActionContext<Context>,
        meta: AgentMetadata,
        context: &Context,
    ) -> StepResult<Self::Completion> {
        loop {
            match std::mem::take(self) {
                AfterClosed::Cleanup {
                    link_key,
                    projection,
                    address,
                    response,
                    keys,
                } => {
                    let JoinMapLane {
                        inner,
                        link_tracker,
                    } = projection(context);
                    link_tracker.borrow_mut().remove_link(&link_key);
                    match response {
                        LinkClosedResponse::Abandon => break StepResult::done(()),
                        LinkClosedResponse::Delete => {
                            break if keys.is_empty() {
                                StepResult::done(())
                            } else {
                                for key in keys.iter() {
                                    inner.remove(key);
                                }
                                StepResult::Complete {
                                    modified_item: Some(Modification::of(inner.id())),
                                    result: (),
                                }
                            };
                        }
                        LinkClosedResponse::Retry => {
                            *self = AfterClosed::Restarting(JoinMapAddDownlink::new(
                                projection,
                                link_key,
                                address.clone(),
                            ));
                        }
                    }
                }
                AfterClosed::Restarting(mut handler) => {
                    let result = handler.step(action_context, meta, context);
                    if result.is_cont() {
                        *self = AfterClosed::Restarting(handler);
                    }
                    break result;
                }
                AfterClosed::Done => break StepResult::after_done(),
            }
        }
    }

    fn describe(
        &self,
        context: &Context,
        f: &mut std::fmt::Formatter<'_>,
    ) -> Result<(), std::fmt::Error> {
        match self {
            AfterClosed::Cleanup {
                projection,
                address,
                response,
                ..
            } => {
                let lane = (projection)(context);
                let name = context.item_name(lane.id());
                f.debug_struct("AfterClosed")
                    .field("state", &"Cleanup")
                    .field("id", &lane.id())
                    .field("lane_name", &name.as_ref().map(|s| s.as_ref()))
                    .field("address", address)
                    .field("response", response)
                    .finish()
            }
            AfterClosed::Restarting(handler) => f
                .debug_struct("AfterClosed")
                .field("state", &"Restarting")
                .field("handler", &Described::new(context, handler))
                .finish(),
            AfterClosed::Done => f
                .debug_struct("AfterClosed")
                .field("state", &"Done")
                .finish(),
        }
    }
}
