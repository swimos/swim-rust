// Copyright 2015-2021 SWIM.AI inc.
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

use crate::agent::lane::channels::uplink::backpressure::{
    KeyedBackpressureConfig, SimpleBackpressureConfig,
};
use crate::agent::lane::channels::uplink::map::MapLaneSyncError;
use crate::agent::lane::model::demand_map::DemandMapLane;
use crate::agent::lane::model::demand_map::DemandMapLaneEvent;
use crate::agent::lane::model::map::{make_update, MapLane, MapLaneEvent};
use crate::agent::lane::model::value::ValueLane;
use swim_utilities::future::item_sink::SendError;
use swim_runtime::routing::{RoutingAddr, TaggedSender};
use either::Either;
use futures::future::{ready, BoxFuture};
use futures::stream::{once, unfold, BoxStream, FusedStream};
use futures::{select, select_biased, FutureExt, Stream, StreamExt};
use pin_utils::pin_mut;
use std::any::Any;
use std::error::Error;
use std::fmt::{Debug, Display, Formatter};
use std::ops::Deref;
use std::sync::atomic::Ordering;
use std::sync::Arc;
use stm::transaction::{RetryManager, TransactionError};
use swim_form::structural::read::ReadError;
use swim_form::Form;
use swim_model::path::RelativePath;
use swim_model::Value;
use swim_runtime::backpressure::keyed::map::MapUpdateMessage;
use swim_utilities::errors::Recoverable;
use swim_utilities::future::item_sink::{FnMutSender, ItemSender};
use swim_utilities::time::AtomicInstant;
use swim_warp::envelope::Envelope;
use swim_warp::map::MapUpdate;
use tokio::time::Instant;
use tracing::{event, Level};

pub(crate) mod backpressure;
pub mod map;
pub(crate) mod spawn;
pub mod stateless;
#[cfg(test)]
mod tests;

/// An enumeration representing the type of an uplink.
#[derive(Debug, PartialEq, Eq, Clone, Copy)]
pub enum UplinkKind {
    Action,
    Command,
    Demand,
    DemandMap,
    Map,
    JoinMap,
    JoinValue,
    Supply,
    Spatial,
    Value,
}

/// State change requests to an uplink.
#[derive(Debug, PartialEq, Eq, Clone, Copy)]
pub enum UplinkAction {
    Link,
    Sync,
    Unlink,
}

/// Tracks the state of an uplink.
#[derive(Debug, PartialEq, Eq, Clone, Copy)]
pub enum UplinkState {
    Opened,
    Linked,
    Synced,
}

/// Stages of the lifecycle of an uplink.
pub enum UplinkPhase<'a, Actions, Updates, Msg> {
    /// The uplink is running.
    Running(UplinkState, &'a mut Actions, &'a mut Updates),
    /// The uplink is synchronizing the state of the client.
    Syncing(
        &'a mut Actions,
        BoxStream<'a, PeelResult<'a, Updates, Result<Msg, UplinkError>>>,
    ),
    /// The uplink has terminated.
    Terminated,
}

/// Responses from a lane uplink to its subscriber.
#[derive(Debug, PartialEq, Eq, Clone)]
pub enum UplinkMessage<Ev> {
    Linked,
    Synced,
    Unlinked,
    Event(Ev),
}

impl<K, V> MapUpdateMessage<K, V> for UplinkMessage<MapUpdate<K, V>> {
    fn discriminate(self) -> Either<MapUpdate<K, V>, Self> {
        match self {
            UplinkMessage::Event(update) => Either::Left(update),
            ow => Either::Right(ow),
        }
    }

    fn repack(update: MapUpdate<K, V>) -> Self {
        UplinkMessage::Event(update)
    }
}

/// An addressed uplink message. Either to be broadcast to all uplinks or to a single address.
#[derive(Debug, PartialEq, Eq, Clone)]
pub enum AddressedUplinkMessage<Ev> {
    /// Broadcast the `UplinkMessage` to all uplinks.
    Broadcast(Ev),
    /// Send the `UplinkMessage` to the `RoutingAddr`.
    Addressed { message: Ev, address: RoutingAddr },
}

impl<Ev> AddressedUplinkMessage<Ev> {
    pub fn broadcast(message: Ev) -> AddressedUplinkMessage<Ev> {
        AddressedUplinkMessage::Broadcast(message)
    }

    pub fn addressed(message: Ev, address: RoutingAddr) -> AddressedUplinkMessage<Ev> {
        AddressedUplinkMessage::Addressed { message, address }
    }
}

/// Error conditions for the task running an uplink.
#[derive(Debug)]
pub enum UplinkError {
    /// The subscriber to the uplink has stopped listening.
    ChannelDropped,
    /// The lane stopped reporting its state changes.
    LaneStoppedReporting,
    /// The uplink attempted to execute a transaction against its lane but failed.
    FailedTransaction(TransactionError),
    /// The form used by the lane is inconsistent.
    InconsistentForm(ReadError),
    /// The uplink failed to start after a number of attempts.
    FailedToStart(usize),
}

fn trans_err_fatal(err: &TransactionError) -> bool {
    matches!(
        err,
        TransactionError::HighContention { .. } | TransactionError::TooManyAttempts { .. }
    )
}

impl Recoverable for UplinkError {
    fn is_fatal(&self) -> bool {
        match self {
            UplinkError::LaneStoppedReporting | UplinkError::InconsistentForm(_) => true,
            UplinkError::FailedTransaction(err) => trans_err_fatal(err),
            _ => false,
        }
    }
}

impl From<MapLaneSyncError> for UplinkError {
    fn from(err: MapLaneSyncError) -> Self {
        match err {
            MapLaneSyncError::FailedTransaction(e) => UplinkError::FailedTransaction(e),
            MapLaneSyncError::InconsistentForm(e) => UplinkError::InconsistentForm(e),
        }
    }
}

impl Display for UplinkError {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            UplinkError::ChannelDropped => write!(f, "Uplink send channel was dropped."),
            UplinkError::LaneStoppedReporting => write!(f, "The lane stopped reporting its state."),
            UplinkError::FailedTransaction(err) => {
                write!(f, "The uplink failed to execute a transaction: {}", err)
            }
            UplinkError::InconsistentForm(err) => write!(
                f,
                "A form implementation used by a lane is inconsistent: {}",
                err
            ),
            UplinkError::FailedToStart(n) => {
                write!(f, "Uplink failed to start after {} attempts.", *n)
            }
        }
    }
}

impl Error for UplinkError {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        match self {
            UplinkError::FailedTransaction(err) => Some(err),
            UplinkError::InconsistentForm(err) => Some(err),
            _ => None,
        }
    }
}

// Yield a message to the subscriber.
async fn send_msg<Msg, Sender, SendErr>(
    sender: &mut Sender,
    msg: UplinkMessage<Msg>,
) -> Result<(), UplinkError>
where
    Msg: Any + Send + Sync,
    Sender: ItemSender<UplinkMessage<Msg>, SendErr>,
{
    sender
        .send_item(msg)
        .await
        .map_err(|_| UplinkError::ChannelDropped)
}

/// Date required to run an uplink.
pub struct Uplink<SM, Actions, Updates> {
    /// The uplink state machine.
    state_machine: SM,
    /// Stream of requested state changes.
    actions: Actions,
    /// Stream of updates to the lane.
    updates: Updates,
}

impl<SM, Actions, Updates> Uplink<SM, Actions, Updates> {
    pub fn new(state_machine: SM, actions: Actions, updates: Updates) -> Self {
        Uplink {
            state_machine,
            actions,
            updates,
        }
    }
}

const FAILED_UNLINK: &str = "Failed to send an unlink message to a failed uplink.";
const UPLINK_FAILED: &str = "Uplink task failed.";

impl<SM, Actions, Updates> Uplink<SM, Actions, Updates>
where
    Updates: FusedStream + Send,
    Updates::Item: Send,
    SM: UplinkStateMachine<Updates::Item> + 'static,
    Actions: FusedStream<Item = UplinkAction> + Send,
{
    /// Run the uplink as an asynchronous task.
    pub async fn run_uplink<Sender, SendErr>(
        self,
        mut sender: Sender,
        uplinks_idle_since: Arc<AtomicInstant>,
    ) -> Result<(), UplinkError>
    where
        Sender: ItemSender<UplinkMessage<SM::Msg>, SendErr> + Send + Sync + Clone,
        SendErr: Send,
    {
        let Uplink {
            state_machine,
            actions,
            updates,
            ..
        } = self;
        pin_mut!(actions);
        pin_mut!(updates);
        let update_stream = as_stream(&state_machine, &mut actions, &mut updates);

        let completion = state_machine
            .send_message_stream(update_stream, sender.clone(), uplinks_idle_since)
            .await;
        let attempt_unlink = match &completion {
            Ok(_) => false,
            Err(error) => {
                event!(Level::ERROR, message = UPLINK_FAILED, ?error);
                match error {
                    UplinkError::ChannelDropped => {
                        event!(Level::ERROR, FAILED_UNLINK);
                        false
                    }
                    _ => true,
                }
            }
        };
        if attempt_unlink && sender.send_item(UplinkMessage::Unlinked).await.is_err() {
            event!(Level::ERROR, FAILED_UNLINK);
        }
        completion
    }
}

fn as_stream<'a, SM, Actions, Updates>(
    state_machine: &'a SM,
    actions: &'a mut Actions,
    updates: &'a mut Updates,
) -> impl Stream<Item = Result<UplinkMessage<SM::Msg>, UplinkError>> + 'a
where
    Updates: FusedStream + Send + Unpin,
    SM: UplinkStateMachine<Updates::Item> + 'static,
    Actions: FusedStream<Item = UplinkAction> + Unpin,
{
    let smr = &*state_machine;

    let state: UplinkPhase<'a, Actions, Updates, SM::Msg> =
        UplinkPhase::Running(UplinkState::Opened, actions, updates);

    unfold(state, move |state| async move {
        match state {
            UplinkPhase::Running(UplinkState::Opened, actions, updates) => {
                loop {
                    let maybe_action: Option<UplinkAction> = select_biased! {
                        action = actions.next() => action,
                        upd = updates.next() => {
                            if upd.is_none() {
                                None
                            } else {
                                continue;
                            }
                        }, //Ignore updates until linked.
                    };
                    break handle_action_alt(
                        smr,
                        UplinkState::Opened,
                        updates,
                        actions,
                        maybe_action,
                    )
                    .await;
                }
            }
            UplinkPhase::Running(simple_state, actions, updates) => loop {
                let next: Either<Option<UplinkAction>, Option<Updates::Item>> = select_biased! {
                    action = actions.next() => Either::Left(action),
                    update = updates.next() => Either::Right(update),
                };

                break match next {
                    Either::Left(action) => {
                        handle_action_alt(smr, simple_state, updates, actions, action).await
                    }
                    Either::Right(Some(update)) => match smr.message_for(update) {
                        Ok(Some(msg)) => Some((
                            Ok(UplinkMessage::Event(msg)),
                            UplinkPhase::Running(simple_state, actions, updates),
                        )),
                        Err(e) => {
                            Some((Err(e), UplinkPhase::Running(simple_state, actions, updates)))
                        }
                        _ => continue,
                    },
                    _ => None,
                };
            },
            UplinkPhase::Syncing(actions, mut sync_stream) => {
                let item_and_state = match sync_stream.next().await {
                    Some(PeelResult::Output(event)) => match event {
                        Ok(msg) => (
                            Ok(UplinkMessage::Event(msg)),
                            UplinkPhase::Syncing(actions, sync_stream),
                        ),
                        Err(e) => (Err(e), UplinkPhase::Terminated),
                    },
                    Some(PeelResult::Complete(updates)) => (
                        Ok(UplinkMessage::Synced),
                        UplinkPhase::Running(UplinkState::Synced, actions, updates),
                    ),
                    _ => {
                        panic!("Lane synchronization stream did not return the update stream.")
                    }
                };
                Some(item_and_state)
            }
            UplinkPhase::Terminated => None,
        }
    })
}

const LINKING: &str = "Creating uplink.";
const SYNCING: &str = "Synchronizing uplink.";
const UNLINKING: &str = "Stopping uplink after client request.";

// Change the state of the uplink based on an action.
async fn handle_action_alt<'a, SM, Updates, Actions>(
    state_machine: &'a SM,
    prev_state: UplinkState,
    updates: &'a mut Updates,
    actions: &'a mut Actions,
    action: Option<UplinkAction>,
) -> Option<(
    Result<UplinkMessage<SM::Msg>, UplinkError>,
    UplinkPhase<'a, Actions, Updates, SM::Msg>,
)>
where
    Updates: FusedStream + Send + Unpin + 'a,
    SM: UplinkStateMachine<Updates::Item> + 'static,
    Actions: FusedStream<Item = UplinkAction> + Unpin + 'a,
{
    match action {
        Some(UplinkAction::Link) => {
            //Move into the linked state which will caused updates to be sent.
            event!(Level::DEBUG, LINKING);
            Some((
                Ok(UplinkMessage::Linked),
                UplinkPhase::Running(UplinkState::Linked, actions, updates),
            ))
        }
        Some(UplinkAction::Sync) => {
            let mut sync_stream = state_machine.sync_lane(updates);
            let msg_and_state = if prev_state == UplinkState::Opened {
                event!(Level::DEBUG, LINKING);
                (
                    Ok(UplinkMessage::Linked),
                    UplinkPhase::Syncing(actions, sync_stream),
                )
            } else {
                event!(Level::DEBUG, SYNCING);
                match sync_stream.next().await {
                    Some(PeelResult::Output(event)) => match event {
                        Ok(msg) => (
                            Ok(UplinkMessage::Event(msg)),
                            UplinkPhase::Syncing(actions, sync_stream),
                        ),
                        Err(e) => (Err(e), UplinkPhase::Terminated),
                    },
                    Some(PeelResult::Complete(updates)) => (
                        Ok(UplinkMessage::Synced),
                        UplinkPhase::Running(UplinkState::Synced, actions, updates),
                    ),
                    _ => {
                        panic!("Lane synchronization stream did not return the update stream.")
                    }
                }
            };
            Some(msg_and_state)
        }
        _ => {
            // When an unlink is requested, send the unlinked response and terminate the uplink.
            event!(Level::DEBUG, UNLINKING);
            Some((Ok(UplinkMessage::Unlinked), UplinkPhase::Terminated))
        }
    }
}

/// Item type for streams that peel values from another stream (by reference), returning the
/// reference to the undelying stream before terminating.
pub enum PeelResult<'a, U, T> {
    /// An output peeled from the underlying stream.
    Output(T),
    /// Returning the underlying stream (the stream should return None after this).
    Complete(&'a mut U),
}

impl<'a, U, T> PeelResult<'a, U, T> {
    fn output(self) -> Option<T> {
        match self {
            PeelResult::Output(v) => Some(v),
            _ => None,
        }
    }
}

/// Trait encoding the differences in uplink behaviour for different kinds of lanes.
pub trait UplinkStateMachine<Event>: Send + Sync {
    type Msg: Any + Send + Sync + Debug;

    /// Create a message to send to the subscriber from a lane event (where appropriate).
    fn message_for(&self, event: Event) -> Result<Option<Self::Msg>, UplinkError>;

    /// Create a sync state machine for the lane, this will create a stream that emits messages
    /// until the sync is complete (which should be forwarded to the subscriber) an then terminates,
    /// after which the synced message can be sent.
    fn sync_lane<'a, Updates>(
        &'a self,
        updates: &'a mut Updates,
    ) -> BoxStream<'a, PeelResult<'a, Updates, Result<Self::Msg, UplinkError>>>
    where
        Updates: FusedStream<Item = Event> + Send + Unpin + 'a;

    fn send_message_stream<'a, Messages, Sender, SendErr>(
        &'a self,
        message_stream: Messages,
        sender: Sender,
        uplinks_idle_since: Arc<AtomicInstant>,
    ) -> BoxFuture<'a, Result<(), UplinkError>>
    where
        Messages: Stream<Item = Result<UplinkMessage<Self::Msg>, UplinkError>> + Send + 'a,
        Sender: ItemSender<UplinkMessage<Self::Msg>, SendErr> + Send + Sync + Clone + 'a,
        SendErr: Send + 'a,
    {
        default_send_message_stream(message_stream, sender, uplinks_idle_since).boxed()
    }
}

async fn default_send_message_stream<Msg, Messages, Sender, SendErr>(
    message_stream: Messages,
    mut sender: Sender,
    uplinks_idle_since: Arc<AtomicInstant>,
) -> Result<(), UplinkError>
where
    Msg: Any + Send + Sync + Debug,
    Messages: Stream<Item = Result<UplinkMessage<Msg>, UplinkError>> + Send,
    Sender: ItemSender<UplinkMessage<Msg>, SendErr> + Send + Sync,
{
    pin_mut!(message_stream);
    loop {
        match message_stream.next().await {
            Some(Ok(msg)) => {
                let result = send_msg(&mut sender, msg).await;
                if result.is_err() {
                    break result;
                } else {
                    uplinks_idle_since.store(Instant::now().into_std(), Ordering::Relaxed)
                }
            }
            Some(Err(e)) => {
                break Err(e);
            }
            _ => {
                break Ok(());
            }
        }
    }
}

pub struct ValueLaneUplink<T> {
    lane: ValueLane<T>,
    backpressure_config: Option<SimpleBackpressureConfig>,
}

impl<T> ValueLaneUplink<T>
where
    T: Any + Send + Sync,
{
    pub fn new(lane: ValueLane<T>, backpressure_config: Option<SimpleBackpressureConfig>) -> Self {
        ValueLaneUplink {
            lane,
            backpressure_config,
        }
    }
}

#[derive(Debug, PartialEq, Eq, PartialOrd, Ord)]
pub struct ValueLaneEvent<T>(pub Arc<T>);

impl<T> Clone for ValueLaneEvent<T> {
    fn clone(&self) -> Self {
        ValueLaneEvent(self.0.clone())
    }
}

impl<T> From<Arc<T>> for ValueLaneEvent<T> {
    fn from(t: Arc<T>) -> Self {
        ValueLaneEvent(t)
    }
}

impl<T: Form> From<ValueLaneEvent<T>> for Value {
    fn from(event: ValueLaneEvent<T>) -> Self {
        let ValueLaneEvent(inner) = event;
        match Arc::try_unwrap(inner) {
            Ok(t) => t.into_value(),
            Err(t) => t.as_value(),
        }
    }
}

impl<T> Deref for ValueLaneEvent<T> {
    type Target = T;

    fn deref(&self) -> &Self::Target {
        self.0.deref()
    }
}

const OBTAINED_VALUE_STATE: &str = "Obtained value lane state.";

impl<T> UplinkStateMachine<Arc<T>> for ValueLaneUplink<T>
where
    T: Any + Send + Sync + Debug,
{
    type Msg = ValueLaneEvent<T>;

    fn message_for(&self, event: Arc<T>) -> Result<Option<Self::Msg>, UplinkError> {
        Ok(Some(event.into()))
    }

    fn sync_lane<'a, Updates>(
        &'a self,
        updates: &'a mut Updates,
    ) -> BoxStream<'a, PeelResult<'a, Updates, Result<Self::Msg, UplinkError>>>
    where
        Updates: FusedStream<Item = Arc<T>> + Send + Unpin + 'a,
    {
        let ValueLaneUplink { lane, .. } = self;
        let str = unfold(
            (false, Some(updates)),
            move |(synced, maybe_updates)| async move {
                if let Some(updates) = maybe_updates {
                    if synced {
                        Some((PeelResult::Complete(updates), (true, None)))
                    } else {
                        let lane_state: Option<Arc<T>> = select! {
                            v = lane.load().fuse() => Some(v),
                            maybe_v = updates.next() => maybe_v,
                        };
                        if let Some(v) = lane_state {
                            event!(Level::TRACE, message = OBTAINED_VALUE_STATE, value = ?v);
                            Some((PeelResult::Output(Ok(v.into())), (true, Some(updates))))
                        } else {
                            Some((
                                PeelResult::Output(Err(UplinkError::LaneStoppedReporting)),
                                (true, Some(updates)),
                            ))
                        }
                    }
                } else {
                    None
                }
            },
        );
        Box::pin(str)
    }

    fn send_message_stream<'a, Messages, Sender, SendErr>(
        &'a self,
        message_stream: Messages,
        sender: Sender,
        uplinks_idle_since: Arc<AtomicInstant>,
    ) -> BoxFuture<'a, Result<(), UplinkError>>
    where
        Messages: Stream<Item = Result<UplinkMessage<Self::Msg>, UplinkError>> + Send + 'a,
        Sender: ItemSender<UplinkMessage<Self::Msg>, SendErr> + Send + Sync + Clone + 'a,
        SendErr: Send + 'a,
    {
        async move {
            if let Some(config) = self.backpressure_config {
                backpressure::value_uplink_release_backpressure(message_stream, sender, config)
                    .await
            } else {
                default_send_message_stream(message_stream, sender, uplinks_idle_since).await
            }
        }
        .boxed()
    }
}

/// Uplink for a [`MapLane`].
pub struct MapLaneUplink<K, V, F> {
    /// The underlying [`MapLane`].
    lane: MapLane<K, V>,
    /// A unique (for this lane) ID for this uplink. This is used to identify events corresponding
    /// to checkpoint transactions that were initiated by this uplink.
    id: u64,
    /// A factory for retry strategies to be used for the checkpoint transactions.
    retries: F,
    backpressure_config: Option<KeyedBackpressureConfig>,
}

impl<K, V, F, Retries> MapLaneUplink<K, V, F>
where
    K: Form + Any + Send + Sync,
    V: Any + Send + Sync,
    F: Fn() -> Retries + Send + Sync + 'static,
    Retries: RetryManager + Send + 'static,
{
    pub fn new(
        lane: MapLane<K, V>,
        id: u64,
        retries: F,
        backpressure_config: Option<KeyedBackpressureConfig>,
    ) -> Self {
        MapLaneUplink {
            lane,
            id,
            retries,
            backpressure_config,
        }
    }
}

impl<K, V, Retries, F> UplinkStateMachine<MapLaneEvent<K, V>> for MapLaneUplink<K, V, F>
where
    K: Form + Any + Send + Sync + Debug,
    V: Any + Form + Send + Sync + Debug,
    F: Fn() -> Retries + Send + Sync + 'static,
    Retries: RetryManager + Send + 'static,
{
    type Msg = MapUpdate<Value, V>;

    fn message_for(&self, event: MapLaneEvent<K, V>) -> Result<Option<Self::Msg>, UplinkError> {
        Ok(make_update(event))
    }

    fn sync_lane<'a, Updates>(
        &'a self,
        updates: &'a mut Updates,
    ) -> BoxStream<'a, PeelResult<'a, Updates, Result<Self::Msg, UplinkError>>>
    where
        Updates: FusedStream<Item = MapLaneEvent<K, V>> + Send + Unpin + 'a,
    {
        let MapLaneUplink {
            lane, id, retries, ..
        } = self;
        Box::pin(
            map::sync_map_lane_peel(*id, lane, updates, retries()).filter_map(|r| {
                ready(match r {
                    PeelResult::Output(Ok(event)) => {
                        make_update(event).map(Ok).map(PeelResult::Output)
                    }
                    PeelResult::Output(Err(err)) => Some(PeelResult::Output(Err(err.into()))),
                    PeelResult::Complete(upd) => Some(PeelResult::Complete(upd)),
                })
            }),
        )
    }

    fn send_message_stream<'a, Messages, Sender, SendErr>(
        &'a self,
        message_stream: Messages,
        sender: Sender,
        uplinks_idle_since: Arc<AtomicInstant>,
    ) -> BoxFuture<'a, Result<(), UplinkError>>
    where
        Messages:
            Stream<Item = Result<UplinkMessage<MapUpdate<Value, V>>, UplinkError>> + Send + 'a,
        Sender: ItemSender<UplinkMessage<MapUpdate<Value, V>>, SendErr> + Send + Sync + Clone + 'a,
        SendErr: Send + 'a,
    {
        async move {
            if let Some(config) = self.backpressure_config {
                backpressure::map_uplink_release_backpressure(message_stream, sender, config).await
            } else {
                default_send_message_stream(message_stream, sender, uplinks_idle_since).await
            }
        }
        .boxed()
    }
}

#[derive(Debug, Clone)]
pub(crate) struct UplinkMessageSender<S> {
    inner: S,
    route: RelativePath,
}

impl<S> UplinkMessageSender<S> {
    pub(crate) fn new(inner: S, route: RelativePath) -> Self {
        UplinkMessageSender { inner, route }
    }
}

impl UplinkMessageSender<TaggedSender> {
    pub fn into_item_sender<Msg>(self) -> impl ItemSender<UplinkMessage<Msg>, SendError> + Clone
    where
        Msg: Into<Value> + Send + 'static,
    {
        FnMutSender::new(self, UplinkMessageSender::send_item)
    }

    pub async fn send_item<Msg>(&mut self, msg: UplinkMessage<Msg>) -> Result<(), SendError>
    where
        Msg: Into<Value> + Send + 'static,
    {
        let UplinkMessageSender { inner, route } = self;
        let envelope = match msg {
            UplinkMessage::Linked => Envelope::linked(&route.node, &route.lane),
            UplinkMessage::Synced => Envelope::synced(&route.node, &route.lane),
            UplinkMessage::Unlinked => Envelope::unlinked(&route.node, &route.lane),
            UplinkMessage::Event(ev) => {
                Envelope::make_event(&route.node, &route.lane, Some(ev.into()))
            }
        };
        inner.send_item(envelope).await
    }
}

pub struct DemandMapLaneUplink<Key, Value>
where
    Key: Form,
    Value: Form,
{
    lane: DemandMapLane<Key, Value>,
}

impl<Key, Value> DemandMapLaneUplink<Key, Value>
where
    Key: Clone + Form,
    Value: Clone + Form,
{
    pub fn new(lane: DemandMapLane<Key, Value>) -> DemandMapLaneUplink<Key, Value> {
        DemandMapLaneUplink { lane }
    }
}

impl<Key, Value> UplinkStateMachine<DemandMapLaneEvent<Key, Value>>
    for DemandMapLaneUplink<Key, Value>
where
    Key: Any + Clone + Form + Send + Sync + Debug,
    Value: Any + Clone + Form + Send + Sync + Debug,
{
    type Msg = DemandMapLaneEvent<Key, Value>;

    fn message_for(
        &self,
        event: DemandMapLaneEvent<Key, Value>,
    ) -> Result<Option<Self::Msg>, UplinkError> {
        Ok(Some(event))
    }

    fn sync_lane<'a, Updates>(
        &'a self,
        updates: &'a mut Updates,
    ) -> BoxStream<'a, PeelResult<'a, Updates, Result<Self::Msg, UplinkError>>>
    where
        Updates: FusedStream<Item = DemandMapLaneEvent<Key, Value>> + Send + Unpin + 'a,
    {
        let DemandMapLaneUplink { lane, .. } = self;

        let controller = lane.controller();
        let sync_fut = controller.sync();

        let messages = once(sync_fut)
            .filter_map(|r| match r {
                Ok(v) => ready(Some(v.into_iter())),
                Err(_) => ready(None),
            })
            .flat_map(|v| futures::stream::iter(v).map(|m| PeelResult::Output(Ok(m))));

        Box::pin(
            messages
                .chain(once(ready(PeelResult::Complete(updates))))
                .fuse(),
        )
    }
}
