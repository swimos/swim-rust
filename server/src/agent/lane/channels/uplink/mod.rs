// Copyright 2015-2020 SWIM.AI inc.
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

use crate::agent::lane::channels::uplink::map::MapLaneSyncError;
use crate::agent::lane::model::map::{MapLane, MapLaneEvent, MapUpdate};
use crate::agent::lane::model::value::ValueLane;
use common::model::Value;
use common::sink::item::ItemSender;
use futures::future::ready;
use futures::stream::{BoxStream, FusedStream};
use futures::{select, select_biased, FutureExt, StreamExt};
use pin_utils::pin_mut;
use std::any::Any;
use std::error::Error;
use std::fmt::{Display, Formatter};
use std::ops::Deref;
use std::sync::Arc;
use stm::transaction::{RetryManager, TransactionError};
use swim_form::{Form, FormDeserializeErr};
use tracing::{event, Level};

#[cfg(test)]
mod tests;

pub mod map;
pub(crate) mod spawn;

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

/// Responses from a lane uplink to its subscriber.
#[derive(Debug, PartialEq, Eq, Clone)]
pub enum UplinkMessage<Ev> {
    Linked,
    Synced,
    Unlinked,
    Event(Ev),
}

/// Error conditions for the task running an uplink.
#[derive(Debug)]
pub enum UplinkError {
    /// The subscriber to the uplink has stopped listening.
    SenderDropped,
    /// The lane stopped reporting its state changes.
    LaneStoppedReporting,
    /// The uplink attempted to execute a transaction against its lane but failed.
    FailedTransaction(TransactionError),
    /// The form used by the lane is inconsistent.
    InconsistentForm(FormDeserializeErr),
}

fn trans_err_fatal(err: &TransactionError) -> bool {
    match err {
        TransactionError::HighContention { .. } | TransactionError::TooManyAttempts { .. } => false,
        _ => true,
    }
}

impl UplinkError {
    pub fn is_fatal(&self) -> bool {
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
            UplinkError::SenderDropped => write!(f, "Uplink send channel was dropped."),
            UplinkError::LaneStoppedReporting => write!(f, "The lane stopped reporting its state."),
            UplinkError::FailedTransaction(err) => {
                write!(f, "The uplink failed to execute a transaction: {}", err)
            }
            UplinkError::InconsistentForm(err) => write!(
                f,
                "A form implementation used by a lane is inconsistent: {}",
                err
            ),
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
        .map_err(|_| UplinkError::SenderDropped)
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

const FAILED_UNLINK: &str = "Failed to send unlink failed uplink.";

impl<SM, Actions, Updates> Uplink<SM, Actions, Updates>
where
    Updates: FusedStream + Send,
    SM: UplinkStateMachine<Updates::Item>,
    Actions: FusedStream<Item = UplinkAction> + Unpin,
{
    /// Run the uplink as an asynchronous task.
    pub async fn run_uplink<Sender, SendErr>(self, mut sender: Sender) -> Result<(), UplinkError>
    where
        Sender: ItemSender<UplinkMessage<SM::Msg>, SendErr>,
    {
        let result = self.run_uplink_internal(&mut sender).await;
        let attempt_unlink = match &result {
            Ok(_) => false,
            Err(UplinkError::SenderDropped) => {
                event!(Level::ERROR, FAILED_UNLINK);
                false
            }
            _ => true,
        };
        if attempt_unlink && sender.send_item(UplinkMessage::Unlinked).await.is_err() {
            event!(Level::ERROR, FAILED_UNLINK);
        }
        result
    }

    async fn run_uplink_internal<Sender, SendErr>(
        self,
        sender: &mut Sender,
    ) -> Result<(), UplinkError>
    where
        Sender: ItemSender<UplinkMessage<SM::Msg>, SendErr>,
    {
        let Uplink {
            state_machine,
            actions,
            updates,
        } = self;

        pin_mut!(actions);
        pin_mut!(updates);

        let mut state = UplinkState::Opened;

        loop {
            if state == UplinkState::Opened {
                let action = loop {
                    select_biased! {
                        action = actions.next() => {
                            break action;
                        },
                        _ = updates.next() => {}, //Ignore updates until linked.
                    }
                };
                if let Some(new_state) = handle_action(
                    &state_machine,
                    UplinkState::Opened,
                    &mut updates,
                    sender,
                    action,
                )
                .await?
                {
                    state = new_state;
                } else {
                    break Ok(());
                }
            } else {
                select_biased! {
                    action = actions.next() => {
                        if let Some(new_state) = handle_action(
                            &state_machine,
                            UplinkState::Opened,
                            &mut updates,
                            sender,
                            action).await? {

                            state = new_state;
                        } else {
                            break Ok(());
                        }
                    },
                    maybe_update = updates.next() => {
                        if let Some(update) = maybe_update {
                            if let Some(msg) = state_machine.message_for(update)? {
                                send_msg(sender, UplinkMessage::Event(msg)).await?;
                            }
                        } else {
                            break Err(UplinkError::LaneStoppedReporting);
                        }
                    },
                }
            }
        }
    }
}

// Change the state of the uplink based on an action.
async fn handle_action<SM, Updates, Sender, SendErr>(
    state_machine: &SM,
    prev_state: UplinkState,
    updates: &mut Updates,
    sender: &mut Sender,
    action: Option<UplinkAction>,
) -> Result<Option<UplinkState>, UplinkError>
where
    Updates: FusedStream + Send + Unpin,
    SM: UplinkStateMachine<Updates::Item>,
    Sender: ItemSender<UplinkMessage<SM::Msg>, SendErr>,
{
    match action {
        Some(UplinkAction::Link) => {
            //Move into the linked state which will caused updates to be sent.
            send_msg(sender, UplinkMessage::Linked).await?;
            Ok(Some(UplinkState::Linked))
        }
        Some(UplinkAction::Sync) => {
            if prev_state == UplinkState::Opened {
                send_msg(sender, UplinkMessage::Linked).await?;
            }
            // Run the sync state machine until it completes then enter the synced state.
            let sync_stream = state_machine.sync_lane(updates);
            pin_mut!(sync_stream);
            while let Some(result) = sync_stream.next().await {
                send_msg(sender, UplinkMessage::Event(result?)).await?;
            }
            send_msg(sender, UplinkMessage::Synced).await?;
            Ok(Some(UplinkState::Synced))
        }
        _ => {
            // When an unlink is requested, send the unlinked response and terminate the uplink.
            send_msg(sender, UplinkMessage::Unlinked).await?;
            Ok(None)
        }
    }
}

/// Trait encoding the differences in uplink behaviour for different kinds of lanes.
pub trait UplinkStateMachine<Event> {
    type Msg: Any + Send + Sync;

    /// Create a message to send to the subscriber from a lane event (where appropriate).
    fn message_for(&self, event: Event) -> Result<Option<Self::Msg>, UplinkError>;

    /// Create a sync state machine for the lane, this will create a stream that emits messages
    /// until the sync is complete (which should be forwarded to the subscriber) an then terminates,
    /// after which the synced message can be sent.
    fn sync_lane<'a, Updates>(
        &'a self,
        updates: &'a mut Updates,
    ) -> BoxStream<'a, Result<Self::Msg, UplinkError>>
    where
        Updates: FusedStream<Item = Event> + Send + Unpin + 'a;
}

pub struct ValueLaneUplink<T>(ValueLane<T>);

impl<T> ValueLaneUplink<T>
where
    T: Any + Send + Sync,
{
    pub fn new(lane: ValueLane<T>) -> Self {
        ValueLaneUplink(lane)
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

impl<T> UplinkStateMachine<Arc<T>> for ValueLaneUplink<T>
where
    T: Any + Send + Sync,
{
    type Msg = ValueLaneEvent<T>;

    fn message_for(&self, event: Arc<T>) -> Result<Option<Self::Msg>, UplinkError> {
        Ok(Some(event.into()))
    }

    fn sync_lane<'a, Updates>(
        &'a self,
        updates: &'a mut Updates,
    ) -> BoxStream<'a, Result<Self::Msg, UplinkError>>
    where
        Updates: FusedStream<Item = Arc<T>> + Send + Unpin + 'a,
    {
        let ValueLaneUplink(lane) = self;
        let fut = async move {
            let lane_state: Option<Arc<T>> = select! {
                v = lane.load().fuse() => Some(v),
                maybe_v = updates.next() => maybe_v,
            };
            if let Some(v) = lane_state {
                Ok(v.into())
            } else {
                Err(UplinkError::LaneStoppedReporting)
            }
        };

        Box::pin(futures::stream::once(fut))
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
}

impl<K, V, F, Retries> MapLaneUplink<K, V, F>
where
    K: Form + Any + Send + Sync,
    V: Any + Send + Sync,
    F: Fn() -> Retries + Send + Sync + 'static,
    Retries: RetryManager + Send + 'static,
{
    pub fn new(lane: MapLane<K, V>, id: u64, retries: F) -> Self {
        MapLaneUplink { lane, id, retries }
    }
}

impl<K, V, Retries, F> UplinkStateMachine<MapLaneEvent<K, V>> for MapLaneUplink<K, V, F>
where
    K: Form + Any + Send + Sync,
    V: Any + Send + Sync,
    F: Fn() -> Retries + Send + Sync + 'static,
    Retries: RetryManager + Send + 'static,
{
    type Msg = MapUpdate<K, V>;

    fn message_for(&self, event: MapLaneEvent<K, V>) -> Result<Option<Self::Msg>, UplinkError> {
        Ok(MapUpdate::make(event))
    }

    fn sync_lane<'a, Updates>(
        &'a self,
        updates: &'a mut Updates,
    ) -> BoxStream<'a, Result<Self::Msg, UplinkError>>
    where
        Updates: FusedStream<Item = MapLaneEvent<K, V>> + Send + Unpin + 'a,
    {
        let MapLaneUplink { lane, id, retries } = self;
        Box::pin(
            map::sync_map_lane(*id, lane, updates, retries()).filter_map(|r| {
                ready(match r {
                    Ok(event) => MapUpdate::make(event).map(Ok),
                    Err(err) => Some(Err(err.into())),
                })
            }),
        )
    }
}
