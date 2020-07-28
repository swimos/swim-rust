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
use crate::agent::lane::LaneModel;
use common::sink::item::ItemSender;
use futures::future::ready;
use futures::stream::{BoxStream, FusedStream};
use futures::{select, select_biased, FutureExt, StreamExt};
use pin_utils::pin_mut;
use std::any::Any;
use std::error::Error;
use std::fmt::{Display, Formatter};
use std::sync::Arc;
use stm::transaction::{RetryManager, TransactionError};
use swim_form::{Form, FormDeserializeErr};

pub mod map;

#[derive(Debug, PartialEq, Eq, Clone, Copy)]
pub enum UplinkAction {
    Link,
    Sync,
    Unlink,
}

#[derive(Debug, PartialEq, Eq, Clone, Copy)]
pub enum UplinkState {
    Opened,
    Linked,
    Synced,
}

#[derive(Debug, PartialEq, Eq, Clone)]
pub enum UplinkMessage<Ev> {
    Linked,
    Synced,
    Unlinked,
    Event(Ev),
}

#[derive(Debug)]
pub enum UplinkError {
    SenderDropped,
    LaneStoppedReporting,
    FailedTransaction(TransactionError),
    InconsistentForm(FormDeserializeErr),
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

impl Error for UplinkError {}

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

pub struct Uplink<Lane, Actions, Updates> {
    lane: Lane,
    actions: Actions,
    updates: Updates,
}

impl<Lane, Actions, Updates> Uplink<Lane, Actions, Updates>
where
    Lane: UplinkStateMachine,
    Actions: FusedStream<Item = UplinkAction> + Unpin,
    Updates: FusedStream<Item = Lane::Event> + Send + Unpin,
{
    pub async fn run_uplink<Sender, SendErr>(self, mut sender: Sender) -> Result<(), UplinkError>
    where
        Sender: ItemSender<UplinkMessage<Lane::Msg>, SendErr>,
    {
        let Uplink {
            lane,
            mut actions,
            mut updates,
        } = self;

        let mut state = UplinkState::Opened;

        let sender = &mut sender;

        loop {
            if state == UplinkState::Opened {
                let action = actions.next().await;
                if let Some(new_state) =
                    handle_action(&lane, UplinkState::Opened, &mut updates, sender, action).await?
                {
                    state = new_state;
                } else {
                    break Ok(());
                }
            } else {
                select_biased! {
                    action = actions.next() => {
                        if let Some(new_state) = handle_action(
                            &lane,
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
                            if let Some(msg) = lane.message_for(update)? {
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

async fn handle_action<Lane, Updates, Sender, SendErr>(
    lane: &Lane,
    prev_state: UplinkState,
    updates: &mut Updates,
    sender: &mut Sender,
    action: Option<UplinkAction>,
) -> Result<Option<UplinkState>, UplinkError>
where
    Lane: UplinkStateMachine,
    Updates: FusedStream<Item = Lane::Event> + Send + Unpin,
    Sender: ItemSender<UplinkMessage<Lane::Msg>, SendErr>,
{
    match action {
        Some(UplinkAction::Link) => {
            send_msg(sender, UplinkMessage::Linked).await?;
            Ok(Some(UplinkState::Linked))
        }
        Some(UplinkAction::Sync) => {
            if prev_state == UplinkState::Opened {
                send_msg(sender, UplinkMessage::Linked).await?;
            }
            let sync_stream = lane.sync_lane(updates);
            pin_mut!(sync_stream);
            while let Some(result) = sync_stream.next().await {
                send_msg(sender, UplinkMessage::Event(result?)).await?;
            }
            send_msg(sender, UplinkMessage::Synced).await?;
            Ok(Some(UplinkState::Synced))
        }
        _ => {
            send_msg(sender, UplinkMessage::Unlinked).await?;
            Ok(None)
        }
    }
}

pub trait UplinkStateMachine: LaneModel {
    type Msg: Any + Send + Sync;

    fn message_for(&self, event: Self::Event) -> Result<Option<Self::Msg>, UplinkError>;

    fn sync_lane<'a, Updates>(
        &'a self,
        updates: &'a mut Updates,
    ) -> BoxStream<'a, Result<Self::Msg, UplinkError>>
    where
        Updates: FusedStream<Item = Self::Event> + Send + Unpin + 'a;
}

impl<T> UplinkStateMachine for ValueLane<T>
where
    T: Any + Send + Sync,
{
    type Msg = Arc<T>;

    fn message_for(&self, event: Self::Event) -> Result<Option<Self::Msg>, UplinkError> {
        Ok(Some(event))
    }

    fn sync_lane<'a, Updates>(
        &'a self,
        updates: &'a mut Updates,
    ) -> BoxStream<'a, Result<Self::Msg, UplinkError>>
    where
        Updates: FusedStream<Item = Self::Event> + Send + Unpin + 'a,
    {
        let fut = async move {
            let lane_state: Option<Arc<T>> = select! {
                v = self.load().fuse() => Some(v),
                maybe_v = updates.next() => maybe_v,
            };
            if let Some(v) = lane_state {
                Ok(v)
            } else {
                Err(UplinkError::LaneStoppedReporting)
            }
        };

        Box::pin(futures::stream::once(fut))
    }
}

pub struct MapLaneUplink<K, V, Retries> {
    lane: MapLane<K, V>,
    id: u64,
    retries: Retries,
}

impl<K, V, Retries> LaneModel for MapLaneUplink<K, V, Retries>
where
    K: Form + Any + Send + Sync,
    V: Any + Send + Sync,
    Retries: RetryManager + Clone + Send,
{
    type Event = MapLaneEvent<K, V>;

    fn same_lane(this: &Self, other: &Self) -> bool {
        LaneModel::same_lane(&this.lane, &other.lane)
    }
}

impl<K, V, Retries> UplinkStateMachine for MapLaneUplink<K, V, Retries>
where
    K: Form + Any + Send + Sync,
    V: Any + Send + Sync,
    Retries: RetryManager + Clone + Send + 'static,
{
    type Msg = MapUpdate<K, V>;

    fn message_for(&self, event: Self::Event) -> Result<Option<Self::Msg>, UplinkError> {
        Ok(MapUpdate::make(event))
    }

    fn sync_lane<'a, Updates>(
        &'a self,
        updates: &'a mut Updates,
    ) -> BoxStream<'a, Result<Self::Msg, UplinkError>>
    where
        Updates: FusedStream<Item = Self::Event> + Send + Unpin + 'a,
    {
        let MapLaneUplink { lane, id, retries } = self;
        Box::pin(
            map::sync_map_lane(*id, lane, updates, retries.clone()).filter_map(|r| {
                ready(match r {
                    Ok(event) => MapUpdate::make(event).map(Ok),
                    Err(err) => Some(Err(err.into())),
                })
            }),
        )
    }
}
