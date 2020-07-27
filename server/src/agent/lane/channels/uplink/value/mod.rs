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

use crate::agent::lane::channels::uplink::{UplinkAction, UplinkError, UplinkMessage, UplinkState};
use crate::agent::lane::model::value::ValueLane;
use common::sink::item::ItemSender;
use futures::stream::FusedStream;
use futures::{select, select_biased, FutureExt, Stream, StreamExt};
use pin_utils::pin_mut;
use std::any::Any;
use std::pin::Pin;
use std::sync::Arc;

pub struct ValueLaneUplinkTask<T> {
    lane: ValueLane<T>,
}

impl<T> ValueLaneUplinkTask<T> {
    pub fn new(lane: ValueLane<T>) -> Self {
        ValueLaneUplinkTask { lane }
    }
}

async fn send_msg<T, Sender, SendErr>(
    sender: &mut Sender,
    msg: UplinkMessage<Arc<T>>,
) -> Result<(), UplinkError>
where
    T: Any + Send + Sync,
    Sender: ItemSender<UplinkMessage<Arc<T>>, SendErr>,
{
    sender
        .send_item(msg)
        .await
        .map_err(|_| UplinkError::SenderDropped)
}

impl<T> ValueLaneUplinkTask<T>
where
    T: Any + Send + Sync,
{
    pub async fn run<Actions, Updates, Sender, SendErr>(
        self,
        actions: Actions,
        updates: Updates,
        mut sender: Sender,
    ) -> Result<(), UplinkError>
    where
        Actions: Stream<Item = UplinkAction>,
        Updates: Stream<Item = Arc<T>>,
        Sender: ItemSender<UplinkMessage<Arc<T>>, SendErr>,
    {
        let updates = updates.fuse();
        let actions = actions.fuse();
        pin_mut!(actions);
        pin_mut!(updates);

        let mut state = UplinkState::Opened;

        let sender = &mut sender;

        loop {
            if state == UplinkState::Opened {
                let action = actions.next().await;
                if let Some(new_state) =
                    self.handle_action(UplinkState::Opened, &mut updates, sender, action).await?
                {
                    state = new_state;
                } else {
                    break Ok(());
                }
            } else {
                select_biased! {
                    action = actions.next() => {
                        if let Some(new_state) = self.handle_action(
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
                            send_msg(sender, UplinkMessage::Event(update)).await?;
                        } else {
                            break Err(UplinkError::LaneStoppedReporting);
                        }
                    },
                }
            }
        }
    }

    async fn handle_action<Updates, Sender, SendErr>(
        &self,
        prev_state: UplinkState,
        updates: &mut Pin<&mut Updates>,
        sender: &mut Sender,
        action: Option<UplinkAction>,
    ) -> Result<Option<UplinkState>, UplinkError>
        where
            Updates: FusedStream<Item = Arc<T>>,
            Sender: ItemSender<UplinkMessage<Arc<T>>, SendErr>,
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
                let lane_state: Option<Arc<T>> = select! {
                v = self.lane.load().fuse() => Some(v),
                maybe_v = updates.next() => maybe_v,
            };
                if let Some(v) = lane_state {
                    send_msg(sender, UplinkMessage::Event(v)).await?;
                    send_msg(sender, UplinkMessage::Synced).await?;
                    Ok(Some(UplinkState::Synced))
                } else {
                    send_msg(sender, UplinkMessage::Unlinked).await?;
                    Err(UplinkError::LaneStoppedReporting)
                }
            }
            _ => {
                send_msg(sender, UplinkMessage::Unlinked).await?;
                Ok(None)
            }
        }
    }
}


