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

use crate::agent::lane::channels::uplink::spawn::UplinkErrorReport;
use crate::agent::lane::channels::uplink::{
    UplinkAction, UplinkError, UplinkMessage, UplinkMessageSender,
};
use crate::agent::lane::channels::TaggedAction;
use crate::routing::{RoutingAddr, ServerRouter, TaggedSender};
use either::Either;
use futures::{select_biased, Stream, StreamExt};
use pin_utils::pin_mut;
use std::collections::{hash_map::Entry, HashMap};
use std::marker::PhantomData;
use swim_common::form::Form;
use swim_common::model::Value;
use swim_common::sink::item::ItemSink;
use swim_common::warp::path::RelativePath;
use tokio::sync::mpsc;
use tracing::{event, Level};

#[cfg(test)]
mod tests;

/// Manages remote uplinks to an [`ActionLane`].
pub struct ActionLaneUplinks<Response> {
    responses: mpsc::Receiver<(RoutingAddr, Response)>,
    route: RelativePath,
}

impl<Response> ActionLaneUplinks<Response> {
    pub fn new(responses: mpsc::Receiver<(RoutingAddr, Response)>, route: RelativePath) -> Self {
        ActionLaneUplinks { responses, route }
    }
}

const LINKING: &str = "Linking uplink an action lane.";
const SYNCING: &str = "Syncing with an action lane (this is a no-op).";
const UNLINKING: &str = "Unlinking from an action lane.";
const AWAITING_PENDING: &str = "Awaiting pending responses.";
const FAILED_ERR_REPORT: &str = "Failed to send uplink error report.";
const UNLINKING_ALL: &str = "Unlinking remaining uplinks.";

impl<Response> ActionLaneUplinks<Response>
where
    Response: Send + Sync + Form + 'static,
{
    pub async fn run<Router>(
        self,
        uplink_actions: impl Stream<Item = TaggedAction>,
        router: Router,
        err_tx: mpsc::Sender<UplinkErrorReport>,
    ) where
        Router: ServerRouter,
    {
        let ActionLaneUplinks { responses, route } = self;

        let mut uplinks: ActionUplinks<Response, Router> =
            ActionUplinks::new(router, err_tx, route);

        let uplink_actions = uplink_actions.fuse();
        let mut responses = responses.fuse();
        pin_mut!(uplink_actions);

        loop {
            let next: Either<Option<(RoutingAddr, Response)>, Option<TaggedAction>> = select_biased! {
               response = responses.next() => Either::Left(response),
               act = uplink_actions.next() => Either::Right(act),
            };

            match next {
                Either::Left(Some((addr, response))) => {
                    uplinks
                        .send_if_open(UplinkMessage::Event(RespMsg(response)), addr)
                        .await;
                }
                Either::Right(Some(TaggedAction(addr, act))) => match act {
                    UplinkAction::Link => {
                        event!(Level::DEBUG, LINKING);
                        if uplinks.send_msg(UplinkMessage::Linked, addr).await.is_err() {
                            break;
                        }
                    }
                    UplinkAction::Sync => {
                        let linked = uplinks.uplinks.contains_key(&addr);
                        if !linked {
                            event!(Level::DEBUG, LINKING);
                            match uplinks.send_msg(UplinkMessage::Linked, addr).await {
                                Ok(true) => {
                                    event!(Level::DEBUG, SYNCING);
                                    if uplinks.send_msg(UplinkMessage::Synced, addr).await.is_err()
                                    {
                                        break;
                                    }
                                }
                                Err(_) => {
                                    break;
                                }
                                _ => {}
                            }
                        } else {
                            event!(Level::DEBUG, SYNCING);
                            if uplinks.send_msg(UplinkMessage::Synced, addr).await.is_err() {
                                break;
                            }
                        }
                    }
                    UplinkAction::Unlink => {
                        event!(Level::DEBUG, UNLINKING);
                        if uplinks.unlink(addr).await.is_err() {
                            break;
                        }
                    }
                },
                _ => {
                    break;
                }
            }
        }

        event!(Level::DEBUG, AWAITING_PENDING);
        while let Some((addr, response)) = responses.next().await {
            uplinks
                .send_if_open(UplinkMessage::Event(RespMsg(response)), addr)
                .await;
        }

        event!(Level::DEBUG, UNLINKING_ALL);
        uplinks.unlink_all().await;
    }
}

struct RespMsg<R>(R);

impl<R: Form> From<RespMsg<R>> for Value {
    fn from(msg: RespMsg<R>) -> Self {
        msg.0.into_value()
    }
}

/// Wraps a map of uplinks and provides compound operations on them to the uplink task.
struct ActionUplinks<Msg, Router: ServerRouter> {
    router: Router,
    uplinks: HashMap<RoutingAddr, UplinkMessageSender<TaggedSender>>,
    err_tx: mpsc::Sender<UplinkErrorReport>,
    route: RelativePath,
    _input: PhantomData<fn(Msg)>,
}

struct RouterStopping;

impl<Msg, Router> ActionUplinks<Msg, Router>
where
    Router: ServerRouter,
    Msg: Form + Send + 'static,
{
    fn new(router: Router, err_tx: mpsc::Sender<UplinkErrorReport>, route: RelativePath) -> Self {
        ActionUplinks {
            router,
            uplinks: HashMap::new(),
            err_tx,
            route,
            _input: PhantomData,
        }
    }

    /// Get the router handle associated with an address or create a new one if necessary.
    async fn get_sender(
        &mut self,
        addr: RoutingAddr,
    ) -> Result<
        (
            &mut UplinkMessageSender<TaggedSender>,
            &mut mpsc::Sender<UplinkErrorReport>,
        ),
        RouterStopping,
    >
    where
        Router: ServerRouter,
    {
        let ActionUplinks {
            router,
            uplinks,
            route,
            err_tx,
            ..
        } = self;
        match uplinks.entry(addr) {
            Entry::Occupied(entry) => Ok((entry.into_mut(), err_tx)),
            Entry::Vacant(vacant) => match router.get_sender(addr).await {
                Ok(sender) => Ok((
                    vacant.insert(UplinkMessageSender::new(sender, route.clone())),
                    err_tx,
                )),
                _ => Err(RouterStopping),
            },
        }
    }

    /// Attempt to send a message to the specified endpoint, returning whether the operation
    /// succeeded.
    async fn send_msg(
        &mut self,
        msg: UplinkMessage<RespMsg<Msg>>,
        addr: RoutingAddr,
    ) -> Result<bool, RouterStopping> {
        let (sender, err_tx) = self.get_sender(addr).await?;
        if sender.send_item(msg).await.is_err() {
            handle_err(err_tx, addr).await;
            self.uplinks.remove(&addr);
            Ok(false)
        } else {
            Ok(true)
        }
    }

    /// Send a message to the speciffied endpoint only if there is already a router handle in the
    /// map.
    async fn send_if_open(&mut self, msg: UplinkMessage<RespMsg<Msg>>, addr: RoutingAddr) {
        if let Some(sender) = self.uplinks.get_mut(&addr) {
            if sender.send_item(msg).await.is_err() {
                handle_err(&mut self.err_tx, addr).await;
                self.uplinks.remove(&addr);
            }
        }
    }

    /// Remove the uplink to a specified endpoint, sending an unlink message if possible.
    async fn unlink(&mut self, addr: RoutingAddr) -> Result<(), RouterStopping> {
        let ActionUplinks {
            router,
            uplinks,
            err_tx,
            route,
            _input,
        } = self;
        let msg: UplinkMessage<RespMsg<Msg>> = UplinkMessage::Unlinked;
        if let Some(sender) = uplinks.get_mut(&addr) {
            if sender.send_item(msg).await.is_err() {
                handle_err(err_tx, addr).await;
            }
            uplinks.remove(&addr);
            Ok(())
        } else if let Ok(sender) = router.get_sender(addr).await {
            let mut sender = UplinkMessageSender::new(sender, route.clone());
            let _ = sender.send_item(msg).await;
            Ok(())
        } else {
            Err(RouterStopping)
        }
    }

    /// Attempt to send an unlink mesage to all remaining uplinks.
    async fn unlink_all(self) {
        let ActionUplinks {
            uplinks,
            mut err_tx,
            ..
        } = self;
        for (addr, mut sender) in uplinks.into_iter() {
            let msg: UplinkMessage<RespMsg<Msg>> = UplinkMessage::Unlinked;
            if sender.send_item(msg).await.is_err() {
                handle_err(&mut err_tx, addr).await;
            }
        }
    }
}

async fn handle_err(err_tx: &mut mpsc::Sender<UplinkErrorReport>, addr: RoutingAddr) {
    if err_tx
        .send(UplinkErrorReport::new(UplinkError::ChannelDropped, addr))
        .await
        .is_err()
    {
        event!(Level::ERROR, message = FAILED_ERR_REPORT, ?addr);
    }
}
