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

use crate::agent::lane::channels::uplink::spawn::UplinkErrorReport;
use crate::agent::lane::channels::uplink::{
    AddressedUplinkMessage, UplinkAction, UplinkError, UplinkKind, UplinkMessage,
    UplinkMessageSender,
};
use crate::agent::lane::channels::TaggedAction;
use either::Either;
use futures::{select_biased, Stream, StreamExt};
use pin_utils::pin_mut;
use std::collections::{hash_map::Entry, HashMap};
use std::marker::PhantomData;
use swim_common::form::Form;
use swim_common::model::Value;
use swim_common::routing::{RoutingAddr, ServerRouter, TaggedSender};
use swim_common::warp::path::RelativePath;
use tokio::sync::mpsc;
use tracing::{event, Level};

#[cfg(test)]
mod tests;

const LINKING: &str = "Linking uplink a ";
const SYNCING: &str = "Syncing with a ";
const UNLINKING: &str = "Unlinking from a ";
const FAILED_ERR_REPORT: &str = "Failed to send uplink error report.";
const UNLINKING_ALL: &str = "Unlinking remaining uplinks.";

fn format_debug_event(uplink_kind: UplinkKind, msg: &'static str) {
    let event_str = format!("{:?} {} lane ", uplink_kind, msg);
    event!(Level::DEBUG, "{}", event_str.as_str());
}

/// Automatically links and syncs (no-op) uplinks. Either sending events directly to an uplink or
/// broadcasting them to all uplinks.
pub struct StatelessUplinks<S> {
    producer: S,
    route: RelativePath,
    uplink_kind: UplinkKind,
}

impl<S, F> StatelessUplinks<S>
where
    S: Stream<Item = AddressedUplinkMessage<F>>,
    F: Send + Sync + Form + 'static,
{
    pub fn new(producer: S, route: RelativePath, uplink_kind: UplinkKind) -> Self {
        StatelessUplinks {
            producer,
            route,
            uplink_kind,
        }
    }
}

impl<S, F> StatelessUplinks<S>
where
    S: Stream<Item = AddressedUplinkMessage<F>>,
    F: Send + Sync + Form + 'static,
{
    pub async fn run<Router>(
        self,
        uplink_actions: impl Stream<Item = TaggedAction>,
        router: Router,
        err_tx: mpsc::Sender<UplinkErrorReport>,
        yield_mod: usize,
    ) where
        Router: ServerRouter,
    {
        let StatelessUplinks {
            route,
            producer,
            uplink_kind,
        } = self;
        let mut uplinks: Uplinks<F, Router> = Uplinks::new(router, err_tx, route);

        let uplink_actions = uplink_actions.fuse();
        let producer = producer.fuse();

        let mut iteration_count: usize = 0;

        pin_mut!(uplink_actions);
        pin_mut!(producer);

        loop {
            let next: Either<Option<AddressedUplinkMessage<F>>, Option<TaggedAction>> = select_biased! {
               p = producer.next() => Either::Left(p),
               act = uplink_actions.next() => Either::Right(act),
            };

            match next {
                Either::Left(None) => {
                    break;
                }
                Either::Left(Some(item)) => match item {
                    AddressedUplinkMessage::Addressed {
                        message,
                        address: addr,
                    } => {
                        if uplinks
                            .send_msg(UplinkMessage::Event(RespMsg(message)), addr)
                            .await
                            .is_err()
                        {
                            break;
                        }
                    }
                    AddressedUplinkMessage::Broadcast(message) => {
                        if uplinks.broadcast_msg(message).await.is_err() {
                            break;
                        }
                    }
                },
                Either::Right(Some(TaggedAction(addr, act))) => match act {
                    UplinkAction::Link => {
                        format_debug_event(uplink_kind, LINKING);
                        if uplinks.send_msg(UplinkMessage::Linked, addr).await.is_err() {
                            break;
                        }
                    }
                    UplinkAction::Sync => {
                        let linked = uplinks.uplinks.contains_key(&addr);
                        if !linked {
                            format_debug_event(uplink_kind, LINKING);
                            match uplinks.send_msg(UplinkMessage::Linked, addr).await {
                                Ok(true) => {
                                    format_debug_event(uplink_kind, SYNCING);
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
                            format_debug_event(uplink_kind, SYNCING);
                            if uplinks.send_msg(UplinkMessage::Synced, addr).await.is_err() {
                                break;
                            }
                        }

                        if uplinks.insert(addr).await.is_err() {
                            break;
                        }
                    }
                    UplinkAction::Unlink => {
                        format_debug_event(uplink_kind, UNLINKING);
                        if uplinks.unlink(addr).await.is_err() {
                            break;
                        }
                    }
                },
                _ => {
                    break;
                }
            }

            iteration_count += 1;
            if iteration_count % yield_mod == 0 {
                tokio::task::yield_now().await;
            }
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
struct Uplinks<Msg, Router: ServerRouter> {
    router: Router,
    uplinks: HashMap<RoutingAddr, UplinkMessageSender<TaggedSender>>,
    err_tx: mpsc::Sender<UplinkErrorReport>,
    route: RelativePath,
    _input: PhantomData<Msg>,
}

struct RouterStopping;

impl<Msg, Router> Uplinks<Msg, Router>
where
    Router: ServerRouter,
    Msg: Form + Send + 'static,
{
    fn new(router: Router, err_tx: mpsc::Sender<UplinkErrorReport>, route: RelativePath) -> Self {
        Uplinks {
            router,
            uplinks: HashMap::new(),
            err_tx,
            route,
            _input: PhantomData,
        }
    }

    /// Broadcast the message to all uplinks.
    async fn broadcast_msg(&mut self, value: Msg) -> Result<bool, RouterStopping> {
        let value = value.into_value();

        for (addr, sender) in &mut self.uplinks {
            let msg = UplinkMessage::Event(RespMsg(value.clone()));

            if sender.send_item(msg).await.is_err() {
                handle_err(&mut self.err_tx, *addr).await;
                return Ok(false);
            }
        }

        Ok(true)
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
        let Uplinks {
            router,
            uplinks,
            route,
            err_tx,
            ..
        } = self;
        match uplinks.entry(addr) {
            Entry::Occupied(entry) => Ok((entry.into_mut(), err_tx)),
            Entry::Vacant(vacant) => match router.resolve_sender(addr, None).await {
                Ok(sender) => Ok((
                    vacant.insert(UplinkMessageSender::new(sender.sender, route.clone())),
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

    async fn insert(&mut self, addr: RoutingAddr) -> Result<(), RouterStopping>
    where
        Router: ServerRouter,
    {
        let Uplinks {
            router,
            uplinks,
            route,
            ..
        } = self;

        match uplinks.entry(addr) {
            Entry::Occupied(_) => Ok(()),
            Entry::Vacant(vacant) => match router.resolve_sender(addr, None).await {
                Ok(sender) => {
                    vacant.insert(UplinkMessageSender::new(sender.sender, route.clone()));
                    Ok(())
                }
                _ => Err(RouterStopping),
            },
        }
    }

    /// Remove the uplink to a specified endpoint, sending an unlink message if possible.
    async fn unlink(&mut self, addr: RoutingAddr) -> Result<(), RouterStopping> {
        let Uplinks {
            router,
            uplinks,
            err_tx,
            route,
            ..
        } = self;
        let msg: UplinkMessage<RespMsg<Msg>> = UplinkMessage::Unlinked;
        if let Some(sender) = uplinks.get_mut(&addr) {
            if sender.send_item(msg).await.is_err() {
                handle_err(err_tx, addr).await;
            }
            uplinks.remove(&addr);
            Ok(())
        } else if let Ok(sender) = router.resolve_sender(addr, None).await {
            let mut sender = UplinkMessageSender::new(sender.sender, route.clone());
            let _ = sender.send_item(msg).await;
            Ok(())
        } else {
            Err(RouterStopping)
        }
    }

    /// Attempt to send an unlink message to all remaining uplinks.
    async fn unlink_all(self) {
        let Uplinks {
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
