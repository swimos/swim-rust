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

use crate::agent::context::AgentExecutionContext;
use crate::agent::lane::channels::task::LaneIoError;
use crate::agent::lane::channels::update::UpdateError;
use crate::agent::lane::channels::uplink::spawn::UplinkErrorReport;
use crate::agent::lane::channels::uplink::UplinkError;
use crate::agent::lane::channels::AgentExecutionConfig;
use crate::agent::{AttachError, Eff, LaneIo};
use crate::plane::error::ResolutionError;
use crate::routing::{RoutingAddr, ServerRouter, TaggedClientEnvelope};
use futures::future::BoxFuture;
use futures::{FutureExt, TryFutureExt};
use http::Uri;
use parking_lot::Mutex;
use std::collections::hash_map::Entry;
use std::collections::HashMap;
use std::sync::Arc;
use stm::transaction::TransactionError;
use swim_common::model::Value;
use swim_common::routing::RoutingError;
use swim_common::sink::item::ItemSink;
use swim_common::warp::envelope::{Envelope, OutgoingHeader, OutgoingLinkMessage};
use swim_common::warp::path::RelativePath;
use tokio::sync::mpsc;
use tokio::sync::mpsc::{Receiver, Sender};
use url::Url;

#[derive(Debug, Default, Clone, Copy)]
pub struct MockLane;

#[derive(Debug, Clone)]
pub struct MockSender(mpsc::Sender<Envelope>);

impl<'a> ItemSink<'a, Envelope> for MockSender {
    type Error = RoutingError;
    type SendFuture = BoxFuture<'a, Result<(), RoutingError>>;

    fn send_item(&'a mut self, value: Envelope) -> Self::SendFuture {
        self.0
            .send(value)
            .map_err(|_| RoutingError::RouterDropped)
            .boxed()
    }
}

#[derive(Debug)]
struct MockRouterInner {
    buffer_size: usize,
    senders: HashMap<RoutingAddr, mpsc::Sender<Envelope>>,
    receivers: HashMap<RoutingAddr, mpsc::Receiver<Envelope>>,
}

impl MockRouterInner {
    fn new(buffer_size: usize) -> Self {
        MockRouterInner {
            buffer_size,
            senders: Default::default(),
            receivers: Default::default(),
        }
    }
}

#[derive(Debug, Clone)]
pub struct MockRouter(Arc<Mutex<MockRouterInner>>);

impl ServerRouter for MockRouter {
    type Sender = MockSender;

    fn get_sender(&mut self, addr: RoutingAddr) -> BoxFuture<Result<Self::Sender, RoutingError>> {
        async move {
            let mut lock = self.0.lock();
            let MockRouterInner {
                buffer_size,
                senders,
                receivers,
            } = &mut *lock;
            let tx = match senders.entry(addr) {
                Entry::Occupied(entry) => entry.get().clone(),
                Entry::Vacant(entry) => {
                    let (tx, rx) = mpsc::channel(*buffer_size);
                    entry.insert(tx.clone());
                    receivers.insert(addr, rx);
                    tx
                }
            };
            Ok(MockSender(tx))
        }
        .boxed()
    }

    fn resolve(
        &mut self,
        _host: Option<Url>,
        _route: Uri,
    ) -> BoxFuture<'static, Result<RoutingAddr, ResolutionError>> {
        panic!("Unexpected resolution attempt.")
    }
}

#[derive(Debug, Clone)]
pub struct MockExecutionContext {
    router: Arc<Mutex<MockRouterInner>>,
    spawner: mpsc::Sender<Eff>,
}

impl AgentExecutionContext for MockExecutionContext {
    type Router = MockRouter;

    fn router_handle(&self) -> Self::Router {
        MockRouter(self.router.clone())
    }

    fn spawner(&self) -> Sender<Eff> {
        self.spawner.clone()
    }
}

impl MockExecutionContext {
    pub fn new(buffer_size: usize, spawner: mpsc::Sender<Eff>) -> Self {
        MockExecutionContext {
            router: Arc::new(Mutex::new(MockRouterInner::new(buffer_size))),
            spawner,
        }
    }

    pub fn take_receiver(&self, addr: &RoutingAddr) -> Option<mpsc::Receiver<Envelope>> {
        let mut lock = self.router.lock();
        let MockRouterInner {
            buffer_size,
            senders,
            receivers,
        } = &mut *lock;
        match senders.entry(*addr) {
            Entry::Occupied(_) => receivers.remove(addr),
            Entry::Vacant(entry) => {
                let (tx, rx) = mpsc::channel(*buffer_size);
                entry.insert(tx);
                Some(rx)
            }
        }
    }
}

pub const POISON_PILL: &str = "FAIL";

impl LaneIo<MockExecutionContext> for MockLane {
    fn attach(
        self,
        route: RelativePath,
        mut envelopes: Receiver<TaggedClientEnvelope>,
        _config: AgentExecutionConfig,
        context: MockExecutionContext,
    ) -> Result<BoxFuture<'static, Result<Vec<UplinkErrorReport>, LaneIoError>>, AttachError> {
        let mut router = context.router_handle();
        if route.lane == POISON_PILL {
            return Err(AttachError::LaneStoppedReporting);
        }
        Ok(async move {
            let mut senders: HashMap<RoutingAddr, MockSender> = HashMap::new();

            let err = loop {
                let next = envelopes.recv().await;
                if let Some(env) = next {
                    let TaggedClientEnvelope(
                        addr,
                        OutgoingLinkMessage {
                            header,
                            path: _,
                            body,
                        },
                    ) = env;

                    if body == Some(Value::text(POISON_PILL)) {
                        break Some(LaneIoError::for_update_err(
                            route.clone(),
                            UpdateError::FailedTransaction(TransactionError::InvalidRetry),
                        ));
                    } else {
                        let response = echo(&route, header, body);
                        let sender = match senders.entry(addr) {
                            Entry::Occupied(entry) => entry.into_mut(),
                            Entry::Vacant(entry) => {
                                if let Ok(sender) = router.get_sender(addr).await {
                                    entry.insert(sender)
                                } else {
                                    break Some(LaneIoError::for_uplink_errors(
                                        route.clone(),
                                        vec![UplinkErrorReport::new(
                                            UplinkError::ChannelDropped,
                                            addr,
                                        )],
                                    ));
                                }
                            }
                        };
                        if sender.send_item(response).await.is_err() {
                            break Some(LaneIoError::for_uplink_errors(
                                route.clone(),
                                vec![UplinkErrorReport::new(UplinkError::ChannelDropped, addr)],
                            ));
                        }
                    }
                } else {
                    break None;
                }
            };

            if let Some(error) = err {
                Err(error)
            } else {
                Ok(vec![])
            }
        }
        .boxed())
    }

    fn attach_boxed(
        self: Box<Self>,
        route: RelativePath,
        envelopes: Receiver<TaggedClientEnvelope>,
        config: AgentExecutionConfig,
        context: MockExecutionContext,
    ) -> Result<BoxFuture<'static, Result<Vec<UplinkErrorReport>, LaneIoError>>, AttachError> {
        (*self).attach(route, envelopes, config, context)
    }
}

pub fn echo(route: &RelativePath, header: OutgoingHeader, body: Option<Value>) -> Envelope {
    match header {
        OutgoingHeader::Link(_) => Envelope::linked(&route.node, &route.lane),
        OutgoingHeader::Sync(_) => Envelope::synced(&route.node, &route.lane),
        OutgoingHeader::Unlink => Envelope::unlinked(&route.node, &route.lane),
        OutgoingHeader::Command => Envelope::make_event(&route.node, &route.lane, body),
    }
}
