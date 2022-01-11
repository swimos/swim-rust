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

use crate::agent::context::AgentExecutionContext;
use crate::agent::dispatch::tests::mock::router::MockRouterInner;
use crate::agent::lane::channels::task::LaneIoError;
use crate::agent::lane::channels::update::UpdateError;
use crate::agent::lane::channels::uplink::spawn::UplinkErrorReport;
use crate::agent::lane::channels::uplink::UplinkError;
use crate::agent::lane::channels::AgentExecutionConfig;
use crate::agent::lane::model::supply::SupplyLane;
use crate::agent::{AttachError, Eff, LaneIo};
use futures::future::BoxFuture;
use futures::FutureExt;
use parking_lot::Mutex;
use server_store::agent::mock::MockNodeStore;
use server_store::agent::SwimNodeStore;
use server_store::plane::mock::MockPlaneStore;
use std::collections::hash_map::Entry;
use std::collections::HashMap;
use std::convert::TryFrom;
use std::sync::Arc;
use stm::transaction::TransactionError;
use swim_metrics::config::MetricAggregatorConfig;
use swim_metrics::{MetaPulseLanes, NodeMetricAggregator};
use swim_model::path::{Path, RelativePath};
use swim_model::Value;
use swim_runtime::compat::{Operation, RequestMessage};
use swim_runtime::error::ConnectionDropped;
use swim_runtime::remote::router::TaggedRouter;
use swim_runtime::remote::RawRoute;
use swim_runtime::routing::{RoutingAddr, TaggedEnvelope, TaggedSender};
use swim_utilities::routing::uri::RelativeUri;
use swim_utilities::time::AtomicInstant;
use swim_utilities::trigger;
use swim_utilities::trigger::promise;
use swim_warp::envelope::Envelope;
use tokio::sync::mpsc;
use tokio::sync::mpsc::{Receiver, Sender};
use tokio::time::Instant;

#[derive(Debug, Default, Clone, Copy)]
pub struct MockLane;

#[derive(Debug)]
pub struct RouteReceiver {
    receiver: Option<mpsc::Receiver<TaggedEnvelope>>,
    _drop_tx: promise::Sender<ConnectionDropped>,
}

impl RouteReceiver {
    fn new(
        receiver: mpsc::Receiver<TaggedEnvelope>,
        drop_tx: promise::Sender<ConnectionDropped>,
    ) -> Self {
        RouteReceiver {
            receiver: Some(receiver),
            _drop_tx: drop_tx,
        }
    }

    fn taken(drop_tx: promise::Sender<ConnectionDropped>) -> Self {
        RouteReceiver {
            receiver: None,
            _drop_tx: drop_tx,
        }
    }
}

mod router {
    use crate::agent::dispatch::tests::mock::RouteReceiver;
    use futures_util::future::BoxFuture;
    use parking_lot::Mutex;
    use std::collections::hash_map::Entry;
    use std::collections::HashMap;
    use std::sync::Arc;
    use swim_model::path::Path;
    use swim_runtime::remote::router::fixture::{invalid, router_fixture, RouterCallback};
    use swim_runtime::remote::router::{PlaneRoutingRequest, RemoteRoutingRequest, TaggedRouter};
    use swim_runtime::remote::RawRoute;
    use swim_runtime::routing::RoutingAddr;
    use swim_utilities::trigger::promise;
    use tokio::sync::mpsc;
    use tokio::task::JoinHandle;

    #[derive(Debug)]
    pub struct MockRouterInner {
        pub buffer_size: usize,
        pub senders: HashMap<RoutingAddr, RawRoute>,
        pub receivers: HashMap<RoutingAddr, RouteReceiver>,
    }

    impl MockRouterInner {
        pub fn new(buffer_size: usize) -> Self {
            MockRouterInner {
                buffer_size,
                senders: Default::default(),
                receivers: Default::default(),
            }
        }
    }

    struct RemoteRouter {
        inner: Arc<Mutex<MockRouterInner>>,
    }

    impl RouterCallback<RemoteRoutingRequest> for RemoteRouter {
        fn call(&mut self, arg: RemoteRoutingRequest) -> BoxFuture<()> {
            let inner = self.inner.clone();
            Box::pin(async move {
                match arg {
                    RemoteRoutingRequest::Endpoint { addr, request } => {
                        let mut lock = inner.lock();
                        let MockRouterInner {
                            buffer_size,
                            senders,
                            receivers,
                        } = &mut *lock;
                        let route = match senders.entry(addr) {
                            Entry::Occupied(entry) => entry.get().clone(),
                            Entry::Vacant(entry) => {
                                let (tx, rx) = mpsc::channel(*buffer_size);
                                let (drop_tx, drop_rx) = promise::promise();
                                entry.insert(RawRoute::new(tx.clone(), drop_rx.clone()));
                                receivers.insert(addr, RouteReceiver::new(rx, drop_tx));
                                RawRoute::new(tx, drop_rx)
                            }
                        };
                        let _r = request.send(Ok(route));
                    }
                    r => panic!("Unexpected request: {:?}", r),
                }
            })
        }
    }

    pub fn make(
        router_addr: RoutingAddr,
        inner: Arc<Mutex<MockRouterInner>>,
    ) -> (TaggedRouter<Path>, JoinHandle<()>) {
        let (router, jh) = router_fixture(invalid, RemoteRouter { inner }, invalid);
        (router.tagged(router_addr), jh)
    }
}

#[derive(Debug, Clone)]
pub struct MockExecutionContext {
    channels: Arc<Mutex<MockRouterInner>>,
    router: TaggedRouter<Path>,
    spawner: mpsc::Sender<Eff>,
    uri: RelativeUri,
    uplinks_idle_since: Arc<AtomicInstant>,
}

impl AgentExecutionContext for MockExecutionContext {
    type Store = SwimNodeStore<MockPlaneStore>;

    fn router_handle(&self) -> TaggedRouter<Path> {
        self.router.clone()
    }

    fn spawner(&self) -> Sender<Eff> {
        self.spawner.clone()
    }

    fn uri(&self) -> &RelativeUri {
        &self.uri
    }

    fn store(&self) -> Self::Store {
        MockNodeStore::mock()
    }

    fn metrics(&self) -> NodeMetricAggregator {
        NodeMetricAggregator::new(
            RelativeUri::try_from("/test").unwrap(),
            trigger::trigger().1,
            MetricAggregatorConfig::default(),
            MetaPulseLanes {
                uplinks: Default::default(),
                lanes: Default::default(),
                node: Box::new(SupplyLane::new(mpsc::channel(1).0)),
            },
        )
        .0
    }

    fn uplinks_idle_since(&self) -> &Arc<AtomicInstant> {
        &self.uplinks_idle_since
    }
}

impl MockExecutionContext {
    pub fn new(router_addr: RoutingAddr, buffer_size: usize, spawner: mpsc::Sender<Eff>) -> Self {
        let inner = Arc::new(Mutex::new(MockRouterInner::new(buffer_size)));
        let (router, _jh) = router::make(router_addr, inner.clone());

        MockExecutionContext {
            channels: inner,
            router,
            spawner,
            uplinks_idle_since: Arc::new(AtomicInstant::new(Instant::now().into_std())),
            uri: RelativeUri::try_from("/mock/router".to_string()).unwrap(),
        }
    }

    pub fn take_receiver(&self, addr: &RoutingAddr) -> Option<mpsc::Receiver<TaggedEnvelope>> {
        let mut lock = self.channels.lock();
        let MockRouterInner {
            buffer_size,
            senders,
            receivers,
        } = &mut *lock;
        match senders.entry(*addr) {
            Entry::Occupied(_) => receivers.get_mut(addr).and_then(|rr| rr.receiver.take()),
            Entry::Vacant(entry) => {
                let (tx, rx) = mpsc::channel(*buffer_size);
                let (drop_tx, drop_rx) = promise::promise();
                entry.insert(RawRoute::new(tx, drop_rx));
                receivers.insert(*addr, RouteReceiver::taken(drop_tx));
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
        mut envelopes: Receiver<RequestMessage<Value>>,
        _config: AgentExecutionConfig,
        context: MockExecutionContext,
    ) -> Result<BoxFuture<'static, Result<Vec<UplinkErrorReport>, LaneIoError>>, AttachError> {
        let mut router = context.router_handle();
        if route.lane == POISON_PILL {
            return Err(AttachError::LaneStoppedReporting);
        }
        Ok(async move {
            let mut senders: HashMap<RoutingAddr, TaggedSender> = HashMap::new();

            let err = loop {
                let next = envelopes.recv().await;
                if let Some(env) = next {
                    let addr = env.origin;

                    if env.body() == Some(&Value::text(POISON_PILL)) {
                        break Some(LaneIoError::for_update_err(
                            env.path.clone(),
                            UpdateError::FailedTransaction(TransactionError::InvalidRetry),
                        ));
                    } else {
                        let response = echo(env);
                        let sender = match senders.entry(addr) {
                            Entry::Occupied(entry) => entry.into_mut(),
                            Entry::Vacant(entry) => {
                                if let Ok(sender) = router.resolve_sender(addr).await {
                                    entry.insert(sender.sender)
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
        envelopes: Receiver<RequestMessage<Value>>,
        config: AgentExecutionConfig,
        context: MockExecutionContext,
    ) -> Result<BoxFuture<'static, Result<Vec<UplinkErrorReport>, LaneIoError>>, AttachError> {
        (*self).attach(route, envelopes, config, context)
    }
}

pub fn echo(env: RequestMessage<Value>) -> Envelope {
    let RequestMessage {
        path: RelativePath { node, lane },
        envelope,
        ..
    } = env;

    match envelope {
        Operation::Link => Envelope::linked().node_uri(node).lane_uri(lane).done(),
        Operation::Sync => Envelope::synced().node_uri(node).lane_uri(lane).done(),
        Operation::Unlink => Envelope::unlinked().node_uri(node).lane_uri(lane).done(),
        Operation::Command(body) => Envelope::command()
            .node_uri(node)
            .lane_uri(lane)
            .body(body)
            .done(),
    }
}
