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

use crate::forest::UriForest;
use crate::meta_mesh::{run_task, NodeInfo, NodeInfoCount, NodeInfoList};
use crate::model::AgentIntrospectionUpdater;
use crate::task::AgentMeta;
use futures::future::{join, BoxFuture};
use futures::{SinkExt, StreamExt};
use parking_lot::RwLock;
use std::fmt::Debug;
use std::future::Future;
use std::num::NonZeroUsize;
use std::sync::{Arc, OnceLock};
use swimos_agent_protocol::encoding::lane::{MapLaneResponseDecoder, RawValueLaneRequestEncoder};
use swimos_agent_protocol::{LaneRequest, LaneResponse, MapOperation};
use swimos_api::agent::{
    AgentContext, DownlinkKind, HttpLaneRequestChannel, LaneConfig, StoreKind, WarpLaneKind,
};
use swimos_api::error::{
    AgentRuntimeError, DownlinkRuntimeError, OpenStoreError,
};
use swimos_form::read::RecognizerReadable;
use swimos_model::{Text, Timestamp};
use swimos_runtime::agent::reporting::UplinkReporter;
use swimos_utilities::byte_channel::{byte_channel, ByteReader, ByteWriter};
use swimos_utilities::{non_zero_usize, trigger};
use tokio_util::codec::{FramedRead, FramedWrite};
use uuid::Uuid;

const BUFFER_SIZE: NonZeroUsize = non_zero_usize!(128);

type ResponseDecoder<T> = MapLaneResponseDecoder<Text, T>;

struct MockAgentContext;

impl AgentContext for MockAgentContext {
    fn add_lane(
        &self,
        _name: &str,
        _lane_kind: WarpLaneKind,
        _config: LaneConfig,
    ) -> BoxFuture<'static, Result<(ByteWriter, ByteReader), AgentRuntimeError>> {
        panic!("Unexpected add lane invocation")
    }

    fn open_downlink(
        &self,
        _host: Option<&str>,
        _node: &str,
        _lane: &str,
        _kind: DownlinkKind,
    ) -> BoxFuture<'static, Result<(ByteWriter, ByteReader), DownlinkRuntimeError>> {
        panic!("Unexpected open downlink invocation")
    }

    fn add_store(
        &self,
        _name: &str,
        _kind: StoreKind,
    ) -> BoxFuture<'static, Result<(ByteWriter, ByteReader), OpenStoreError>> {
        panic!("Unexpected add store invocation")
    }

    fn command_channel(&self) -> BoxFuture<'static, Result<ByteWriter, DownlinkRuntimeError>> {
        panic!("Unexpected ad hoc commands invocation")
    }

    fn add_http_lane(
        &self,
        _name: &str,
    ) -> BoxFuture<'static, Result<HttpLaneRequestChannel, AgentRuntimeError>> {
        panic!("Unexpected add HTTP lane invocation")
    }
}

struct LaneChannel<D>
where
    D: RecognizerReadable,
{
    sender: FramedWrite<ByteWriter, RawValueLaneRequestEncoder>,
    receiver: FramedRead<ByteReader, ResponseDecoder<D>>,
}

impl<D> LaneChannel<D>
where
    D: RecognizerReadable + PartialEq + Debug,
{
    const ID: Uuid = Uuid::from_u128(13);

    fn new(sender: ByteWriter, receiver: ByteReader) -> LaneChannel<D> {
        LaneChannel {
            sender: FramedWrite::new(sender, Default::default()),
            receiver: FramedRead::new(receiver, MapLaneResponseDecoder::default()),
        }
    }

    async fn send_sync(&mut self) {
        let req: LaneRequest<&[u8]> = LaneRequest::Sync(LaneChannel::<D>::ID);
        assert!(self.sender.send(req).await.is_ok());
    }

    async fn recv(&mut self) -> LaneResponse<MapOperation<Text, D>> {
        self.receiver
            .next()
            .await
            .expect("Expected a response")
            .expect("Invalid frame")
    }

    async fn recv_synced(&mut self) {
        assert_eq!(
            self.recv().await,
            LaneResponse::Synced(LaneChannel::<D>::ID)
        );
    }

    async fn expect_n_sync_events(&mut self, n: usize) -> Vec<(Text, D)> {
        let mut events = Vec::with_capacity(n);
        for _ in 0..n {
            match self.recv().await {
                LaneResponse::SyncEvent(
                    LaneChannel::<D>::ID,
                    MapOperation::Update { key, value },
                ) => {
                    events.push((key, value));
                }
                resp => panic!("Expected an event. Received: {:?}", resp),
            }
        }
        events
    }
}

struct Context {
    shutdown_tx: trigger::Sender,
    forest: Arc<RwLock<UriForest<AgentMeta>>>,
    nodes_channel: LaneChannel<NodeInfoList>,
    nodes_count_channel: LaneChannel<NodeInfo>,
}

async fn run_test<F, Fut>(test: F) -> Fut::Output
where
    F: FnOnce(Context) -> Fut,
    Fut: Future,
{
    let _r = NOW.set(Timestamp::now());

    let (nodes_in_tx, nodes_in_rx) = byte_channel(BUFFER_SIZE);
    let (nodes_out_tx, nodes_out_rx) = byte_channel(BUFFER_SIZE);

    let (nodes_count_in_tx, nodes_count_in_rx) = byte_channel(BUFFER_SIZE);
    let (nodes_count_out_tx, nodes_count_out_rx) = byte_channel(BUFFER_SIZE);

    let forest = Arc::new(RwLock::new(UriForest::new()));
    let (shutdown_tx, shutdown_rx) = trigger::trigger();

    let task = run_task(
        shutdown_rx,
        forest.clone(),
        Box::new(MockAgentContext),
        (nodes_out_tx, nodes_in_rx),
        (nodes_count_out_tx, nodes_count_in_rx),
    );

    let context = Context {
        shutdown_tx,
        forest,
        nodes_channel: LaneChannel::new(nodes_in_tx, nodes_out_rx),
        nodes_count_channel: LaneChannel::new(nodes_count_in_tx, nodes_count_out_rx),
    };

    let (task_result, output) = join(task, test(context)).await;
    task_result.expect("Test failure");
    output
}

fn push_uri(forest: &mut UriForest<AgentMeta>, reporter: &UplinkReporter, path: &str, name: &str) {
    forest.insert(
        path,
        AgentMeta {
            name: name.into(),
            created: *NOW.get().unwrap(),
            updater: AgentIntrospectionUpdater::new(reporter.reader()),
        },
    );
}

static NOW: OnceLock<Timestamp> = OnceLock::new();

#[tokio::test]
async fn count_lanes_empty() {
    run_test(|ctx| async {
        let Context {
            shutdown_tx,
            mut nodes_count_channel,
            ..
        } = ctx;

        nodes_count_channel.send_sync().await;
        nodes_count_channel.recv_synced().await;
        assert!(shutdown_tx.trigger());
    })
    .await
}

#[tokio::test]
async fn count_lanes() {
    run_test(|ctx| async {
        let Context {
            shutdown_tx,
            forest,
            nodes_channel: _nodes_channel,
            mut nodes_count_channel,
        } = ctx;

        let reporter = UplinkReporter::default();

        {
            let forest = &mut *forest.write();
            push_uri(forest, &reporter, "/listener", "listener_agent");
            push_uri(forest, &reporter, "/cnt/1", "counter_1");
            push_uri(forest, &reporter, "/cnt/2", "counter_2");
            push_uri(forest, &reporter, "/cnt/3", "counter_3");
            push_uri(forest, &reporter, "/cnt/4", "counter_4");
        }
        nodes_count_channel.send_sync().await;

        let mut expected = vec![
            (
                "/listener".into(),
                NodeInfo::List(NodeInfoList {
                    node_uri: "/listener".to_string(),
                    created: NOW.get().unwrap().clone().millis(),
                    agents: vec!["listener_agent".into()],
                }),
            ),
            (
                "/cnt".into(),
                NodeInfo::Count(NodeInfoCount {
                    node_uri: "/cnt".to_string(),
                    created: 0,
                    child_count: 4,
                }),
            ),
        ];

        let mut events = nodes_count_channel
            .expect_n_sync_events(expected.len())
            .await;

        expected.sort();
        events.sort();

        assert_eq!(expected, events);

        nodes_count_channel.recv_synced().await;
        assert!(shutdown_tx.trigger());
    })
    .await
}

#[tokio::test]
async fn list_lanes_empty() {
    run_test(|ctx| async {
        let Context {
            shutdown_tx,
            mut nodes_channel,
            ..
        } = ctx;

        nodes_channel.send_sync().await;
        nodes_channel.recv_synced().await;
        assert!(shutdown_tx.trigger());
    })
    .await
}

#[tokio::test]
async fn list_lanes() {
    run_test(|ctx| async {
        let Context {
            shutdown_tx,
            mut nodes_channel,
            forest,
            ..
        } = ctx;

        let reporter = UplinkReporter::default();

        {
            let forest = &mut *forest.write();
            push_uri(forest, &reporter, "/listener", "listener_agent");
            push_uri(forest, &reporter, "/cnt/1", "counter_1");
            push_uri(forest, &reporter, "/cnt/2", "counter_2");
            push_uri(forest, &reporter, "/cnt/3", "counter_3");
            push_uri(forest, &reporter, "/cnt/4", "counter_4");
        }

        let mut expected = vec![
            (
                "/listener".into(),
                NodeInfoList {
                    node_uri: "/listener".to_string(),
                    created: NOW.get().unwrap().clone().millis(),
                    agents: vec!["listener_agent".into()],
                },
            ),
            (
                "/cnt/1".into(),
                NodeInfoList {
                    node_uri: "/cnt/1".to_string(),
                    created: NOW.get().unwrap().clone().millis(),
                    agents: vec!["counter_1".into()],
                },
            ),
            (
                "/cnt/2".into(),
                NodeInfoList {
                    node_uri: "/cnt/2".to_string(),
                    created: NOW.get().unwrap().clone().millis(),
                    agents: vec!["counter_2".into()],
                },
            ),
            (
                "/cnt/3".into(),
                NodeInfoList {
                    node_uri: "/cnt/3".to_string(),
                    created: NOW.get().unwrap().clone().millis(),
                    agents: vec!["counter_3".into()],
                },
            ),
            (
                "/cnt/4".into(),
                NodeInfoList {
                    node_uri: "/cnt/4".to_string(),
                    created: NOW.get().unwrap().clone().millis(),
                    agents: vec!["counter_4".into()],
                },
            ),
        ];

        nodes_channel.send_sync().await;

        let mut events = nodes_channel.expect_n_sync_events(expected.len()).await;

        expected.sort();
        events.sort();

        assert_eq!(expected, events);

        nodes_channel.recv_synced().await;
        assert!(shutdown_tx.trigger());
    })
    .await
}
