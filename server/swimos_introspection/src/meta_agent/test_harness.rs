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

use std::{collections::HashMap, num::NonZeroUsize, sync::Arc};

use futures::{
    future::{join, BoxFuture},
    Future, FutureExt,
};
use parking_lot::Mutex;
use swimos_api::{
    agent::{
        Agent, AgentConfig, AgentContext, DownlinkKind, HttpLaneRequestChannel, LaneConfig,
        StoreKind, WarpLaneKind,
    },
    error::{AgentRuntimeError, CommanderRegistrationError, DownlinkRuntimeError, OpenStoreError},
};
use swimos_runtime::agent::UplinkReporterRegistration;
use swimos_utilities::{
    byte_channel::{byte_channel, ByteReader, ByteWriter},
    future::RetryStrategy,
    non_zero_usize,
    routing::RouteUri,
    trigger,
};
use tokio::sync::mpsc;

use crate::task::{IntrospectionMessage, IntrospectionResolver};

pub async fn introspection_agent_test<Fac, A, F, Fut>(
    lane_config: LaneConfig,
    lanes: Vec<(String, WarpLaneKind)>,
    route: RouteUri,
    route_params: HashMap<String, String>,
    agent_fac: Fac,
    test_case: F,
) -> Fut::Output
where
    A: Agent + Send + 'static,
    Fac: FnOnce(IntrospectionResolver) -> A,
    F: FnOnce(IntrospectionTestContext) -> Fut,
    Fut: Future,
{
    let (init_tx, init_rx) = trigger::trigger();

    let (queries_tx, queries_rx) = mpsc::unbounded_channel();
    let (reg_tx, reg_rx) = mpsc::channel(8);
    let resolver = IntrospectionResolver::new(queries_tx, reg_tx);

    let (fake_context, test_context) = init(lane_config, lanes, init_rx, queries_rx, reg_rx);
    let context: Box<dyn AgentContext + Send + 'static> = Box::new(fake_context);

    let config = AgentConfig {
        default_lane_config: Some(lane_config),
        keep_linked_retry: RetryStrategy::none(),
    };

    let agent_task = async move {
        let agent = agent_fac(resolver);
        let run_agent = agent
            .run(route, route_params, config, context)
            .await
            .expect("Init failed.");
        init_tx.trigger();
        run_agent.await.expect("Running agent failed.");
    };
    let test_task = test_case(test_context);
    let (_, result) = join(agent_task, test_task).await;
    result
}

struct FakeRuntimeLane {
    kind: WarpLaneKind,
    expected_config: LaneConfig,
    io: Option<(ByteWriter, ByteReader)>,
}

pub struct IntrospectionTestContext {
    pub lanes: HashMap<String, (ByteWriter, ByteReader)>,
    pub init_done: trigger::Receiver,
    pub queries_rx: mpsc::UnboundedReceiver<IntrospectionMessage>,
    pub _reg_rx: mpsc::Receiver<UplinkReporterRegistration>,
}

struct ContextInner {
    expected_lanes: HashMap<String, FakeRuntimeLane>,
}

const BUFFER_SIZE: NonZeroUsize = non_zero_usize!(4096);

pub fn init(
    lane_config: LaneConfig,
    lanes: Vec<(String, WarpLaneKind)>,
    init_done: trigger::Receiver,
    queries_rx: mpsc::UnboundedReceiver<IntrospectionMessage>,
    reg_rx: mpsc::Receiver<UplinkReporterRegistration>,
) -> (FakeContext, IntrospectionTestContext) {
    let mut expected = HashMap::new();
    let mut runtime_endpoints = HashMap::new();
    for (name, kind) in lanes {
        let (in_tx, in_rx) = byte_channel(BUFFER_SIZE);
        let (out_tx, out_rx) = byte_channel(BUFFER_SIZE);
        let fake_lane = FakeRuntimeLane {
            kind,
            expected_config: lane_config,
            io: Some((out_tx, in_rx)),
        };
        expected.insert(name.clone(), fake_lane);
        runtime_endpoints.insert(name, (in_tx, out_rx));
    }
    let fake_context = FakeContext {
        inner: Arc::new(Mutex::new(ContextInner {
            expected_lanes: expected,
        })),
    };
    let test_context = IntrospectionTestContext {
        lanes: runtime_endpoints,
        init_done,
        queries_rx,
        _reg_rx: reg_rx,
    };
    (fake_context, test_context)
}

pub struct FakeContext {
    inner: Arc<Mutex<ContextInner>>,
}

impl AgentContext for FakeContext {
    fn command_channel(&self) -> BoxFuture<'static, Result<ByteWriter, DownlinkRuntimeError>> {
        panic!("Unexpected request for ad hoc channel.");
    }

    fn register_command_endpoint(
        &self,
        _host: Option<&str>,
        _node: &str,
        _lane: &str,
        _id: u16,
    ) -> BoxFuture<'static, Result<(), CommanderRegistrationError>> {
        panic!("Unexpected command endpoint registration.");
    }

    fn add_lane(
        &self,
        name: &str,
        lane_kind: WarpLaneKind,
        config: LaneConfig,
    ) -> BoxFuture<'static, Result<(ByteWriter, ByteReader), AgentRuntimeError>> {
        let inner = self.inner.clone();
        let key = name.to_string();
        async move {
            let mut lock = inner.lock();
            let ContextInner { expected_lanes } = &mut *lock;
            let FakeRuntimeLane {
                kind,
                expected_config,
                io,
            } = expected_lanes.get_mut(&key).expect("Unknown lane.");
            assert_eq!(lane_kind, *kind);
            assert_eq!(config, *expected_config);
            Ok(io.take().expect("Lane registered twice."))
        }
        .boxed()
    }

    fn open_downlink(
        &self,
        _host: Option<&str>,
        _node: &str,
        _lane: &str,
        _kind: DownlinkKind,
    ) -> BoxFuture<'static, Result<(ByteWriter, ByteReader), DownlinkRuntimeError>> {
        panic!("Unexpected downlink request.");
    }

    fn add_store(
        &self,
        _name: &str,
        _kind: StoreKind,
    ) -> BoxFuture<'static, Result<(ByteWriter, ByteReader), OpenStoreError>> {
        panic!("Unexpected store request.");
    }

    fn add_http_lane(
        &self,
        _name: &str,
    ) -> BoxFuture<'static, Result<HttpLaneRequestChannel, AgentRuntimeError>> {
        panic!("Unexpected HTTP lane request.");
    }
}
