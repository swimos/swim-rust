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

use crate::agent;
use crate::agent::context::AgentExecutionContext;
use crate::agent::lane::channels::AgentExecutionConfig;
use crate::agent::lane::lifecycle::StatefulLaneLifecycle;
use crate::agent::lane::model::map::MapLaneEvent;
use crate::agent::lane::tests::ExactlyOnce;
use crate::agent::lifecycle::AgentLifecycle;
use crate::agent::model::map::MapLane;
use crate::agent::model::value::{ValueLane, ValueLaneEvent};
use crate::agent::tests::stub_router::SingleChannelRouter;
use crate::agent::{
    AgentContext, AgentParameters, DynamicAgentIo, DynamicLaneTasks, LaneConfig, LaneParts,
    SwimAgent, TestClock,
};
use crate::agent::{IoPair, LaneTasks};
use crate::interface::ServerDownlinksConfig;
use crate::plane::provider::AgentProvider;
use crate::routing::TopLevelServerRouterFactory;
use futures::future::ready;
use futures::future::{BoxFuture, Ready};
use server_store::agent::NodeStore;
use server_store::engine::rocks::RocksOpts;
use server_store::keystore::{KeyStore, COUNTER_BYTES};
use server_store::plane::{PlaneStore, SwimPlaneStore};
use server_store::rocks::{default_keyspaces, RocksDatabase};
use server_store::{KeyspaceName, StoreEngine, StoreKey};
use std::collections::HashMap;
use std::sync::Arc;
use stm::stm::Stm;
use stm::transaction::atomically;
use swim_client::configuration::DownlinkConnectionsConfig;
use swim_client::connections::SwimConnPool;
use swim_client::downlink::Downlinks;
use swim_client::interface::ClientContext;
use swim_client::router::ClientRouterFactory;
use swim_common::routing::RoutingAddr;
use swim_common::warp::path::Path;
use swim_store::{deserialize, serialize, KeyspaceByteEngine, StoreBuilder, StoreError};
use swim_utilities::algebra::non_zero_usize;
use swim_utilities::io::fs::Dir;
use swim_utilities::routing::uri::RelativeUri;
use swim_utilities::trigger::promise;
use tokio::sync::mpsc;
use tokio::time::Duration;
use tokio_stream::wrappers::ReceiverStream;

const INTERVAL: Duration = Duration::from_millis(1);
const MAX_PERIODS: i32 = 10;

macro_rules! ser {
    ($($it:tt)*) => {
        serialize(&$($it)*).unwrap().as_slice()
    };
}

#[derive(Debug)]
pub struct StoreAgent {
    value: ValueLane<i32>,
    map: MapLane<String, i32>,
}

#[derive(Debug, Clone)]
struct StoreAgentLifecycle;

impl AgentLifecycle<StoreAgent> for StoreAgentLifecycle {
    fn starting<'a, C>(&'a self, context: &'a C) -> BoxFuture<'a, ()>
    where
        C: AgentContext<StoreAgent> + Send + Sync + 'a,
    {
        Box::pin(async move {
            let value_lane = context.agent().value.clone();

            context
                .periodically(
                    move || {
                        let value_lane = value_lane.clone();

                        Box::pin(async move {
                            let value = value_lane.get().and_then(move |n| value_lane.set(*n + 1));
                            atomically(&value, ExactlyOnce)
                                .await
                                .expect("Failed to increment value");
                        })
                    },
                    INTERVAL,
                    Some(MAX_PERIODS as usize),
                )
                .await;
        })
    }
}

struct MapLifecycle;

impl<'a> StatefulLaneLifecycle<'a, MapLane<String, i32>, StoreAgent> for MapLifecycle {
    type StartFuture = Ready<()>;
    type EventFuture = Ready<()>;

    fn on_start<C>(&'a self, _model: &'a MapLane<String, i32>, _context: &'a C) -> Self::StartFuture
    where
        C: AgentContext<StoreAgent> + Send + Sync + 'a,
    {
        ready(())
    }

    fn on_event<C>(
        &'a mut self,
        _event: &'a MapLaneEvent<String, i32>,
        _model: &'a MapLane<String, i32>,
        _context: &'a C,
    ) -> Self::EventFuture
    where
        C: AgentContext<StoreAgent> + Send + Sync + 'static,
    {
        ready(())
    }
}

struct ValueLifecycle;

impl<'a> StatefulLaneLifecycle<'a, ValueLane<i32>, StoreAgent> for ValueLifecycle {
    type StartFuture = Ready<()>;
    type EventFuture = BoxFuture<'a, ()>;

    fn on_start<C>(&'a self, _model: &'a ValueLane<i32>, _context: &'a C) -> Self::StartFuture
    where
        C: AgentContext<StoreAgent> + Send + Sync + 'a,
    {
        ready(())
    }

    fn on_event<C>(
        &'a mut self,
        event: &'a ValueLaneEvent<i32>,
        _model: &'a ValueLane<i32>,
        context: &'a C,
    ) -> Self::EventFuture
    where
        C: AgentContext<StoreAgent> + Send + Sync + 'static,
    {
        Box::pin(async move {
            let map_lane = &context.agent().map;
            let stm = map_lane.update(event.current.to_string(), event.current.clone());

            if let Err(e) = atomically(&stm, ExactlyOnce).await {
                panic!("{:?}", e)
            }
        })
    }
}

impl SwimAgent<AgentConfig> for StoreAgent {
    fn instantiate<Context, Store>(
        _config: &AgentConfig,
        exec_conf: &AgentExecutionConfig,
        store: Store,
    ) -> (
        Self,
        DynamicLaneTasks<Self, Context>,
        DynamicAgentIo<Context>,
    )
    where
        Context: AgentContext<Self> + AgentExecutionContext + Send + Sync + 'static,
        Store: NodeStore,
    {
        let LaneParts {
            lane: value,
            tasks: value_task,
            io: value_lane_io,
        } = agent::make_value_lane(
            LaneConfig::new("value".to_string(), false, false),
            exec_conf,
            Default::default(),
            ValueLifecycle,
            |agent: &StoreAgent| &agent.value,
            store.clone(),
        );

        let LaneParts {
            lane: map,
            tasks: map_task,
            io: map_lane_io,
        } = agent::make_map_lane(
            "map",
            false,
            exec_conf,
            MapLifecycle,
            |agent: &StoreAgent| &agent.map,
            false,
            store,
        );

        let agent = StoreAgent { value, map };
        let mut io = HashMap::new();
        io.insert(
            "value".to_string(),
            IoPair {
                routing: None,
                persistence: value_lane_io.persistence,
            },
        );
        io.insert(
            "map".to_string(),
            IoPair {
                routing: None,
                persistence: map_lane_io.persistence,
            },
        );

        (agent, vec![value_task.boxed(), map_task.boxed()], io)
    }
}

#[derive(Clone, Debug)]
struct AgentConfig;

struct TestStore {
    _dir: Dir,
    delegate: SwimPlaneStore<RocksDatabase>,
    inner: Arc<RocksDatabase>,
}

impl Default for TestStore {
    fn default() -> Self {
        let temp_dir = Dir::transient("store_agent").unwrap();
        let db = RocksOpts::default()
            .build(temp_dir.path(), &default_keyspaces())
            .unwrap();
        let arcd_db = Arc::new(db);
        let store = SwimPlaneStore::new(
            "test",
            arcd_db.clone(),
            KeyStore::initialise_with(arcd_db.clone()),
        );

        TestStore {
            _dir: temp_dir,
            delegate: store,
            inner: arcd_db,
        }
    }
}

fn make_dl_context() -> ClientContext<Path> {
    let (client_tx, client_rx) = mpsc::channel(8);
    let (remote_tx, _remote_rx) = mpsc::channel(8);
    let (plane_tx, _plane_rx) = mpsc::channel(8);
    let (_close_tx, close_rx) = promise::promise();

    let top_level_factory =
        TopLevelServerRouterFactory::new(plane_tx, client_tx.clone(), remote_tx);

    let client_router_fac = ClientRouterFactory::new(client_tx.clone(), top_level_factory);

    let (conn_pool, _pool_task) = SwimConnPool::new(
        DownlinkConnectionsConfig::default(),
        (client_tx, client_rx),
        client_router_fac,
        close_rx.clone(),
    );

    let (downlinks, _downlinks_task) = Downlinks::new(
        non_zero_usize!(8),
        conn_pool,
        Arc::new(ServerDownlinksConfig::default()),
        close_rx,
    );

    ClientContext::new(downlinks)
}

#[tokio::test]
async fn store_loads() {
    let node_uri = "/test";

    let store = TestStore::default();
    let value_id = store
        .delegate
        .node_id_of(format!("{}/value", node_uri))
        .unwrap();
    let key = StoreKey::Value { lane_id: value_id };

    store.delegate.put(key, ser!(i32::MAX)).unwrap();

    let map_id = store
        .delegate
        .node_id_of(format!("{}/map", node_uri))
        .unwrap();

    let mut expected = HashMap::new();
    for i in 0..10 {
        expected.insert(i.to_string(), Arc::new(i));
        let key = serialize(&i.to_string()).unwrap();
        let key = StoreKey::Map {
            lane_id: map_id,
            key: Some(key),
        };

        store.delegate.put(key, ser!(i)).unwrap();
    }

    let provider = AgentProvider::new(AgentConfig, StoreAgentLifecycle);
    let uri: RelativeUri = node_uri.parse().unwrap();
    let buffer_size = non_zero_usize!(10);
    let clock = TestClock::default();

    let exec_config = AgentExecutionConfig::with(
        buffer_size,
        1,
        0,
        Duration::from_secs(1),
        None,
        Duration::from_secs(1),
    );
    let (envelope_tx, envelope_rx) = mpsc::channel(buffer_size.get());
    let (agent, agent_proc) = provider.run(
        AgentParameters::new(AgentConfig, exec_config, uri.clone(), HashMap::new()),
        clock.clone(),
        make_dl_context(),
        ReceiverStream::new(envelope_rx),
        SingleChannelRouter::new(RoutingAddr::plane(1024)),
        store.delegate.node_store(uri.to_string()),
    );

    let agent_task = swim_runtime::task::spawn(agent_proc);

    match agent.downcast_ref::<StoreAgent>() {
        Some(agent) => {
            let value = agent.value.load().await;
            assert_eq!(*value, i32::MAX);

            let map = atomically(&agent.map.snapshot(), ExactlyOnce)
                .await
                .unwrap();

            assert_eq!(expected, map);
        }
        None => {
            panic!("Failed to get store agent");
        }
    }

    drop(envelope_tx);
    assert!(agent_task.await.is_ok());
}

#[tokio::test]
async fn events() {
    let provider = AgentProvider::new(AgentConfig, StoreAgentLifecycle);
    let uri: RelativeUri = "/test".parse().unwrap();
    let buffer_size = non_zero_usize!(10);
    let clock = TestClock::default();

    let store = TestStore::default();

    let exec_config = AgentExecutionConfig::with(
        buffer_size,
        1,
        0,
        Duration::from_secs(1),
        None,
        Duration::from_secs(1),
    );
    let (envelope_tx, envelope_rx) = mpsc::channel(buffer_size.get());
    let (_, agent_proc) = provider.run(
        AgentParameters::new(AgentConfig, exec_config, uri.clone(), HashMap::new()),
        clock.clone(),
        make_dl_context(),
        ReceiverStream::new(envelope_rx),
        SingleChannelRouter::new(RoutingAddr::plane(1024)),
        store.delegate.node_store(uri.to_string()),
    );

    let agent_task = swim_runtime::task::spawn(agent_proc);

    fn check_value_lane(store: &TestStore, i: i32) {
        let delegate = &store.delegate;

        let value_uri = "/test/value";
        let value_id = delegate.node_id_of(value_uri).unwrap();
        let value_key = StoreKey::Value { lane_id: value_id };

        let value = delegate
            .get(value_key)
            .unwrap()
            .expect("Missing value for value lane");
        let deserialized = deserialize::<i32>(value.as_slice()).unwrap();
        assert_eq!(i, deserialized);
    }

    fn check_map_lane(store: &TestStore, i: i32) {
        let delegate = &store.delegate;

        let map_uri = "/test/map";
        let map_id = delegate.node_id_of(map_uri).unwrap();
        let prefix = StoreKey::Map {
            lane_id: map_id,
            key: None,
        };

        let snapshot_opt = delegate
            .get_prefix_range(prefix, |key, value| {
                let store_key = deserialize::<StoreKey>(&key)?;

                match store_key {
                    StoreKey::Map { key, .. } => {
                        let key = deserialize::<String>(&key.ok_or(StoreError::KeyNotFound)?)?;
                        let value = deserialize::<i32>(&value)?;

                        Ok((key, value))
                    }
                    StoreKey::Value { .. } => {
                        Err(StoreError::Decoding("Expected a map key".to_string()))
                    }
                }
            })
            .expect("Snapshot failure");

        match snapshot_opt {
            Some(snapshot) => {
                let actual: HashMap<String, i32> = snapshot.into_iter().collect();
                let expected = (1..=i).into_iter().fold(HashMap::new(), |mut map, i| {
                    map.insert(i.to_string(), i);
                    map
                });

                assert_eq!(expected, actual);
            }
            None => {
                panic!("Expected at least one entry in the snapshot")
            }
        }
    }

    clock.advance_when_blocked(INTERVAL).await;

    for i in 1..MAX_PERIODS {
        clock.advance_when_blocked(INTERVAL).await;
        check_value_lane(&store, i);
        check_map_lane(&store, i);
    }

    let counter = store
        .inner
        .get_keyspace(KeyspaceName::Lane, COUNTER_BYTES)
        .unwrap()
        .expect("Missing counter");
    let counter_value = deserialize::<u64>(counter.as_slice()).unwrap();
    assert_eq!(counter_value, 2);

    drop(envelope_tx);
    assert!(agent_task.await.is_ok());
}
