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
use crate::agent::store::NodeStore;
use crate::agent::tests::stub_router::SingleChannelRouter;
use crate::agent::LaneTasks;
use crate::agent::{
    AgentContext, DynamicAgentIo, DynamicLaneTasks, LaneConfig, SwimAgent, TestClock,
};
use crate::plane::provider::AgentProvider;
use crate::plane::store::{PlaneStore, SwimPlaneStore};
use crate::plane::RouteAndParameters;
use crate::routing::RoutingAddr;
use crate::store::keystore::{KeyStore, COUNTER_BYTES};
use crate::store::{default_keyspaces, KeyspaceName, RocksDatabase, StoreEngine, StoreKey};
use futures::future::ready;
use futures::future::{BoxFuture, Ready};
use futures::FutureExt;
use futures::Stream;
use std::collections::HashMap;
use std::num::NonZeroUsize;
use std::sync::Arc;
use stm::stm::Stm;
use stm::transaction::atomically;
use store::engines::{FromKeyspaces, RocksOpts};
use store::keyspaces::{KeyspaceByteEngine, KeyspaceRangedSnapshotLoad};
use store::{deserialize, serialize, StoreError};
use store::keyspaces::KeyspaceByteEngine;
use store::{EngineInfo, StoreError};
use swim_common::model::text::Text;
use tokio::sync::mpsc;
use tokio::time::Duration;
use tokio_stream::wrappers::ReceiverStream;
use utilities::fs::Dir;
use utilities::uri::RelativeUri;

const INTERVAL: Duration = Duration::from_millis(1);
const MAX_PERIODS: usize = 10;

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
                    Some(MAX_PERIODS),
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
        let (value, value_task, value_lane_io) = agent::make_value_lane(
            LaneConfig::new("value".to_string(), false, false),
            exec_conf,
            Default::default(),
            ValueLifecycle,
            |agent: &StoreAgent| &agent.value,
            store.clone(),
        );

        let (map, map_task, map_lane_io) = agent::make_map_lane(
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
            LaneIo {
                routing: None,
                persistence: value_lane_io.persistence,
            },
        );
        io.insert(
            "map".to_string(),
            LaneIo {
                routing: None,
                persistence: map_lane_io.persistence,
            },
        );

impl KeystoreTask for PlaneEventStore {
    fn run<DB, S>(_db: Arc<DB>, _events: S) -> BoxFuture<'static, Result<(), StoreError>>
    where
        DB: KeyspaceByteEngine,
        S: Stream<Item = KeyRequest> + Unpin + Send + 'static,
    {
        Box::pin(async { Ok(()) })
    }
}

impl PlaneStore for PlaneEventStore {
    type NodeStore = SwimNodeStore<Self>;

    fn node_store<I>(&self, node_uri: I) -> Self::NodeStore
    where
        I: Into<Text>,
    {
        SwimNodeStore::new(self.clone(), node_uri)
    }

    fn get_prefix_range<F, K, V>(
        &self,
        _prefix: StoreKey,
        _map_fn: F,
    ) -> Result<Option<Vec<(K, V)>>, StoreError>
    where
        F: for<'i> Fn(&'i [u8], &'i [u8]) -> Result<(K, V), StoreError>,
    {
        panic!("Unexpected snapshot attempt");
    }

    fn engine_info(&self) -> EngineInfo {
        EngineInfo {
            path: "Mock".to_string(),
            kind: "Mock".to_string(),
        }
    }

    fn lane_id_of<I>(&self, _lane: I) -> BoxFuture<u64>
    where
        I: Into<String>,
    {
        ready(1).boxed()
    }
}

impl StoreEngine for PlaneEventStore {
    fn put(&self, _key: StoreKey, value: &[u8]) -> Result<(), StoreError> {
        let mut guard = self.events.lock().unwrap();
        let events = &mut *guard;
        events.push(value.to_vec());
        Ok(())
    }

    fn get(&self, _key: StoreKey) -> Result<Option<Vec<u8>>, StoreError> {
        let mut lock = self.loaded.lock().unwrap();
        if let Some(trigger) = lock.take() {
            trigger.trigger();
        }

        match &self.default_value {
            Some(default) => {
                let value = bincode::serialize(default).expect("Failed to serialize default value");
                Ok(Some(value))
            }
            None => Ok(None),
        }
    }

    fn delete(&self, _key: StoreKey) -> Result<(), StoreError> {
        Ok(())
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
        let db = RocksDatabase::from_keyspaces(
            temp_dir.path(),
            &RocksOpts::default(),
            default_keyspaces(),
        )
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
    let buffer_size = NonZeroUsize::new(10).unwrap();
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
        RouteAndParameters::new(uri.clone(), HashMap::new()),
        exec_config,
        clock.clone(),
        ReceiverStream::new(envelope_rx),
        SingleChannelRouter::new(RoutingAddr::local(1024)),
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
    let buffer_size = NonZeroUsize::new(10).unwrap();
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
        RouteAndParameters::new(uri.clone(), HashMap::new()),
        exec_config,
        clock.clone(),
        ReceiverStream::new(envelope_rx),
        SingleChannelRouter::new(RoutingAddr::local(1024)),
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
            .keyspace_load_ranged_snapshot(&KeyspaceName::Map, ser!(prefix), |key, value| {
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
        check_value_lane(&store, i as i32);
        check_map_lane(&store, i as i32);
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
