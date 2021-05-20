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
use crate::agent::lane::tests::ExactlyOnce;
use crate::agent::lifecycle::AgentLifecycle;
use crate::agent::model::value::{ValueLane, ValueLaneEvent};
use crate::agent::store::{NodeStore, SwimNodeStore};
use crate::agent::tests::stub_router::SingleChannelRouter;
use crate::agent::{
    AgentContext, DynamicAgentIo, DynamicLaneTasks, LaneConfig, SwimAgent, TestClock,
};
use crate::agent::{LaneIo, LaneTasks};
use crate::plane::provider::AgentProvider;
use crate::plane::store::PlaneStore;
use crate::plane::RouteAndParameters;
use crate::routing::RoutingAddr;
use crate::store::{StoreEngine, StoreKey};
use futures::future::ready;
use futures::future::{BoxFuture, Ready};
use std::collections::HashMap;
use std::num::NonZeroUsize;
use std::sync::{Arc, Mutex};
use stm::stm::Stm;
use stm::transaction::atomically;
use store::engines::KeyedSnapshot;
use store::keyspaces::{KeyType, Keyspace, KeyspaceRangedSnapshotLoad, KeyspaceResolver};
use store::{StoreError, StoreInfo};
use swim_common::model::text::Text;
use tokio::sync::mpsc;
use tokio::time::Duration;
use tokio_stream::wrappers::ReceiverStream;
use utilities::uri::RelativeUri;

#[derive(Debug)]
pub struct StoreAgent {
    value: ValueLane<i32>,
}

#[derive(Debug, Clone)]
struct StoreAgentLifecycle {
    count: Option<usize>,
}

impl AgentLifecycle<StoreAgent> for StoreAgentLifecycle {
    fn starting<'a, C>(&'a self, context: &'a C) -> BoxFuture<'a, ()>
    where
        C: AgentContext<StoreAgent> + Send + Sync + 'a,
    {
        let StoreAgentLifecycle { count } = self;

        Box::pin(async move {
            let value_lane = context.agent().value.clone();

            if let Some(count) = count {
                context
                    .periodically(
                        move || {
                            let value_lane = value_lane.clone();

                            Box::pin(async move {
                                let value =
                                    value_lane.get().and_then(move |n| value_lane.set(*n + 1));
                                atomically(&value, ExactlyOnce)
                                    .await
                                    .expect("Failed to increment value");
                            })
                        },
                        Duration::from_millis(100),
                        Some(*count),
                    )
                    .await;
            }
        })
    }
}

struct ValueLifecycle {
    tx: mpsc::Sender<i32>,
}
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
        _context: &'a C,
    ) -> Self::EventFuture
    where
        C: AgentContext<StoreAgent> + Send + Sync + 'static,
    {
        Box::pin(async move {
            assert!(self.tx.send(*event.current).await.is_ok());
        })
    }
}

impl SwimAgent<AgentConfig> for StoreAgent {
    fn instantiate<Context, Store>(
        config: &AgentConfig,
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
        let AgentConfig { init, tx } = config;

        let (value, task, lane_io) = agent::make_value_lane(
            LaneConfig::new("value".to_string(), false, false),
            exec_conf,
            *init,
            ValueLifecycle { tx: tx.clone() },
            |agent: &StoreAgent| &agent.value,
            store,
        );

        let agent = StoreAgent { value };
        let mut io = HashMap::new();
        io.insert(
            "value".to_string(),
            LaneIo {
                routing: None,
                persistence: lane_io.persistence,
            },
        );

        (agent, vec![task.boxed()], io)
    }
}

#[derive(Debug, Clone)]
struct PlaneEventStore {
    default_value: Option<i32>,
    events: Arc<Mutex<Vec<Vec<u8>>>>,
}

impl KeyspaceResolver for PlaneEventStore {
    type ResolvedKeyspace = ();

    fn resolve_keyspace<K: Keyspace>(&self, _space: &K) -> Option<&Self::ResolvedKeyspace> {
        None
    }
}

impl KeyspaceRangedSnapshotLoad for PlaneEventStore {
    fn keyspace_load_ranged_snapshot<F, K, V, S>(
        &self,
        _keyspace: &S,
        _prefix: &[u8],
        _map_fn: F,
    ) -> Result<Option<KeyedSnapshot<K, V>>, StoreError>
    where
        F: for<'i> Fn(&'i [u8], &'i [u8]) -> Result<(K, V), StoreError>,
        S: Keyspace,
    {
        Ok(None)
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

    fn store_info(&self) -> StoreInfo {
        StoreInfo {
            path: "Mock".to_string(),
            kind: "Mock".to_string(),
        }
    }

    fn node_id_of<I>(&self, _lane: I) -> Result<KeyType, StoreError>
    where
        I: Into<String>,
    {
        Ok(0)
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
struct AgentConfig {
    init: i32,
    tx: mpsc::Sender<i32>,
}

#[tokio::test]
async fn events() {
    let default_value = 0;
    let event_count = 10;

    let (tx, mut rx) = mpsc::channel(5);
    let config = AgentConfig {
        init: default_value,
        tx,
    };

    let store_lifecycle = StoreAgentLifecycle {
        count: Some(event_count),
    };
    let provider = AgentProvider::new(config, store_lifecycle);
    let uri: RelativeUri = "/test".parse().unwrap();
    let buffer_size = NonZeroUsize::new(10).unwrap();
    let clock = TestClock::default();
    let events = Arc::new(Mutex::new(vec![]));

    let plane_store = PlaneEventStore {
        default_value: None,
        events: events.clone(),
    };

    let exec_config = AgentExecutionConfig::with(buffer_size, 1, 0, Duration::from_secs(1), None);
    let (envelope_tx, envelope_rx) = mpsc::channel(buffer_size.get());
    let (_, agent_proc) = provider.run(
        RouteAndParameters::new(uri.clone(), HashMap::new()),
        exec_config,
        clock.clone(),
        ReceiverStream::new(envelope_rx),
        SingleChannelRouter::new(RoutingAddr::local(1024)),
        plane_store.node_store(uri.to_string()),
    );

    let agent_task = swim_runtime::task::spawn(agent_proc);

    async fn expect(rx: &mut mpsc::Receiver<i32>, expected: i32, mutex: &Arc<Mutex<Vec<Vec<u8>>>>) {
        let result = rx.recv().await;
        assert!(result.is_some());
        let event = result.unwrap();
        assert_eq!(event, expected);

        let mut guard = mutex.lock().unwrap();
        let events = &mut *guard;
        assert_eq!(events.len(), 1);

        let event = events.pop().unwrap();
        let value = bincode::deserialize::<i32>(&event).expect("Failed to deserialize event");

        assert_eq!(value, expected);
    }

    for i in 0..event_count {
        clock.advance_when_blocked(Duration::from_millis(100)).await;
        expect(&mut rx, (i + 1) as i32, &events).await;
    }

    drop(envelope_tx);
    assert!(agent_task.await.is_ok());
}

#[tokio::test]
async fn loads_store_default() {
    let store_lifecycle = StoreAgentLifecycle { count: None };
    let (tx, _rx) = mpsc::channel(8);

    let config = AgentConfig { init: 13, tx };
    let provider = AgentProvider::new(config, store_lifecycle);
    let uri: RelativeUri = "/test".parse().unwrap();
    let buffer_size = NonZeroUsize::new(10).unwrap();
    let clock = TestClock::default();
    let events = Arc::new(Mutex::new(vec![]));

    let plane_store = PlaneEventStore {
        default_value: Some(i32::MAX),
        events: events.clone(),
    };

    let exec_config = AgentExecutionConfig::with(buffer_size, 1, 0, Duration::from_secs(1), None);
    let (envelope_tx, envelope_rx) = mpsc::channel(buffer_size.get());
    let (agent, agent_proc) = provider.run(
        RouteAndParameters::new(uri.clone(), HashMap::new()),
        exec_config,
        clock.clone(),
        ReceiverStream::new(envelope_rx),
        SingleChannelRouter::new(RoutingAddr::local(1024)),
        plane_store.node_store(uri.to_string()),
    );

    let agent_task = swim_runtime::task::spawn(agent_proc);

    clock.advance(Duration::from_millis(100)).await;

    match agent.downcast_ref::<StoreAgent>() {
        Some(agent) => {
            let value = agent.value.load().await;
            assert_eq!(*value, i32::MAX);
        }
        None => {
            panic!("Failed to get store agent");
        }
    }

    drop(envelope_tx);
    assert!(agent_task.await.is_ok());
}
