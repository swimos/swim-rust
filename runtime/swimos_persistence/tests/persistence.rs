// Copyright 2015-2023 Swim Inc.
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

use bytes::BytesMut;
use swimos_api::store::{NodePersistence, PlanePersistence};
use swimos_store::{RangeConsumer, StoreError};

const STORE_FAILURE: &str = "Creating store failed.";
const NODE_FAILURE: &str = "Failed to open node store.";
const ID_FAILURE: &str = "Failed to get lane id.";
const PUT_FAILURE: &str = "Store failed";
const GET_MAP_FAILURE: &str = "Retrieving map failed.";
const NODE: &str = "/node";
const VALUE_LANE: &str = "value_lane";
const MAP_LANE: &str = "value_lane";

const MAP_LANE1: &str = "map_lane1";
const MAP_LANE2: &str = "map_lane2";

struct TestSuite<F> {
    store_factory: F,
}

impl<F> TestSuite<F> {
    const fn new(store_factory: F) -> Self {
        TestSuite { store_factory }
    }
}

impl<F, S> TestSuite<F>
where
    F: Fn() -> Result<S, StoreError>,
    S: PlanePersistence + Send + Sync + 'static,
{
    async fn open_node_store(&self) {
        let TestSuite { store_factory } = self;

        let store = store_factory().expect(STORE_FAILURE);
        let node_store = store.node_store(NODE).await;
        assert!(node_store.is_ok());
    }

    async fn retrieve_undefined_value(&self) {
        let TestSuite { store_factory } = self;
        let store = store_factory().expect(STORE_FAILURE);
        let node_store = store.node_store("/node").await.expect(NODE_FAILURE);
        let id = node_store.id_for(VALUE_LANE).expect(ID_FAILURE);

        let mut buffer = BytesMut::new();

        assert!(matches!(node_store.get_value(id, &mut buffer), Ok(None)));
        assert!(buffer.is_empty());
    }

    async fn put_and_retrieve_value(&self) {
        let TestSuite { store_factory } = self;
        let store = store_factory().expect(STORE_FAILURE);
        let mut node_store = store.node_store("/node").await.expect(NODE_FAILURE);
        let id = node_store.id_for(VALUE_LANE).expect(ID_FAILURE);

        let mut buffer = BytesMut::new();

        node_store.put_value(id, &[1, 2, 3, 4]).expect(PUT_FAILURE);
        let result = node_store.get_value(id, &mut buffer);
        assert!(matches!(result, Ok(Some(4))));
        assert_eq!(buffer.as_ref(), &[1, 2, 3, 4]);
    }

    async fn replace_existing_value(&self) {
        let TestSuite { store_factory } = self;

        let store = store_factory().expect(STORE_FAILURE);
        let mut node_store = store.node_store("/node").await.expect(NODE_FAILURE);
        let id = node_store.id_for(VALUE_LANE).expect(ID_FAILURE);

        let mut buffer = BytesMut::new();

        node_store.put_value(id, &[1, 2, 3, 4]).expect(PUT_FAILURE);

        node_store.put_value(id, &[5, 6, 7]).expect(PUT_FAILURE);
        let result = node_store.get_value(id, &mut buffer);
        assert!(matches!(result, Ok(Some(3))));
        assert_eq!(buffer.as_ref(), &[5, 6, 7]);
    }

    async fn delete_undefined_value(&self) {
        let TestSuite { store_factory } = self;
        let store = store_factory().expect(STORE_FAILURE);
        let mut node_store = store.node_store("/node").await.expect(NODE_FAILURE);
        let id = node_store.id_for(VALUE_LANE).expect(ID_FAILURE);

        assert!(node_store.delete_value(id).is_ok());
    }

    async fn delete_existing_value(&self) {
        let TestSuite { store_factory } = self;
        let store = store_factory().expect(STORE_FAILURE);
        let mut node_store = store.node_store("/node").await.expect(NODE_FAILURE);
        let id = node_store.id_for(VALUE_LANE).expect(ID_FAILURE);

        let mut buffer = BytesMut::new();

        node_store.put_value(id, &[1, 2, 3, 4]).expect(PUT_FAILURE);
        assert!(node_store.delete_value(id).is_ok());

        let result = node_store.get_value(id, &mut buffer);
        assert!(matches!(result, Ok(None)));
        assert!(buffer.is_empty());
    }

    async fn retrieve_undefined_map(&self) {
        let TestSuite { store_factory } = self;
        let store = store_factory().expect(STORE_FAILURE);
        let node_store = store.node_store("/node").await.expect(NODE_FAILURE);
        let id = node_store.id_for(MAP_LANE).expect(ID_FAILURE);

        let mut consumer = node_store.read_map(id).expect(GET_MAP_FAILURE);
        assert!(matches!(consumer.consume_next(), Ok(None)));
    }

    async fn populate_map_and_retrieve(&self) {
        let TestSuite { store_factory } = self;
        let store = store_factory().expect(STORE_FAILURE);
        let mut node_store = store.node_store("/node").await.expect(NODE_FAILURE);
        let id = node_store.id_for(MAP_LANE).expect(ID_FAILURE);

        assert!(node_store.update_map(id, b"a", &[1, 2, 3]).is_ok());
        assert!(node_store.update_map(id, b"b", &[4, 5, 6]).is_ok());
        assert!(node_store.update_map(id, b"c", &[]).is_ok());

        let mut consumer = node_store.read_map(id).expect(GET_MAP_FAILURE);
        match consumer.consume_next() {
            Ok(Some((k, v))) => {
                assert_eq!(k, b"a");
                assert_eq!(v, &[1, 2, 3]);
            }
            _ => panic!("Expected entry."),
        }
        match consumer.consume_next() {
            Ok(Some((k, v))) => {
                assert_eq!(k, b"b");
                assert_eq!(v, &[4, 5, 6]);
            }
            _ => panic!("Expected entry."),
        }
        match consumer.consume_next() {
            Ok(Some((k, v))) => {
                assert_eq!(k, b"c");
                assert_eq!(v, &[]);
            }
            _ => panic!("Expected entry."),
        }
        assert!(matches!(consumer.consume_next(), Ok(None)));
    }

    async fn remove_from_map(&self) {
        let TestSuite { store_factory } = self;
        let store = store_factory().expect(STORE_FAILURE);
        let mut node_store = store.node_store("/node").await.expect(NODE_FAILURE);
        let id = node_store.id_for(MAP_LANE).expect(ID_FAILURE);

        assert!(node_store.update_map(id, b"a", &[1, 2, 3]).is_ok());
        assert!(node_store.update_map(id, b"b", &[4, 5, 6]).is_ok());
        assert!(node_store.update_map(id, b"c", &[]).is_ok());

        assert!(node_store.remove_map(id, b"b").is_ok());

        let mut consumer = node_store.read_map(id).expect(GET_MAP_FAILURE);
        match consumer.consume_next() {
            Ok(Some((k, v))) => {
                assert_eq!(k, b"a");
                assert_eq!(v, &[1, 2, 3]);
            }
            _ => panic!("Expected entry."),
        }
        match consumer.consume_next() {
            Ok(Some((k, v))) => {
                assert_eq!(k, b"c");
                assert_eq!(v, &[]);
            }
            _ => panic!("Expected entry."),
        }
        assert!(matches!(consumer.consume_next(), Ok(None)));
    }

    async fn clear_map(&self) {
        let TestSuite { store_factory } = self;
        let store = store_factory().expect(STORE_FAILURE);
        let mut node_store = store.node_store("/node").await.expect(NODE_FAILURE);
        let id = node_store.id_for(MAP_LANE).expect(ID_FAILURE);

        assert!(node_store.update_map(id, b"a", &[1, 2, 3]).is_ok());
        assert!(node_store.update_map(id, b"b", &[4, 5, 6]).is_ok());
        assert!(node_store.update_map(id, b"c", &[]).is_ok());

        assert!(node_store.clear_map(id).is_ok());

        let mut consumer = node_store.read_map(id).expect(GET_MAP_FAILURE);
        assert!(matches!(consumer.consume_next(), Ok(None)));
    }

    async fn populate_two_maps_and_retrieve(&self) {
        let TestSuite { store_factory } = self;
        let store = store_factory().expect(STORE_FAILURE);
        let mut node_store = store.node_store("/node").await.expect(NODE_FAILURE);
        let id1 = node_store.id_for(MAP_LANE1).expect(ID_FAILURE);
        let id2 = node_store.id_for(MAP_LANE2).expect(ID_FAILURE);

        assert!(node_store.update_map(id1, b"a", &[1, 2, 3]).is_ok());
        assert!(node_store.update_map(id2, b"b", &[4, 5, 6]).is_ok());
        assert!(node_store.update_map(id2, b"c", &[]).is_ok());

        let mut consumer1 = node_store.read_map(id1).expect(GET_MAP_FAILURE);
        let mut consumer2 = node_store.read_map(id2).expect(GET_MAP_FAILURE);
        match consumer1.consume_next() {
            Ok(Some((k, v))) => {
                assert_eq!(k, b"a");
                assert_eq!(v, &[1, 2, 3]);
            }
            _ => panic!("Expected entry."),
        }
        match consumer2.consume_next() {
            Ok(Some((k, v))) => {
                assert_eq!(k, b"b");
                assert_eq!(v, &[4, 5, 6]);
            }
            _ => panic!("Expected entry."),
        }
        assert!(matches!(consumer1.consume_next(), Ok(None)));
        match consumer2.consume_next() {
            Ok(Some((k, v))) => {
                assert_eq!(k, b"c");
                assert_eq!(v, &[]);
            }
            _ => panic!("Expected entry."),
        }
        assert!(matches!(consumer2.consume_next(), Ok(None)));
    }

    async fn populate_two_maps_and_clear(&self) {
        let TestSuite { store_factory } = self;

        let store = store_factory().expect(STORE_FAILURE);
        let mut node_store = store.node_store("/node").await.expect(NODE_FAILURE);
        let id1 = node_store.id_for(MAP_LANE1).expect(ID_FAILURE);
        let id2 = node_store.id_for(MAP_LANE2).expect(ID_FAILURE);

        assert!(node_store.update_map(id1, b"a", &[1, 2, 3]).is_ok());
        assert!(node_store.update_map(id2, b"b", &[4, 5, 6]).is_ok());
        assert!(node_store.update_map(id2, b"c", &[]).is_ok());

        assert!(node_store.clear_map(id1).is_ok());

        let mut consumer1 = node_store.read_map(id1).expect(GET_MAP_FAILURE);
        let mut consumer2 = node_store.read_map(id2).expect(GET_MAP_FAILURE);
        assert!(matches!(consumer1.consume_next(), Ok(None)));
        match consumer2.consume_next() {
            Ok(Some((k, v))) => {
                assert_eq!(k, b"b");
                assert_eq!(v, &[4, 5, 6]);
            }
            _ => panic!("Expected entry."),
        }
        match consumer2.consume_next() {
            Ok(Some((k, v))) => {
                assert_eq!(k, b"c");
                assert_eq!(v, &[]);
            }
            _ => panic!("Expected entry."),
        }
        assert!(matches!(consumer2.consume_next(), Ok(None)));
    }
}

#[cfg(feature = "rocks")]
mod rocks {

    const PLANE_NAME: &str = "plane";

    use super::*;
    use swimos_persistence::{
        agent::StoreWrapper,
        rocks::{default_db_opts, default_keyspaces},
        ServerStore, SwimStore,
    };

    fn create_rocks_store(
        name: &str,
    ) -> Result<impl PlanePersistence + Send + Sync + 'static, StoreError> {
        let keyspaces = default_keyspaces();
        let options = default_db_opts();

        let server_store = ServerStore::transient(options, keyspaces, "TEST_STORE")?;
        let store = server_store.plane_store(name)?;
        Ok(StoreWrapper(store))
    }

    #[tokio::test]
    async fn rocks_open_node_store() {
        let suite = TestSuite::new(|| create_rocks_store(PLANE_NAME));
        suite.open_node_store().await;
    }

    #[tokio::test]
    async fn rocks_retrieve_undefined_value() {
        let suite = TestSuite::new(|| create_rocks_store(PLANE_NAME));
        suite.retrieve_undefined_value().await;
    }

    #[tokio::test]
    async fn rocks_put_and_retrieve_value() {
        let suite = TestSuite::new(|| create_rocks_store(PLANE_NAME));
        suite.put_and_retrieve_value().await;
    }

    #[tokio::test]
    async fn rocks_replace_existing_value() {
        let suite = TestSuite::new(|| create_rocks_store(PLANE_NAME));
        suite.replace_existing_value().await;
    }

    #[tokio::test]
    async fn rocks_delete_undefined_value() {
        let suite = TestSuite::new(|| create_rocks_store(PLANE_NAME));
        suite.delete_undefined_value().await;
    }

    #[tokio::test]
    async fn rocks_delete_existing_value() {
        let suite = TestSuite::new(|| create_rocks_store(PLANE_NAME));
        suite.delete_existing_value().await;
    }

    #[tokio::test]
    async fn rocks_retrieve_undefined_map() {
        let suite = TestSuite::new(|| create_rocks_store(PLANE_NAME));
        suite.retrieve_undefined_map().await;
    }

    #[tokio::test]
    async fn rocks_remove_from_map() {
        let suite = TestSuite::new(|| create_rocks_store(PLANE_NAME));
        suite.remove_from_map().await;
    }

    #[tokio::test]
    async fn rocks_clear_map() {
        let suite = TestSuite::new(|| create_rocks_store(PLANE_NAME));
        suite.clear_map().await;
    }

    #[tokio::test]
    async fn rocks_populate_map_and_retrieve() {
        let suite = TestSuite::new(|| create_rocks_store(PLANE_NAME));
        suite.populate_map_and_retrieve().await;
    }

    #[tokio::test]
    async fn rocks_populate_two_maps_and_retrieve() {
        let suite = TestSuite::new(|| create_rocks_store(PLANE_NAME));
        suite.populate_two_maps_and_retrieve().await;
    }

    #[tokio::test]
    async fn rocks_populate_two_maps_and_clear() {
        let suite = TestSuite::new(|| create_rocks_store(PLANE_NAME));
        suite.populate_two_maps_and_clear().await;
    }
}

mod in_memory {
    use super::*;
    use swimos_persistence::agent::in_memory::InMemoryPlanePersistence;

    fn in_memory_factory() -> Result<impl PlanePersistence + Send + Sync + 'static, StoreError> {
        Ok(InMemoryPlanePersistence::default())
    }

    #[tokio::test]
    async fn in_memory_open_node_store() {
        let suite = TestSuite::new(in_memory_factory);
        suite.open_node_store().await;
    }

    #[tokio::test]
    async fn in_memory_retrieve_undefined_value() {
        let suite = TestSuite::new(in_memory_factory);
        suite.retrieve_undefined_value().await;
    }

    #[tokio::test]
    async fn in_memory_put_and_retrieve_value() {
        let suite = TestSuite::new(in_memory_factory);
        suite.put_and_retrieve_value().await;
    }

    #[tokio::test]
    async fn in_memory_replace_existing_value() {
        let suite = TestSuite::new(in_memory_factory);
        suite.replace_existing_value().await;
    }

    #[tokio::test]
    async fn in_memory_delete_undefined_value() {
        let suite = TestSuite::new(in_memory_factory);
        suite.delete_undefined_value().await;
    }

    #[tokio::test]
    async fn in_memory_delete_existing_value() {
        let suite = TestSuite::new(in_memory_factory);
        suite.delete_existing_value().await;
    }

    #[tokio::test]
    async fn in_memory_retrieve_undefined_map() {
        let suite = TestSuite::new(in_memory_factory);
        suite.retrieve_undefined_map().await;
    }

    #[tokio::test]
    async fn in_memory_remove_from_map() {
        let suite = TestSuite::new(in_memory_factory);
        suite.remove_from_map().await;
    }

    #[tokio::test]
    async fn in_memory_clear_map() {
        let suite = TestSuite::new(in_memory_factory);
        suite.clear_map().await;
    }

    #[tokio::test]
    async fn in_memory_populate_map_and_retrieve() {
        let suite = TestSuite::new(in_memory_factory);
        suite.populate_map_and_retrieve().await;
    }

    #[tokio::test]
    async fn in_memory_populate_two_maps_and_retrieve() {
        let suite = TestSuite::new(in_memory_factory);
        suite.populate_two_maps_and_retrieve().await;
    }

    #[tokio::test]
    async fn in_memory_populate_two_maps_and_clear() {
        let suite = TestSuite::new(in_memory_factory);
        suite.populate_two_maps_and_clear().await;
    }
}
