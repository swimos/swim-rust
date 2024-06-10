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

use swimos_api::{
    error::StoreError,
    persistence::{PlanePersistence, StoreDisabled},
};

pub trait ServerPersistence {
    type PlaneStore: PlanePersistence + Clone + Send + Sync + 'static;

    fn open_plane(&self, name: &str) -> Result<Self::PlaneStore, StoreError>;
}

impl ServerPersistence for StoreDisabled {
    type PlaneStore = StoreDisabled;

    fn open_plane(&self, _name: &str) -> Result<Self::PlaneStore, StoreError> {
        Ok(Self)
    }
}

pub mod in_memory {
    use std::collections::{hash_map::Entry, HashMap};

    use parking_lot::Mutex;
    use swimos_store::in_memory::InMemoryPlanePersistence;

    use super::ServerPersistence;

    #[derive(Debug, Default)]
    /// A store implementation that mains agent state transiently in memory. State will persist across
    /// an agent restarting but will be lost if the process stops.
    pub struct InMemoryPersistence {
        planes: Mutex<HashMap<String, InMemoryPlanePersistence>>,
    }

    impl ServerPersistence for InMemoryPersistence {
        type PlaneStore = InMemoryPlanePersistence;

        fn open_plane(
            &self,
            name: &str,
        ) -> Result<Self::PlaneStore, swimos_api::error::StoreError> {
            let mut guard = self.planes.lock();
            Ok(match guard.entry(name.to_string()) {
                Entry::Occupied(entry) => entry.into_mut(),
                Entry::Vacant(entry) => entry.insert(InMemoryPlanePersistence::default()),
            }
            .clone())
        }
    }
}

#[cfg(feature = "rocks_store")]
pub mod rocks {
    use std::path::PathBuf;

    use swimos_api::error::StoreError;
    use swimos_rocks_store::{
        default_keyspaces, RocksEngine, RocksOpts, ServerStore, StoreWrapper, SwimPlaneStore,
        SwimStore,
    };

    use super::ServerPersistence;

    struct RocksServerPersistence {
        inner: ServerStore<RocksOpts>,
    }

    const PREFIX: &str = "swimos_store";

    impl ServerPersistence for RocksServerPersistence {
        type PlaneStore = StoreWrapper<SwimPlaneStore<RocksEngine>>;

        fn open_plane(&self, name: &str) -> Result<Self::PlaneStore, StoreError> {
            let RocksServerPersistence { inner } = self;
            let store = inner.plane_store(name)?;
            Ok(StoreWrapper(store))
        }
    }

    pub fn create_rocks_store(
        path: Option<PathBuf>,
        options: RocksOpts,
    ) -> Result<impl ServerPersistence + Send + Sync + 'static, StoreError> {
        let keyspaces = default_keyspaces();

        let server_store = match path {
            Some(base_path) => ServerStore::new(options, keyspaces, base_path),
            _ => ServerStore::transient(options, keyspaces, PREFIX),
        }?;
        Ok(RocksServerPersistence {
            inner: server_store,
        })
    }
}
