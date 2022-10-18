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

use swim_api::{
    error::StoreError,
    store::{PlanePersistence, StoreDisabled},
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

#[cfg(feature = "persistence")]
pub mod rocks {
    use std::path::PathBuf;

    use swim_api::error::StoreError;
    use swim_persistence::{
        agent::StoreWrapper,
        plane::SwimPlaneStore,
        rocks::{default_keyspaces, RocksDatabase, RocksOpts},
        ServerStore, SwimStore,
    };

    use super::ServerPersistence;

    struct RocksServerPersistence {
        inner: ServerStore<RocksOpts>,
    }

    const PREFIX: &str = "swim_store";

    impl ServerPersistence for RocksServerPersistence {
        type PlaneStore = StoreWrapper<SwimPlaneStore<RocksDatabase>>;

        fn open_plane(&self, name: &str) -> Result<Self::PlaneStore, StoreError> {
            let RocksServerPersistence { inner } = self;
            let store = inner.plane_store(name)?;
            Ok(StoreWrapper(store))
        }
    }

    pub fn create_rocks_store(
        path: Option<PathBuf>,
        options: crate::RocksOpts,
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
