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

pub mod in_memory {
    use std::collections::{hash_map::Entry, HashMap};

    use crate::in_memory_store::InMemoryPlanePersistence;
    use parking_lot::Mutex;
    use swimos_api::persistence::ServerPersistence;

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
