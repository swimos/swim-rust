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

use std::collections::HashMap;

use swim_model::Text;
use tracing::debug;

/// Assigns integer IDs to each lane of an agent. This is to avoid moving copies of the string name
/// into futures.
#[derive(Debug, Default)]
pub struct LaneRegistry {
    lane_id_counter: u64,
    lane_names: HashMap<Text, u64>,
    lane_names_rev: HashMap<u64, Text>,
}

impl LaneRegistry {
    fn next_id(&mut self) -> u64 {
        let id = self.lane_id_counter;
        self.lane_id_counter += 1;
        id
    }

    /// Add a new lane to the registry, getting its assigned ID.
    #[must_use]
    pub fn add_endpoint(&mut self, name: Text) -> u64 {
        let id = self.next_id();
        let LaneRegistry {
            lane_names,
            lane_names_rev,
            ..
        } = self;
        debug!("Adding lane with name '{}' and ID {}.", name, id);
        lane_names.insert(name.clone(), id);
        lane_names_rev.insert(id, name);
        id
    }

    pub fn id_for(&self, name: &str) -> Option<u64> {
        self.lane_names.get(name).copied()
    }

    pub fn name_for(&self, id: u64) -> Option<&str> {
        self.lane_names_rev.get(&id).map(Text::as_str)
    }
}

#[cfg(test)]
mod tests {
    use swim_model::Text;

    use super::LaneRegistry;

    #[test]
    fn add_endpoint() {
        let mut registry = LaneRegistry::default();
        let id = registry.add_endpoint(Text::new("lane"));

        assert_eq!(registry.id_for("lane"), Some(id));
        assert_eq!(registry.name_for(id), Some("lane"));
    }
}
