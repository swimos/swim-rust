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

use swim_runtime::routing::RoutingAddr;
use uuid::Uuid;

#[derive(Default, Debug)]
pub struct IdIssuer {
    remote_count: u32,
    agent_count: u32,
    downlink_count: u32,
}

impl IdIssuer {
    pub fn next_remote(&mut self) -> Uuid {
        let count = self.remote_count;
        self.remote_count += 1;
        *RoutingAddr::remote(count).uuid()
    }

    pub fn next_agent(&mut self) -> Uuid {
        let count = self.agent_count;
        self.agent_count += 1;
        *RoutingAddr::plane(count).uuid()
    }

    pub fn next_downlink(&mut self) -> Uuid {
        let count = self.downlink_count;
        self.downlink_count += 1;
        *RoutingAddr::client(count).uuid()
    }
}
