// Copyright 2015-2020 SWIM.AI inc.
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

use crate::plane::error::{NoAgentAtRoute, Unresolvable};
use crate::routing::RoutingAddr;

#[test]
fn no_agent_at_route_display() {
    let err = NoAgentAtRoute("/test".to_string());
    let string = err.to_string();

    assert_eq!(string, "No agent at route: '/test'");
}

#[test]
fn unresolvable_display() {
    let err = Unresolvable(RoutingAddr::local(4));

    let string = err.to_string();

    assert_eq!(string, "No active endpoint with ID: Local(4)");
}
