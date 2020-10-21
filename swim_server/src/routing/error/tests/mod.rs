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

use crate::routing::error::Unresolvable;
use crate::routing::RoutingAddr;

#[test]
fn unresolvable_display() {
    let err = Unresolvable(RoutingAddr::local(4));

    let string = err.to_string();

    assert_eq!(string, "No active endpoint with ID: Local(4)");
}

/*#[test]
fn resolution_error_display() {
    let err = ResolutionError::NoRoute(RoutingError::HostUnreachable);

    assert_eq!(err.to_string(), RoutingError::HostUnreachable.to_string());

    let err = ResolutionError::NoAgent(NoAgentAtRoute("/path".parse().unwrap()));

    assert_eq!(
        err.to_string(),
        NoAgentAtRoute("/path".parse().unwrap()).to_string()
    );
}
*/
