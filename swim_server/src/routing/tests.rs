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

use crate::routing::{RoutingAddr, TaggedEnvelope, TaggedSender};
use swim_common::routing::server::{
    ResolutionError, RouterError, ServerConnectionError, Unresolvable,
};
use swim_common::warp::envelope::Envelope;
use tokio::sync::mpsc;
use utilities::uri::RelativeUri;

#[tokio::test]
async fn tagged_sender() {
    let (tx, mut rx) = mpsc::channel(8);
    let mut sender = TaggedSender::new(RoutingAddr::remote(7), tx);

    assert!(sender
        .send_item(Envelope::linked("/node", "lane"))
        .await
        .is_ok());

    let received = rx.recv().await;
    assert_eq!(
        received,
        Some(TaggedEnvelope(
            RoutingAddr::remote(7),
            Envelope::linked("/node", "lane")
        ))
    );
}

#[test]
fn routing_addr_display() {
    let string = format!("{}", RoutingAddr::remote(0x1));
    assert_eq!(string, "Remote(1)");

    let string = format!("{}", RoutingAddr::local(0x1a));
    assert_eq!(string, "Local(1A)");
}

#[test]
fn unresolvable_display() {
    let err = Unresolvable(RoutingAddr::local(4).to_string());

    let string = err.to_string();

    assert_eq!(string, "No active endpoint with ID: Local(4)");
}

#[test]
fn resolution_error_display() {
    let string =
        ResolutionError::Unresolvable(Unresolvable(RoutingAddr::local(4).to_string())).to_string();
    assert_eq!(string, "Address Local(4) could not be resolved.");

    let string = ResolutionError::RouterDropped.to_string();
    assert_eq!(string, "The router channel was dropped.");
}

#[test]
fn router_error_display() {
    let uri: RelativeUri = "/name".parse().unwrap();
    let string = RouterError::NoAgentAtRoute(uri).to_string();
    assert_eq!(string, "No agent at: '/name'");

    let string = RouterError::ConnectionFailure(ServerConnectionError::ClosedRemotely).to_string();
    assert_eq!(
        string,
        "Failed to route to requested endpoint: 'The connection was closed remotely.'"
    );

    let string = RouterError::RouterDropped.to_string();
    assert_eq!(string, "The router channel was dropped.");
}
