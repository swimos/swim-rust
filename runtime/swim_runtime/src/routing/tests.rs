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

use crate::routing::{RoutingAddr, TaggedEnvelope, TaggedSender};
use swim_warp::envelope::Envelope;
use tokio::sync::mpsc;

#[tokio::test]
async fn tagged_sender() {
    let (tx, mut rx) = mpsc::channel(8);
    let mut sender = TaggedSender::new(RoutingAddr::remote(7), tx);

    assert!(sender
        .send_item(Envelope::linked().node_uri("/node").lane_uri("lane").done())
        .await
        .is_ok());

    let received = rx.recv().await;
    assert_eq!(
        received,
        Some(TaggedEnvelope(
            RoutingAddr::remote(7),
            Envelope::linked().node_uri("/node").lane_uri("lane").done()
        ))
    );
}

#[test]
fn routing_addr_discriminate() {
    assert!(RoutingAddr::remote(0x1).is_remote());
    assert!(RoutingAddr::client(0x1).is_client());
    assert!(RoutingAddr::plane(0x1).is_plane());
    assert!(RoutingAddr::remote(u32::MAX).is_remote());
    assert!(RoutingAddr::client(u32::MAX).is_client());
    assert!(RoutingAddr::plane(u32::MAX).is_plane());
}

#[test]
fn routing_addr_display() {
    assert_eq!(
        RoutingAddr::remote(0x1).to_string(),
        "Remote(1)".to_string()
    );
    assert_eq!(
        RoutingAddr::remote(u32::MAX).to_string(),
        "Remote(4294967295)".to_string()
    );
    assert_eq!(RoutingAddr::plane(0x1).to_string(), "Plane(1)".to_string());
    assert_eq!(
        RoutingAddr::plane(u32::MAX).to_string(),
        "Plane(4294967295)".to_string()
    );
    assert_eq!(
        RoutingAddr::client(0x1).to_string(),
        "Client(1)".to_string()
    );
    assert_eq!(
        RoutingAddr::client(u32::MAX).to_string(),
        "Client(4294967295)".to_string()
    );
}
