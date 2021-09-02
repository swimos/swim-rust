// Copyright 2015-2021 SWIM.AI inc.
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
use crate::warp::envelope::Envelope;
use tokio::sync::mpsc;

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

    let string = format!("{}", RoutingAddr::plane(0x1a));
    assert_eq!(string, "Local(1A)");
}
