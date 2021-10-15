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

use crate::routing::remote::table::{HostAndPort, RoutingTable};
use crate::routing::remote::RawRoute;
use crate::routing::{ConnectionDropped, RoutingAddr, TaggedEnvelope};
use std::time::Duration;
use swim_warp::envelope::Envelope;
use tokio::sync::mpsc;

#[test]
fn host_and_port_display() {
    let hp = HostAndPort::new("host".to_string(), 12);
    assert_eq!(hp.to_string(), "host:12");
}

#[tokio::test]
async fn insert_and_retrieve() {
    let mut table = RoutingTable::default();
    let addr = RoutingAddr::remote(5);
    let hp = HostAndPort::new("host".to_string(), 45);
    let sock_addr = "192.168.0.1:80".parse().unwrap();
    let (tx, mut rx) = mpsc::channel(8);

    table.insert(addr, Some(hp.clone()), sock_addr, tx);

    assert_eq!(table.try_resolve(&hp), Some(addr));
    assert_eq!(table.get_resolved(&sock_addr), Some(addr));

    let RawRoute { sender, on_drop: _ } = table.resolve(addr).unwrap();

    let env = TaggedEnvelope(
        addr,
        Envelope::unlink().node_uri("node").lane_uri("lane").done(),
    );

    assert!(sender.send(env.clone()).await.is_ok());

    let result = rx.recv().await;

    assert_eq!(result, Some(env));
}

#[tokio::test]
async fn add_host_to_existing() {
    let mut table = RoutingTable::default();
    let addr = RoutingAddr::remote(5);
    let hp = HostAndPort::new("host".to_string(), 45);
    let hp2 = HostAndPort::new("host2".to_string(), 45);
    let sock_addr = "192.168.0.1:80".parse().unwrap();
    let (tx, _rx) = mpsc::channel(8);

    assert!(table.add_host(hp2.clone(), sock_addr).is_none());
    table.insert(addr, Some(hp.clone()), sock_addr, tx);
    assert_eq!(table.add_host(hp2.clone(), sock_addr), Some(addr));

    assert_eq!(table.try_resolve(&hp2), Some(addr));
}

#[tokio::test]
async fn remove_entry() {
    let mut table = RoutingTable::default();
    let addr = RoutingAddr::remote(5);
    let hp = HostAndPort::new("host".to_string(), 45);
    let sock_addr = "192.168.0.1:80".parse().unwrap();
    let (tx, _rx) = mpsc::channel(8);

    table.insert(addr, Some(hp.clone()), sock_addr, tx);

    let RawRoute {
        sender: _sender,
        on_drop,
    } = table.resolve(addr).unwrap();

    let reason = ConnectionDropped::TimedOut(Duration::from_secs(4));

    let drop_trigger = table.remove(addr).unwrap();
    assert!(drop_trigger.provide(reason.clone()).is_ok());

    let result = on_drop.await;
    assert!(result.is_ok());
    assert_eq!(&*result.unwrap(), &reason);
}
