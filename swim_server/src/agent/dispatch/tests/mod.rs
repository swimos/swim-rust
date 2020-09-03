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

use crate::agent::dispatch::tests::mock::{MockExecutionContext, MockLane};
use crate::agent::dispatch::{AgentDispatcher, DispatcherError};
use crate::agent::lane::channels::AgentExecutionConfig;
use crate::routing::{TaggedEnvelope, RoutingAddr};
use futures::future::{join, BoxFuture};
use futures::{Stream, StreamExt, FutureExt};
use std::collections::HashMap;
use tokio::sync::mpsc;
use crate::agent::LaneIo;
use swim_common::warp::envelope::{Envelope, OutgoingLinkMessage};
use swim_common::warp::path::RelativePath;
use pin_utils::core_reexport::num::NonZeroUsize;
use std::time::Duration;

mod mock;

fn make_dispatcher(
    buffer_size: usize,
    max_pending: usize,
    lanes: HashMap<String, MockLane>,
    envelopes: impl Stream<Item = TaggedEnvelope> + Send + 'static,
) -> (
    BoxFuture<'static, Result<(), DispatcherError>>,
    MockExecutionContext,
) {
    let (spawn_tx, spawn_rx) = mpsc::channel(8);

    let boxed_lanes = lanes
        .into_iter()
        .map(|(name, lane)| (name, lane.boxed()))
        .collect();

    let context = MockExecutionContext::new(buffer_size, spawn_tx);

    let default_size = NonZeroUsize::new(8).unwrap();

    let config = AgentExecutionConfig {
        max_pending_envelopes: max_pending,
        action_buffer: default_size,
        update_buffer: default_size,
        feedback_buffer: default_size,
        uplink_err_buffer: default_size,
        max_fatal_uplink_errors: 0,
        max_uplink_start_attempts: default_size,
        lane_buffer: default_size,
        lane_attachment_buffer: default_size,
        yield_after: NonZeroUsize::new(2048).unwrap(),
        retry_strategy: Default::default(),
        cleanup_timeout: Duration::from_secs(1),
    };

    let dispatcher = AgentDispatcher::new(
        "node".to_string(),
        config,
        context.clone(),
        boxed_lanes,
    );

    let spawn_task = spawn_rx.for_each_concurrent(None, |eff| eff);

    let dispatch_task = dispatcher.run(envelopes);

    (
        join(spawn_task, dispatch_task).map(|(_, r)| r).boxed(),
        context,
    )
}

fn lanes(names: Vec<&str>) -> HashMap<String, MockLane> {
    let mut map = HashMap::new();
    for name in names.iter() {
        map.insert(name.to_string(), MockLane);
    }
    map
}

async fn expect_echo(rx: &mut mpsc::Receiver<Envelope>,
                     lane: &str,
                     envelope: Envelope) {
    let route = RelativePath::new("node", lane);
    let maybe_envelope = rx.recv().await;
    assert!(maybe_envelope.is_some());
    let rec_envelope = maybe_envelope.unwrap();
    if let Ok(OutgoingLinkMessage {
                  header,
                  path: _,
                  body }) = envelope.into_outgoing() {
        let expected = mock::echo(&route, header, body);
        assert_eq!(rec_envelope, expected);
    } else {
        panic!("Cannot echo incoming envelope.")
    }
}

#[tokio::test]
async fn dispatch_nothing() {
    let (envelope_tx, envelope_rx) = mpsc::channel::<TaggedEnvelope>(8);

    let (task, context) = make_dispatcher(8, 10, lanes(vec!["lane"]), envelope_rx);

    drop(envelope_tx);
    drop(context);

    let result = task.await;
    assert!(result.is_ok());
}

#[tokio::test]
async fn dispatch_single() {
    let (mut envelope_tx, envelope_rx) = mpsc::channel::<TaggedEnvelope>(8);

    let (task, context) = make_dispatcher(8, 10, lanes(vec!["lane"]), envelope_rx);

    let addr = RoutingAddr::remote(1);

    let link = Envelope::link("node", "lane");

    let assertion_task = async move {

        assert!(envelope_tx.send(TaggedEnvelope(addr, link.clone())).await.is_ok());

        let mut rx = context.take_receiver(&addr).unwrap();
        expect_echo(&mut rx, "lane", link).await;

        drop(envelope_tx);
        drop(context);
    };

    let (result, _) = join(task, assertion_task).await;
    assert!(result.is_ok());
}

#[tokio::test]
async fn dispatch_two_lanes() {
    let (mut envelope_tx, envelope_rx) = mpsc::channel::<TaggedEnvelope>(8);

    let (task, context) = make_dispatcher(8, 10,
                                          lanes(vec!["lane_a", "lane_b"]), envelope_rx);

    let addr1 = RoutingAddr::remote(1);
    let addr2 = RoutingAddr::remote(2);

    let link = Envelope::link("node", "lane_a");
    let sync = Envelope::link("node", "lane_b");

    let assertion_task = async move {

        assert!(envelope_tx.send(TaggedEnvelope(addr1, link.clone())).await.is_ok());
        assert!(envelope_tx.send(TaggedEnvelope(addr2, sync.clone())).await.is_ok());

        let mut rx1 = context.take_receiver(&addr1).unwrap();
        expect_echo(&mut rx1, "lane_a", link).await;

        let mut rx2 = context.take_receiver(&addr2).unwrap();
        expect_echo(&mut rx2, "lane_b", sync).await;

        drop(envelope_tx);
        drop(context);
    };

    let (result, _) = join(task, assertion_task).await;
    assert!(result.is_ok());
}

#[tokio::test]
async fn dispatch_multiple_same_lane() {
    let (mut envelope_tx, envelope_rx) = mpsc::channel::<TaggedEnvelope>(8);

    let (task, context) = make_dispatcher(8, 10, lanes(vec!["lane"]), envelope_rx);

    let addr = RoutingAddr::remote(1);

    let link = Envelope::link("node", "lane");
    let cmd1 = Envelope::make_command("node", "lane", Some(1.into()));
    let cmd2 = Envelope::make_command("node", "lane", Some(2.into()));

    let assertion_task = async move {

        assert!(envelope_tx.send(TaggedEnvelope(addr, link.clone())).await.is_ok());
        assert!(envelope_tx.send(TaggedEnvelope(addr, cmd1.clone())).await.is_ok());

        let mut rx = context.take_receiver(&addr).unwrap();
        expect_echo(&mut rx, "lane", link).await;
        expect_echo(&mut rx, "lane", cmd1).await;

        assert!(envelope_tx.send(TaggedEnvelope(addr, cmd2.clone())).await.is_ok());
        expect_echo(&mut rx, "lane", cmd2).await;
        drop(envelope_tx);
        drop(context);
    };

    let (result, _) = join(task, assertion_task).await;
    assert!(result.is_ok());
}

#[tokio::test]
async fn blocked_lane() {
    let (mut envelope_tx, envelope_rx) = mpsc::channel::<TaggedEnvelope>(8);

    let (task, context) = make_dispatcher(1, 10,
                                          lanes(vec!["lane_a", "lane_b"]), envelope_rx);

    let addr1 = RoutingAddr::remote(1);
    let addr2 = RoutingAddr::remote(2);

    let cmd1 = Envelope::make_command("node", "lane_a", Some(1.into()));
    let cmd2 = Envelope::make_command("node", "lane_a", Some(2.into()));
    let cmd3 = Envelope::make_command("node", "lane_a", Some(3.into()));
    let link = Envelope::link("node", "lane_b");

    let assertion_task = async move {

        assert!(envelope_tx.send(TaggedEnvelope(addr1, cmd1.clone())).await.is_ok());
        let mut rx1 = context.take_receiver(&addr1).unwrap();
        expect_echo(&mut rx1, "lane_a", cmd1).await;
        //Lane A is now attached.


        assert!(envelope_tx.send(TaggedEnvelope(addr1, cmd2.clone())).await.is_ok());
        assert!(envelope_tx.send(TaggedEnvelope(addr1, cmd3.clone())).await.is_ok());
        assert!(envelope_tx.send(TaggedEnvelope(addr2, link.clone())).await.is_ok());

        //Wait until we receive the message for lane B indicating that lane A is definitely blocked
        //and the second message must be pending.
        let mut rx2 = context.take_receiver(&addr2).unwrap();
        expect_echo(&mut rx2, "lane_b", link).await;

        //Unblock lane A and wait for both messages.
        expect_echo(&mut rx1, "lane_a", cmd2).await;
        expect_echo(&mut rx1, "lane_a", cmd3).await;

        drop(envelope_tx);
        drop(context);
    };

    let (result, _) = join(task, assertion_task).await;
    assert!(result.is_ok());
}
