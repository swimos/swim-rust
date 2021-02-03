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

use crate::agent::dispatch::error::{DispatcherError, DispatcherErrors};
use crate::agent::dispatch::tests::mock::{MockExecutionContext, MockLane};
use crate::agent::dispatch::AgentDispatcher;
use crate::agent::lane::channels::task::LaneIoError;
use crate::agent::lane::channels::update::UpdateError;
use crate::agent::lane::channels::AgentExecutionConfig;
use crate::agent::meta::{LogLevel, MetaNodeAddressed};
use crate::agent::AttachError;
use crate::agent::LaneIo;
use crate::routing::{
    LaneIdentifier, RoutingAddr, TaggedAgentEnvelope, TaggedEnvelope, TaggedMetaEnvelope,
};
use futures::future::{join, BoxFuture};
use futures::{FutureExt, Stream, StreamExt};
use std::collections::HashMap;
use std::num::NonZeroUsize;
use std::time::Duration;
use stm::transaction::TransactionError;
use swim_common::warp::envelope::{Envelope, OutgoingLinkMessage};
use swim_common::warp::path::RelativePath;
use tokio::sync::mpsc;

mod mock;

fn make_dispatcher(
    buffer_size: usize,
    max_pending: usize,
    lanes: HashMap<LaneIdentifier, MockLane>,
    envelopes: impl Stream<Item = TaggedEnvelope> + Send + 'static,
) -> (
    BoxFuture<'static, Result<DispatcherErrors, DispatcherErrors>>,
    MockExecutionContext,
) {
    let (spawn_tx, spawn_rx) = mpsc::channel(8);

    let boxed_lanes = lanes
        .into_iter()
        .map(|(name, lane)| (name, lane.boxed()))
        .collect();

    let context = MockExecutionContext::new(RoutingAddr::local(1024), buffer_size, spawn_tx);

    let config = AgentExecutionConfig::with(
        NonZeroUsize::new(8).unwrap(),
        max_pending,
        0,
        Duration::from_secs(1),
    );

    let dispatcher = AgentDispatcher::new(
        "/node".parse().unwrap(),
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

fn lanes(names: Vec<&str>) -> HashMap<LaneIdentifier, MockLane> {
    let mut map = HashMap::new();
    for name in names.iter() {
        map.insert(LaneIdentifier::agent(name.to_string()), MockLane);
    }

    map.insert(
        LaneIdentifier::meta(MetaNodeAddressed::Log {
            node_uri: "/node".into(),
            level: LogLevel::Info,
        }),
        MockLane,
    );

    map
}

async fn expect_echo(rx: &mut mpsc::Receiver<TaggedEnvelope>, lane: &str, envelope: Envelope) {
    let route = RelativePath::new("/node", lane);
    let maybe_envelope = rx.recv().await;
    assert!(maybe_envelope.is_some());
    let rec_envelope = maybe_envelope.unwrap();
    if let Ok(OutgoingLinkMessage {
        header,
        path: _,
        body,
    }) = envelope.into_outgoing()
    {
        let expected = mock::echo(&route, header, body);
        assert_eq!(rec_envelope.into_envelope(), expected);
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
    assert!(matches!(result, Ok(errs) if errs.is_empty()));
}

#[tokio::test]
async fn dispatch_meta() {
    let (envelope_tx, envelope_rx) = mpsc::channel::<TaggedEnvelope>(8);

    let (task, context) = make_dispatcher(8, 10, lanes(vec!["lane"]), envelope_rx);

    let addr = RoutingAddr::remote(1);

    let link = Envelope::link("/node", "infoLog");

    let assertion_task = async move {
        assert!(envelope_tx
            .send(TaggedEnvelope::meta(TaggedMetaEnvelope(
                addr,
                link.clone(),
                MetaNodeAddressed::Log {
                    node_uri: "/node".into(),
                    level: LogLevel::Info
                }
            )))
            .await
            .is_ok());

        let mut rx = context.take_receiver(&addr).unwrap();
        expect_echo(&mut rx, "infoLog", link).await;

        drop(envelope_tx);
        drop(context);
    };

    let (result, _) = join(task, assertion_task).await;
    assert!(matches!(result, Ok(errs) if errs.is_empty()));
}

#[tokio::test]
async fn dispatch_single() {
    let (envelope_tx, envelope_rx) = mpsc::channel::<TaggedEnvelope>(8);

    let (task, context) = make_dispatcher(8, 10, lanes(vec!["lane"]), envelope_rx);

    let addr = RoutingAddr::remote(1);

    let link = Envelope::link("/node", "lane");

    let assertion_task = async move {
        assert!(envelope_tx
            .send(TaggedAgentEnvelope(addr, link.clone()).into())
            .await
            .is_ok());

        let mut rx = context.take_receiver(&addr).unwrap();
        expect_echo(&mut rx, "lane", link).await;

        drop(envelope_tx);
        drop(context);
    };

    let (result, _) = join(task, assertion_task).await;
    assert!(matches!(result, Ok(errs) if errs.is_empty()));
}

#[tokio::test]
async fn dispatch_two_lanes() {
    let (envelope_tx, envelope_rx) = mpsc::channel::<TaggedEnvelope>(8);

    let (task, context) = make_dispatcher(8, 10, lanes(vec!["lane_a", "lane_b"]), envelope_rx);

    let addr1 = RoutingAddr::remote(1);
    let addr2 = RoutingAddr::remote(2);

    let link = Envelope::link("/node", "lane_a");
    let sync = Envelope::link("/node", "lane_b");

    let assertion_task = async move {
        assert!(envelope_tx
            .send(TaggedAgentEnvelope(addr1, link.clone()).into())
            .await
            .is_ok());
        assert!(envelope_tx
            .send(TaggedAgentEnvelope(addr2, sync.clone()).into())
            .await
            .is_ok());

        let mut rx1 = context.take_receiver(&addr1).unwrap();
        expect_echo(&mut rx1, "lane_a", link).await;

        let mut rx2 = context.take_receiver(&addr2).unwrap();
        expect_echo(&mut rx2, "lane_b", sync).await;

        drop(envelope_tx);
        drop(context);
    };

    let (result, _) = join(task, assertion_task).await;
    assert!(matches!(result, Ok(errs) if errs.is_empty()));
}

#[tokio::test]
async fn dispatch_multiple_same_lane() {
    let (envelope_tx, envelope_rx) = mpsc::channel::<TaggedEnvelope>(8);

    let (task, context) = make_dispatcher(8, 10, lanes(vec!["lane"]), envelope_rx);

    let addr = RoutingAddr::remote(1);

    let link = Envelope::link("/node", "lane");
    let cmd1 = Envelope::make_command("/node", "lane", Some(1.into()));
    let cmd2 = Envelope::make_command("/node", "lane", Some(2.into()));

    let assertion_task = async move {
        assert!(envelope_tx
            .send(TaggedAgentEnvelope(addr, link.clone()).into())
            .await
            .is_ok());
        assert!(envelope_tx
            .send(TaggedAgentEnvelope(addr, cmd1.clone()).into())
            .await
            .is_ok());

        let mut rx = context.take_receiver(&addr).unwrap();
        expect_echo(&mut rx, "lane", link).await;
        expect_echo(&mut rx, "lane", cmd1).await;

        assert!(envelope_tx
            .send(TaggedAgentEnvelope(addr, cmd2.clone()).into())
            .await
            .is_ok());
        expect_echo(&mut rx, "lane", cmd2).await;
        drop(envelope_tx);
        drop(context);
    };

    let (result, _) = join(task, assertion_task).await;
    assert!(matches!(result, Ok(errs) if errs.is_empty()));
}

#[tokio::test]
async fn blocked_lane() {
    let (envelope_tx, envelope_rx) = mpsc::channel::<TaggedEnvelope>(8);

    let (task, context) = make_dispatcher(1, 10, lanes(vec!["lane_a", "lane_b"]), envelope_rx);

    let addr1 = RoutingAddr::remote(1);
    let addr2 = RoutingAddr::remote(2);

    let cmd1 = Envelope::make_command("/node", "lane_a", Some(1.into()));
    let cmd2 = Envelope::make_command("/node", "lane_a", Some(2.into()));
    let cmd3 = Envelope::make_command("/node", "lane_a", Some(3.into()));
    let cmd4 = Envelope::make_command("/node", "lane_a", Some(4.into()));
    let link = Envelope::link("/node", "lane_b");

    let assertion_task = async move {
        assert!(envelope_tx
            .send(TaggedAgentEnvelope(addr1, cmd1.clone()).into())
            .await
            .is_ok());
        let mut rx1 = context.take_receiver(&addr1).unwrap();
        expect_echo(&mut rx1, "lane_a", cmd1).await;
        //Lane A is now attached.

        assert!(envelope_tx
            .send(TaggedAgentEnvelope(addr1, cmd2.clone()).into())
            .await
            .is_ok());
        assert!(envelope_tx
            .send(TaggedAgentEnvelope(addr1, cmd3.clone()).into())
            .await
            .is_ok());
        assert!(envelope_tx
            .send(TaggedAgentEnvelope(addr1, cmd4.clone()).into())
            .await
            .is_ok());

        assert!(envelope_tx
            .send(TaggedAgentEnvelope(addr2, link.clone()).into())
            .await
            .is_ok());

        //Wait until we receive the message for lane B indicating that lane A is definitely blocked
        //and the remaining messages must be pending.
        let mut rx2 = context.take_receiver(&addr2).unwrap();
        expect_echo(&mut rx2, "lane_b", link).await;

        //Unblock lane A and wait for both messages.
        expect_echo(&mut rx1, "lane_a", cmd2).await;
        expect_echo(&mut rx1, "lane_a", cmd3).await;
        expect_echo(&mut rx1, "lane_a", cmd4).await;

        drop(envelope_tx);
        drop(context);
    };

    let (result, _) = join(task, assertion_task).await;
    assert!(matches!(result, Ok(errs) if errs.is_empty()));
}

#[tokio::test]
async fn flush_pending() {
    let (envelope_tx, envelope_rx) = mpsc::channel::<TaggedEnvelope>(8);

    let (task, context) = make_dispatcher(1, 10, lanes(vec!["lane_a", "lane_b"]), envelope_rx);

    let addr1 = RoutingAddr::remote(1);
    let addr2 = RoutingAddr::remote(2);

    let link = Envelope::link("/node", "lane_b");

    //Chose to ensure there are several pending messages when the dispatcher stops.
    let n = 8;

    let assertion_task = async move {
        let cmd0 = Envelope::make_command("/node", "lane_a", Some(0.into()));
        assert!(envelope_tx
            .send(TaggedAgentEnvelope(addr1, cmd0.clone()).into())
            .await
            .is_ok());
        let mut rx1 = context.take_receiver(&addr1).unwrap();
        expect_echo(&mut rx1, "lane_a", cmd0).await;

        //Lane A is now attached.

        for i in 0..n {
            let cmd = Envelope::make_command("/node", "lane_a", Some((i + 1).into()));
            assert!(envelope_tx
                .send(TaggedAgentEnvelope(addr1, cmd.clone()).into())
                .await
                .is_ok());
        }

        assert!(envelope_tx
            .send(TaggedAgentEnvelope(addr2, link.clone()).into())
            .await
            .is_ok());

        //Wait until we receive the message for lane B indicating that lane A is definitely blocked
        //and the remaining messages must be pending.
        let mut rx2 = context.take_receiver(&addr2).unwrap();
        expect_echo(&mut rx2, "lane_b", link).await;

        //Drop the envelope sender to begin the shutdown process for the dispatcher.
        drop(envelope_tx);

        for i in 0..n {
            let cmd = Envelope::make_command("/node", "lane_a", Some((i + 1).into()));
            expect_echo(&mut rx1, "lane_a", cmd).await;
        }

        drop(context);
    };

    let (result, _) = join(task, assertion_task).await;
    assert!(matches!(result, Ok(errs) if errs.is_empty()));
}

#[tokio::test]
async fn dispatch_to_non_existent() {
    let (envelope_tx, envelope_rx) = mpsc::channel::<TaggedEnvelope>(8);

    let (task, context) = make_dispatcher(8, 10, lanes(vec!["lane"]), envelope_rx);

    let addr = RoutingAddr::remote(1);

    let link = Envelope::link("/node", "other");

    let assertion_task = async move {
        assert!(envelope_tx
            .send(TaggedAgentEnvelope(addr, link).into())
            .await
            .is_ok());

        drop(envelope_tx);
        drop(context);
    };

    let (result, _) = join(task, assertion_task).await;
    match result.as_ref().map(|e| e.errors()) {
        Ok([DispatcherError::AttachmentFailed(AttachError::LaneDoesNotExist(name))]) => {
            assert_eq!(name, "other");
        }
        ow => panic!("Unexpected result {:?}.", ow),
    }
}

#[tokio::test]
async fn failed_lane_task() {
    let (envelope_tx, envelope_rx) = mpsc::channel::<TaggedEnvelope>(8);

    let (task, context) = make_dispatcher(8, 10, lanes(vec!["lane"]), envelope_rx);

    let addr = RoutingAddr::remote(1);

    let cmd = Envelope::make_command("/node", "lane", Some(mock::POISON_PILL.into()));

    let assertion_task = async move {
        assert!(envelope_tx
            .send(TaggedAgentEnvelope(addr, cmd).into())
            .await
            .is_ok());
        drop(context);
    };

    let (result, _) = join(task, assertion_task).await;
    match result.as_ref().map_err(|e| e.errors()) {
        Err([DispatcherError::LaneTaskFailed(err)]) => {
            let LaneIoError {
                route,
                update_error,
                uplink_errors,
            } = err;
            assert_eq!(route, &RelativePath::new("/node", "lane"));
            assert!(uplink_errors.is_empty());
            assert!(matches!(
                update_error,
                Some(UpdateError::FailedTransaction(
                    TransactionError::InvalidRetry
                ))
            ));
        }
        ow => panic!("Unexpected result {:?}.", ow),
    }
}

#[tokio::test]
async fn fatal_failed_attachment() {
    let (envelope_tx, envelope_rx) = mpsc::channel::<TaggedEnvelope>(8);

    let (task, context) = make_dispatcher(8, 10, lanes(vec![mock::POISON_PILL]), envelope_rx);

    let addr = RoutingAddr::remote(1);

    let link = Envelope::link("/node", mock::POISON_PILL);

    let assertion_task = async move {
        assert!(envelope_tx
            .send(TaggedAgentEnvelope(addr, link).into())
            .await
            .is_ok());
        drop(context);
    };

    let (result, _) = join(task, assertion_task).await;
    match result.as_ref().map_err(|e| e.errors()) {
        Err([DispatcherError::AttachmentFailed(err)]) => {
            assert_eq!(err, &AttachError::LaneStoppedReporting);
        }
        ow => panic!("Unexpected result {:?}.", ow),
    }
}
