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

use crate::agent::dispatch::error::{DispatcherError, DispatcherErrors};
use crate::agent::dispatch::tests::mock::{MockExecutionContext, MockLane};
use crate::agent::dispatch::{AgentDispatcher, LaneIdentifier, LaneIdentifierParseErr};
use crate::agent::lane::channels::task::LaneIoError;
use crate::agent::lane::channels::update::UpdateError;
use crate::agent::lane::channels::AgentExecutionConfig;
use crate::agent::AttachError;
use crate::agent::LaneIo;
use crate::meta::log::LogLevel;
use crate::meta::{LaneAddressedKind, MetaNodeAddressed};
use futures::future::{join, BoxFuture};
use futures::{FutureExt, Stream, StreamExt};
use std::collections::HashMap;
use std::convert::TryFrom;
use std::sync::Arc;
use std::time::Duration;
use stm::transaction::TransactionError;
use swim_async_runtime::time::timeout;
use swim_model::path::RelativePath;
use swim_model::Value;
use swim_runtime::compat::RequestMessage;
use swim_runtime::routing::{RoutingAddr, TaggedEnvelope};
use swim_utilities::algebra::non_zero_usize;
use swim_utilities::errors::Recoverable;
use swim_utilities::time::AtomicInstant;
use swim_warp::envelope::Envelope;
use tokio::sync::mpsc;
use tokio::sync::mpsc::Sender;
use tokio::time::Instant;
use tokio_stream::wrappers::ReceiverStream;

mod mock;

fn make_dispatcher(
    buffer_size: usize,
    max_pending: usize,
    lanes: HashMap<LaneIdentifier, MockLane>,
    envelopes: impl Stream<Item = RequestMessage<Value>> + Send + 'static,
) -> (
    BoxFuture<'static, Result<DispatcherErrors, DispatcherErrors>>,
    MockExecutionContext,
) {
    let (spawn_tx, spawn_rx) = mpsc::channel(8);

    let boxed_lanes = lanes
        .into_iter()
        .map(|(name, lane)| (name, lane.boxed()))
        .collect();

    let context = MockExecutionContext::new(RoutingAddr::plane(1024), buffer_size, spawn_tx);

    let config = AgentExecutionConfig::with(
        non_zero_usize!(8),
        max_pending,
        0,
        Duration::from_secs(1),
        None,
        Duration::from_secs(60),
    );

    let dispatcher = AgentDispatcher::new(
        "/node".parse().unwrap(),
        config,
        context.clone(),
        boxed_lanes,
    );

    let spawn_task = ReceiverStream::new(spawn_rx).for_each_concurrent(None, |eff| eff);

    let uplinks_idle_since = Arc::new(AtomicInstant::new(Instant::now().into_std()));
    let dispatch_task = dispatcher.run(envelopes, uplinks_idle_since);

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

    map
}

async fn expect_echo(rx: &mut mpsc::Receiver<TaggedEnvelope>, env: RequestMessage<Value>) {
    let maybe_envelope = rx.recv().await;
    assert!(maybe_envelope.is_some());
    let rec_envelope = maybe_envelope.unwrap();

    let expected = mock::echo(env);
    assert_eq!(rec_envelope.1, expected);
}

#[tokio::test]
async fn dispatch_nothing() {
    let (envelope_tx, envelope_rx) = mpsc::channel::<RequestMessage<Value>>(8);

    let (task, context) =
        make_dispatcher(8, 10, lanes(vec!["lane"]), ReceiverStream::new(envelope_rx));

    drop(envelope_tx);
    drop(context);

    let result = task.await;
    assert!(matches!(result, Ok(errs) if errs.is_empty()));
}

#[tokio::test]
async fn dispatch_single() {
    let (envelope_tx, envelope_rx) = mpsc::channel::<RequestMessage<Value>>(8);

    let (task, context) =
        make_dispatcher(8, 10, lanes(vec!["lane"]), ReceiverStream::new(envelope_rx));

    let addr = RoutingAddr::remote(1);

    let link = RequestMessage::link(addr, RelativePath::new("/node", "lane"));

    let assertion_task = async move {
        assert!(envelope_tx.send(link.clone()).await.is_ok());

        let mut rx = context.take_receiver(&addr).unwrap();
        expect_echo(&mut rx, link).await;

        drop(envelope_tx);
        drop(context);
    };

    let (result, _) = join(task, assertion_task).await;
    assert!(matches!(result, Ok(errs) if errs.is_empty()));
}

#[tokio::test]
async fn dispatch_two_lanes() {
    let (envelope_tx, envelope_rx) = mpsc::channel::<RequestMessage<Value>>(8);

    let (task, context) = make_dispatcher(
        8,
        10,
        lanes(vec!["lane_a", "lane_b"]),
        ReceiverStream::new(envelope_rx),
    );

    let addr1 = RoutingAddr::remote(1);
    let addr2 = RoutingAddr::remote(2);

    let link = RequestMessage::link(addr1, RelativePath::new("/node", "lane_a"));
    let sync = RequestMessage::sync(addr2, RelativePath::new("/node", "lane_b"));

    let assertion_task = async move {
        assert!(envelope_tx.send(link.clone()).await.is_ok());
        assert!(envelope_tx.send(sync.clone()).await.is_ok());

        let mut rx1 = context.take_receiver(&addr1).unwrap();
        expect_echo(&mut rx1, link).await;

        let mut rx2 = context.take_receiver(&addr2).unwrap();
        expect_echo(&mut rx2, sync).await;

        drop(envelope_tx);
        drop(context);
    };

    let (result, _) = join(task, assertion_task).await;
    assert!(matches!(result, Ok(errs) if errs.is_empty()));
}

#[tokio::test]
async fn dispatch_multiple_same_lane() {
    let (envelope_tx, envelope_rx) = mpsc::channel::<RequestMessage<Value>>(8);

    let (task, context) =
        make_dispatcher(8, 10, lanes(vec!["lane"]), ReceiverStream::new(envelope_rx));

    let addr = RoutingAddr::remote(1);

    let link = RequestMessage::link(addr, RelativePath::new("/node", "lane"));

    let cmd1 = RequestMessage::command(addr, RelativePath::new("/node", "lane"), 1.into());
    let cmd2 = RequestMessage::command(addr, RelativePath::new("/node", "lane"), 2.into());

    let assertion_task = async move {
        assert!(envelope_tx.send(link.clone()).await.is_ok());
        assert!(envelope_tx.send(cmd1.clone()).await.is_ok());

        let mut rx = context.take_receiver(&addr).unwrap();
        expect_echo(&mut rx, link).await;
        expect_echo(&mut rx, cmd1).await;

        assert!(envelope_tx.send(cmd2.clone()).await.is_ok());
        expect_echo(&mut rx, cmd2).await;
        drop(envelope_tx);
        drop(context);
    };

    let (result, _) = join(task, assertion_task).await;
    assert!(matches!(result, Ok(errs) if errs.is_empty()));
}

#[tokio::test]
async fn blocked_lane() {
    let (envelope_tx, envelope_rx) = mpsc::channel::<RequestMessage<Value>>(8);

    let (task, context) = make_dispatcher(
        1,
        10,
        lanes(vec!["lane_a", "lane_b"]),
        ReceiverStream::new(envelope_rx),
    );

    let addr1 = RoutingAddr::remote(1);
    let addr2 = RoutingAddr::remote(2);

    let cmd1 = RequestMessage::command(addr1, RelativePath::new("/node", "lane_a"), 1.into());
    let cmd2 = RequestMessage::command(addr1, RelativePath::new("/node", "lane_a"), 2.into());
    let cmd3 = RequestMessage::command(addr1, RelativePath::new("/node", "lane_a"), 3.into());
    let cmd4 = RequestMessage::command(addr1, RelativePath::new("/node", "lane_a"), 4.into());

    let link = RequestMessage::link(addr2, RelativePath::new("/node", "lane_b"));

    let assertion_task = async move {
        assert!(envelope_tx.send(cmd1.clone()).await.is_ok());
        let mut rx1 = context.take_receiver(&addr1).unwrap();
        expect_echo(&mut rx1, cmd1).await;
        //Lane A is now attached.

        assert!(envelope_tx.send(cmd2.clone()).await.is_ok());
        assert!(envelope_tx.send(cmd3.clone()).await.is_ok());
        assert!(envelope_tx.send(cmd4.clone()).await.is_ok());

        assert!(envelope_tx.send(link.clone()).await.is_ok());

        //Wait until we receive the message for lane B indicating that lane A is definitely blocked
        //and the remaining messages must be pending.
        let mut rx2 = context.take_receiver(&addr2).unwrap();
        expect_echo(&mut rx2, link).await;

        //Unblock lane A and wait for both messages.
        expect_echo(&mut rx1, cmd2).await;
        expect_echo(&mut rx1, cmd3).await;
        expect_echo(&mut rx1, cmd4).await;

        drop(envelope_tx);
        drop(context);
    };

    let (result, _) = join(task, assertion_task).await;
    assert!(matches!(result, Ok(errs) if errs.is_empty()));
}

#[tokio::test]
async fn flush_pending() {
    let (envelope_tx, envelope_rx) = mpsc::channel::<RequestMessage<Value>>(8);

    let (task, context) = make_dispatcher(
        1,
        10,
        lanes(vec!["lane_a", "lane_b"]),
        ReceiverStream::new(envelope_rx),
    );

    let addr1 = RoutingAddr::remote(1);
    let addr2 = RoutingAddr::remote(2);

    let link = RequestMessage::link(addr2, RelativePath::new("/node", "lane_b"));

    //Chose to ensure there are several pending messages when the dispatcher stops.
    let n = 8;

    let assertion_task = async move {
        let cmd0 = RequestMessage::command(addr1, RelativePath::new("/node", "lane_a"), 0.into());

        assert!(envelope_tx.send(cmd0.clone()).await.is_ok());
        let mut rx1 = context.take_receiver(&addr1).unwrap();
        expect_echo(&mut rx1, cmd0).await;

        //Lane A is now attached.

        for i in 0..n {
            let cmd = RequestMessage::command(
                addr1,
                RelativePath::new("/node", "lane_a"),
                (i + 1).into(),
            );
            assert!(envelope_tx.send(cmd).await.is_ok());
        }

        assert!(envelope_tx.send(link.clone()).await.is_ok());

        //Wait until we receive the message for lane B indicating that lane A is definitely blocked
        //and the remaining messages must be pending.
        let mut rx2 = context.take_receiver(&addr2).unwrap();
        expect_echo(&mut rx2, link).await;

        //Drop the envelope sender to begin the shutdown process for the dispatcher.
        drop(envelope_tx);

        for i in 0..n {
            let cmd = RequestMessage::command(
                addr1,
                RelativePath::new("/node", "lane_a"),
                (i + 1).into(),
            );

            expect_echo(&mut rx1, cmd).await;
        }

        drop(context);
    };

    let (result, _) = join(task, assertion_task).await;
    assert!(matches!(result, Ok(errs) if errs.is_empty()));
}

#[tokio::test]
async fn dispatch_link_to_non_existent() {
    let (envelope_tx, envelope_rx) = mpsc::channel::<RequestMessage<Value>>(8);

    let (task, context) =
        make_dispatcher(8, 10, lanes(vec!["lane"]), ReceiverStream::new(envelope_rx));

    let addr = RoutingAddr::remote(1);

    let link = RequestMessage::link(addr, RelativePath::new("/node", "other"));

    let assertion_task = async move {
        assert!(envelope_tx.send(link.clone()).await.is_ok());

        let expected_env = Envelope::lane_not_found("/node", "other");

        let mut rx = context.take_receiver(&addr).unwrap();
        let TaggedEnvelope(_, env) = rx.recv().await.unwrap();

        assert_eq!(expected_env, env);

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
async fn dispatch_sync_to_non_existent() {
    let (envelope_tx, envelope_rx) = mpsc::channel::<RequestMessage<Value>>(8);

    let (task, context) =
        make_dispatcher(8, 10, lanes(vec!["lane"]), ReceiverStream::new(envelope_rx));

    let addr = RoutingAddr::remote(1);

    let sync = RequestMessage::sync(addr, RelativePath::new("/node", "other"));

    let assertion_task = async move {
        assert!(envelope_tx.send(sync.clone()).await.is_ok());

        let mut rx = context.take_receiver(&addr).unwrap();
        let result = timeout::timeout(Duration::from_secs(5), rx.recv()).await;

        assert!(result.is_err());

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
    let (envelope_tx, envelope_rx) = mpsc::channel::<RequestMessage<Value>>(8);

    let (task, context) =
        make_dispatcher(8, 10, lanes(vec!["lane"]), ReceiverStream::new(envelope_rx));

    let addr = RoutingAddr::remote(1);

    let cmd = RequestMessage::command(
        addr,
        RelativePath::new("/node", "lane"),
        mock::POISON_PILL.into(),
    );

    let assertion_task = async move {
        assert!(envelope_tx.send(cmd).await.is_ok());
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
    let (envelope_tx, envelope_rx) = mpsc::channel::<RequestMessage<Value>>(8);

    let (task, context) = make_dispatcher(
        8,
        10,
        lanes(vec![mock::POISON_PILL]),
        ReceiverStream::new(envelope_rx),
    );

    let addr = RoutingAddr::remote(1);

    let link = RequestMessage::link(addr, RelativePath::new("/node", mock::POISON_PILL));

    let assertion_task = async move {
        assert!(envelope_tx.send(link).await.is_ok());
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

#[tokio::test]
#[ignore]
async fn dispatch_meta() {
    let mut map = HashMap::new();

    map.insert(LaneIdentifier::agent("lane".to_string()), MockLane);
    map.insert(
        LaneIdentifier::Meta(MetaNodeAddressed::NodeProfile),
        MockLane,
    );
    map.insert(
        LaneIdentifier::Meta(MetaNodeAddressed::UplinkProfile {
            lane_uri: "bar".into(),
        }),
        MockLane,
    );
    map.insert(
        LaneIdentifier::Meta(MetaNodeAddressed::LaneAddressed {
            lane_uri: "bar".into(),
            kind: LaneAddressedKind::Pulse,
        }),
        MockLane,
    );
    map.insert(
        LaneIdentifier::Meta(MetaNodeAddressed::LaneAddressed {
            lane_uri: "bar".into(),
            kind: LaneAddressedKind::Log(LogLevel::Trace),
        }),
        MockLane,
    );
    map.insert(LaneIdentifier::Meta(MetaNodeAddressed::Lanes), MockLane);

    for level in LogLevel::enumerated() {
        map.insert(
            LaneIdentifier::Meta(MetaNodeAddressed::NodeLog(*level)),
            MockLane,
        );
    }

    let (envelope_tx, envelope_rx) = mpsc::channel::<RequestMessage<Value>>(8);

    let (task, context) = make_dispatcher(8, 10, map, ReceiverStream::new(envelope_rx));

    let mut receiver_idx = 0;

    let assertion_task = async move {
        let mut make_addr = || {
            receiver_idx += 1;
            RoutingAddr::remote(receiver_idx)
        };

        async fn assert(
            envelope_tx: &Sender<RequestMessage<Value>>,
            context: &MockExecutionContext,
            env: RequestMessage<Value>,
        ) {
            assert!(envelope_tx.send(env.clone()).await.is_ok());

            let addr = env.origin;

            let mut rx = context.take_receiver(&addr).unwrap();
            expect_echo(&mut rx, env).await;
        }

        assert(
            &envelope_tx,
            &context,
            RequestMessage::link(
                make_addr(),
                RelativePath::new("/swim:meta:node/unit%2Ffoo/", "pulse"),
            ),
        )
        .await;

        assert(
            &envelope_tx,
            &context,
            RequestMessage::link(
                make_addr(),
                RelativePath::new("/swim:meta:node/unit%2Ffoo/lane/bar", "uplink"),
            ),
        )
        .await;

        assert(
            &envelope_tx,
            &context,
            RequestMessage::link(
                make_addr(),
                RelativePath::new("/swim:meta:node/unit%2Ffoo/lane/bar", "traceLog"),
            ),
        )
        .await;

        assert(
            &envelope_tx,
            &context,
            RequestMessage::link(
                make_addr(),
                RelativePath::new("/swim:meta:node/unit%2Ffoo/", "lanes"),
            ),
        )
        .await;

        for level in LogLevel::enumerated() {
            assert(
                &envelope_tx,
                &context,
                RequestMessage::link(
                    make_addr(),
                    RelativePath::new("/swim:meta:node/unit%2Ffoo", level.uri_ref()),
                ),
            )
            .await;
        }

        let addr = make_addr();
        let env = RequestMessage::link(
            make_addr(),
            RelativePath::new("/swim:meta:node/unit%2Ffoo/bar/fizz", "lane"),
        );

        assert!(envelope_tx.send(env.clone()).await.is_ok());

        let expected_env = Envelope::lane_not_found("/swim:meta:node/unit%2Ffoo/bar/fizz", "lane");

        let mut rx = context.take_receiver(&addr).unwrap();
        let TaggedEnvelope(_, env) = rx.recv().await.unwrap();

        assert_eq!(expected_env, env);

        drop(envelope_tx);
        drop(context);
    };

    let (result, _) = join(task, assertion_task).await;

    assert!(matches!(result, Ok(errs) if !errs.is_fatal()));
}

#[test]
fn parse_lane_identifier() {
    let path = RelativePath::new("/swim:meta:node/unit%2Ffoo/lane/bar", "traceLog");
    let result = LaneIdentifier::try_from(&path);

    assert_eq!(
        result,
        Ok(LaneIdentifier::meta(MetaNodeAddressed::LaneAddressed {
            lane_uri: "bar".into(),
            kind: LaneAddressedKind::Log(LogLevel::Trace)
        }))
    );

    let path = RelativePath::new("/swim:meta:node/unit%2Ffoo/host/bar", "traceLog");
    let result = LaneIdentifier::try_from(&path);

    assert_eq!(
        result,
        Err(LaneIdentifierParseErr::UnknownMetaNodeAddress(
            "/swim:meta:node/unit%2Ffoo/host/bar".to_string()
        ))
    );

    let path = RelativePath::new("/node", "lane");
    let result = LaneIdentifier::try_from(&path);

    assert_eq!(result, Ok(LaneIdentifier::agent("lane".to_string())));
}

#[test]
fn lane_identifier_display() {
    let agent_identifier = LaneIdentifier::agent("/lane".to_string());
    assert_eq!(format!("{}", agent_identifier), "Agent(lane: \"/lane\")");

    let meta_identifier = LaneIdentifier::meta(MetaNodeAddressed::Lanes);
    assert_eq!(format!("{}", meta_identifier), "Meta(Lanes)");
}
