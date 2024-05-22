// Copyright 2015-2023 Swim Inc.
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

use std::time::Duration;

use futures::{
    future::{join, join3, select, Either},
    stream::SelectAll,
    Future, StreamExt,
};
use swimos_agent_protocol::{agent::LaneRequest, map::MapMessage};
use swimos_api::agent::UplinkKind;
use swimos_model::Text;
use swimos_utilities::{
    io::byte_channel::{byte_channel, ByteReader},
    trigger,
};
use tokio::sync::mpsc;
use tokio_stream::wrappers::ReceiverStream;
use uuid::Uuid;

use crate::agent::{
    reporting::{UplinkReporter, UplinkSnapshot},
    task::{
        read_task,
        tests::{RemoteSender, BUFFER_SIZE, DEFAULT_TIMEOUT, INACTIVE_TEST_TIMEOUT},
        timeout_coord::{self, VoteResult},
        LaneEndpoint, ReadTaskMessage, RwCoordinationMessage, WriteTaskMessage,
    },
};

use super::{
    make_config, Event, LaneReader, ReportReaders, Snapshots, MAP_LANE, QUEUE_SIZE, TEST_TIMEOUT,
    VAL_LANE,
};

struct FakeAgent {
    initial: Vec<LaneEndpoint<ByteReader>>,
    coord: mpsc::Receiver<WriteTaskMessage>,
    stopping: trigger::Receiver,
    event_tx: mpsc::UnboundedSender<Event>,
}

impl FakeAgent {
    fn new(
        initial: Vec<LaneEndpoint<ByteReader>>,
        coord: mpsc::Receiver<WriteTaskMessage>,
        stopping: trigger::Receiver,
        event_tx: mpsc::UnboundedSender<Event>,
    ) -> Self {
        FakeAgent {
            initial,
            coord,
            stopping,
            event_tx,
        }
    }

    async fn run(self) -> Vec<Event> {
        let FakeAgent {
            initial,
            coord,
            stopping,
            event_tx,
        } = self;

        let mut lanes = SelectAll::new();
        for endpoint in initial {
            lanes.push(LaneReader::new(endpoint));
        }

        let mut coord_stream = ReceiverStream::new(coord).take_until(stopping);

        let mut events = vec![];

        loop {
            let event = match select(lanes.next(), coord_stream.next()).await {
                Either::Left((Some((name, Ok(Either::Left(LaneRequest::Sync(id))))), _))
                | Either::Left((Some((name, Ok(Either::Right(LaneRequest::Sync(id))))), _)) => {
                    Event::Sync { name, id }
                }
                Either::Left((Some((name, Ok(Either::Left(LaneRequest::Command(n))))), _)) => {
                    Event::ValueCommand { name, n }
                }
                Either::Left((Some((name, Ok(Either::Right(LaneRequest::Command(msg))))), _)) => {
                    Event::MapCommand { name, cmd: msg }
                }
                Either::Left((Some((name, Err(e))), _)) => {
                    panic!("Bad frame for {}: {:?}", name, e);
                }
                Either::Right((Some(WriteTaskMessage::Coord(coord)), _)) => Event::Coord(coord),
                _ => {
                    break;
                }
            };
            events.push(event.clone());
            let _ = event_tx.send(event);
        }
        events
    }
}

struct TestContext {
    stop_sender: trigger::Sender,
    reg_tx: mpsc::Sender<ReadTaskMessage>,
    write_voter: timeout_coord::Voter,
    http_voter: timeout_coord::Voter,
    vote_rx: timeout_coord::Receiver,
    event_rx: mpsc::UnboundedReceiver<Event>,
    readers: Option<ReportReaders>,
}

async fn run_test_case<F, Fut>(
    inactive_timeout: Duration,
    with_reporting: bool,
    test_case: F,
) -> (Vec<Event>, Fut::Output)
where
    F: FnOnce(TestContext) -> Fut,
    Fut: Future + Send,
{
    let (stop_tx, stop_rx) = trigger::trigger();
    let config = make_config(inactive_timeout);

    let (agg_rep, val_rep, map_rep, reporting) = if with_reporting {
        let agg_rep = UplinkReporter::default();
        let val_rep = UplinkReporter::default();
        let map_rep = UplinkReporter::default();
        let (_, reg_rx) = mpsc::channel(QUEUE_SIZE.get());
        let reporting = ReportReaders {
            _reg_rx: reg_rx,
            aggregate: agg_rep.reader(),
            lanes: [(VAL_LANE, val_rep.reader()), (MAP_LANE, map_rep.reader())]
                .into_iter()
                .collect(),
        };
        (Some(agg_rep), Some(val_rep), Some(map_rep), Some(reporting))
    } else {
        (None, None, None, None)
    };

    let endpoints = vec![
        LaneEndpoint {
            name: Text::new(VAL_LANE),
            kind: UplinkKind::Value,
            transient: false,
            io: byte_channel(BUFFER_SIZE),
            reporter: val_rep,
        },
        LaneEndpoint {
            name: Text::new(MAP_LANE),
            kind: UplinkKind::Map,
            transient: false,
            io: byte_channel(BUFFER_SIZE),
            reporter: map_rep,
        },
    ];

    let (event_tx, event_rx) = mpsc::unbounded_channel();

    let (endpoints_tx, endpoints_rx) = endpoints.into_iter().map(LaneEndpoint::split).unzip();
    let (coord_tx, coord_rx) = mpsc::channel(QUEUE_SIZE.get());
    let (reg_tx, reg_rx) = mpsc::channel(QUEUE_SIZE.get());

    let agent = FakeAgent::new(endpoints_rx, coord_rx, stop_rx.clone(), event_tx);

    let (vote1, vote2, vote3, vote_rx) = timeout_coord::agent_timeout_coordinator();

    let read = read_task(
        config,
        endpoints_tx,
        reg_rx,
        coord_tx,
        vote1,
        stop_rx,
        agg_rep,
    );

    let context = TestContext {
        stop_sender: stop_tx,
        reg_tx,
        write_voter: vote2,
        http_voter: vote3,
        vote_rx,
        event_rx,
        readers: reporting,
    };

    let test_task = test_case(context);

    let (events, _, value) =
        tokio::time::timeout(TEST_TIMEOUT, join3(agent.run(), read, test_task))
            .await
            .expect("Test timeout out");
    (events, value)
}

#[tokio::test]
async fn shutdown_no_remotes() {
    let (events, _) = run_test_case(DEFAULT_TIMEOUT, false, |context| async move {
        let TestContext {
            stop_sender,
            reg_tx: _reg_tx,
            write_voter: _write_voter,
            http_voter: _http_voter,
            vote_rx: _vote_rx,
            event_rx: _event_rx,
            ..
        } = context;
        stop_sender.trigger();
    })
    .await;
    assert!(events.is_empty());
}

const RID: Uuid = Uuid::from_u128(0);
const RID2: Uuid = Uuid::from_u128(1);
const NODE: &str = "node";

async fn attach_remote_with(rid: Uuid, reg_tx: &mpsc::Sender<ReadTaskMessage>) -> RemoteSender {
    let (tx, rx) = byte_channel(BUFFER_SIZE);
    assert!(reg_tx
        .send(ReadTaskMessage::Remote {
            reader: rx,
            on_attached: None
        })
        .await
        .is_ok());
    RemoteSender::new(NODE.to_string(), rid, tx)
}
async fn attach_remote(reg_tx: &mpsc::Sender<ReadTaskMessage>) -> RemoteSender {
    attach_remote_with(RID, reg_tx).await
}

#[tokio::test]
async fn attach_remote_and_link() {
    let (events, _) = run_test_case(DEFAULT_TIMEOUT, false, |context| async move {
        let TestContext {
            stop_sender,
            reg_tx,
            write_voter: _write_voter,
            http_voter: _http_voter,
            vote_rx: _vote_rx,
            mut event_rx,
            ..
        } = context;
        let mut sender = attach_remote(&reg_tx).await;
        sender.link(VAL_LANE).await;
        let event = event_rx.recv().await;
        match event {
            Some(Event::Coord(RwCoordinationMessage::Link { origin, lane })) => {
                assert_eq!(origin, RID);
                assert_eq!(lane, VAL_LANE);
            }
            ow => panic!("Unexpected event: {:?}", ow),
        }
        stop_sender.trigger();
    })
    .await;
    assert_eq!(events.len(), 1);
}

#[tokio::test]
async fn attach_remote_and_sync() {
    let (events, _) = run_test_case(DEFAULT_TIMEOUT, false, |context| async move {
        let TestContext {
            stop_sender,
            reg_tx,
            write_voter: _write_voter,
            http_voter: _http_voter,
            vote_rx: _vote_rx,
            mut event_rx,
            ..
        } = context;
        let mut sender = attach_remote(&reg_tx).await;
        sender.sync(VAL_LANE).await;
        let event = event_rx.recv().await;
        match event {
            Some(Event::Sync { name, id }) => {
                assert_eq!(id, RID);
                assert_eq!(name, VAL_LANE);
            }
            ow => panic!("Unexpected event: {:?}", ow),
        }
        stop_sender.trigger();
    })
    .await;
    assert_eq!(events.len(), 1);
}

#[tokio::test]
async fn attach_remote_and_value_command() {
    let (events, _) = run_test_case(DEFAULT_TIMEOUT, false, |context| async move {
        let TestContext {
            stop_sender,
            reg_tx,
            write_voter: _write_voter,
            http_voter: _http_voter,
            vote_rx: _vote_rx,
            mut event_rx,
            ..
        } = context;
        let mut sender = attach_remote(&reg_tx).await;
        sender.value_command(VAL_LANE, 77).await;
        let event = event_rx.recv().await;
        match event {
            Some(Event::ValueCommand { name, n }) => {
                assert_eq!(name, VAL_LANE);
                assert_eq!(n, 77);
            }
            ow => panic!("Unexpected event: {:?}", ow),
        }
        stop_sender.trigger();
    })
    .await;
    assert_eq!(events.len(), 1);
}

#[tokio::test]
async fn attach_remote_and_map_command() {
    let (events, _) = run_test_case(DEFAULT_TIMEOUT, false, |context| async move {
        let TestContext {
            stop_sender,
            reg_tx,
            write_voter: _write_voter,
            http_voter: _http_voter,
            vote_rx: _vote_rx,
            mut event_rx,
            ..
        } = context;
        let mut sender = attach_remote(&reg_tx).await;
        sender.map_command(MAP_LANE, "key", 647).await;
        let event = event_rx.recv().await;
        match event {
            Some(Event::MapCommand {
                name,
                cmd: MapMessage::Update { key, value },
            }) => {
                assert_eq!(name, MAP_LANE);
                assert_eq!(key, "key");
                assert_eq!(value, 647);
            }
            ow => panic!("Unexpected event: {:?}", ow),
        }
        stop_sender.trigger();
    })
    .await;
    assert_eq!(events.len(), 1);
}

#[tokio::test]
async fn votes_to_stop() {
    let (events, _stop_sender) =
        run_test_case(INACTIVE_TEST_TIMEOUT, false, |context| async move {
            let TestContext {
                stop_sender,
                reg_tx,
                write_voter,
                http_voter,
                vote_rx,
                event_rx: _event_rx,
                ..
            } = context;
            let _sender = attach_remote(&reg_tx).await;
            //Voting on behalf of the missing write and HTTP tasks.
            assert_eq!(write_voter.vote(), VoteResult::UnanimityPending);
            assert_eq!(http_voter.vote(), VoteResult::UnanimityPending);
            vote_rx.await;
            stop_sender
        })
        .await;
    assert!(events.is_empty());
}

#[tokio::test]
async fn rescinds_stop_vote_on_input() {
    let (events, _) = run_test_case(INACTIVE_TEST_TIMEOUT, false, |context| async move {
        let TestContext {
            stop_sender,
            reg_tx,
            write_voter,
            http_voter,
            vote_rx: _vote_rx,
            mut event_rx,
            ..
        } = context;
        let mut sender = attach_remote(&reg_tx).await;

        tokio::time::sleep(2 * INACTIVE_TEST_TIMEOUT).await;

        sender.value_command(VAL_LANE, 77).await;
        let _ = event_rx.recv().await;
        assert_eq!(write_voter.vote(), VoteResult::UnanimityPending);
        assert_eq!(http_voter.vote(), VoteResult::UnanimityPending);
        stop_sender
    })
    .await;
    assert_eq!(events.len(), 1);
}

#[tokio::test]
async fn attach_two_remotes_and_link() {
    let (events, _) = run_test_case(DEFAULT_TIMEOUT, false, |context| async move {
        let TestContext {
            stop_sender,
            reg_tx,
            write_voter: _write_voter,
            http_voter: _http_voter,
            vote_rx: _vote_rx,
            mut event_rx,
            ..
        } = context;
        let mut sender1 = attach_remote_with(RID, &reg_tx).await;
        let mut sender2 = attach_remote_with(RID2, &reg_tx).await;
        sender1.link(VAL_LANE).await;
        sender2.link(VAL_LANE).await;
        let event1 = event_rx.recv().await;
        let event2 = event_rx.recv().await;
        let seen;
        match event1 {
            Some(Event::Coord(RwCoordinationMessage::Link { origin, lane })) => {
                assert!(origin == RID || origin == RID2);
                seen = origin;
                assert_eq!(lane, VAL_LANE);
            }
            ow => panic!("Unexpected event: {:?}", ow),
        }
        match event2 {
            Some(Event::Coord(RwCoordinationMessage::Link { origin, lane })) => {
                assert!(origin == RID || origin == RID2);
                assert_ne!(origin, seen);
                assert_eq!(lane, VAL_LANE);
            }
            ow => panic!("Unexpected event: {:?}", ow),
        }
        stop_sender.trigger();
    })
    .await;
    assert_eq!(events.len(), 2);
}

#[tokio::test]
async fn send_on_two_remotes() {
    let (events, _) = run_test_case(DEFAULT_TIMEOUT, false, |context| async move {
        let TestContext {
            stop_sender,
            reg_tx,
            write_voter: _write_voter,
            http_voter: _http_voter,
            vote_rx: _vote_rx,
            mut event_rx,
            ..
        } = context;

        let reg_ref = &reg_tx;
        let sub_task_1 = async move {
            let mut sender1 = attach_remote_with(RID, reg_ref).await;
            for i in 0..100 {
                sender1.value_command(VAL_LANE, i).await;
            }
            sender1
        };

        let sub_task_2 = async move {
            let mut sender2 = attach_remote_with(RID2, reg_ref).await;
            for i in 0..100 {
                sender2.map_command(MAP_LANE, "key", i).await;
            }
            sender2
        };

        let (_s1, _s2) = join(sub_task_1, sub_task_2).await;

        let mut count = 0;
        while event_rx.recv().await.is_some() {
            count += 1;
            if count == 200 {
                break;
            }
        }

        stop_sender.trigger();
    })
    .await;

    assert_eq!(events.len(), 200);

    let mut prev_value = None;
    let mut prev_map = None;

    for event in events {
        match event {
            Event::ValueCommand { name, n } => {
                assert_eq!(name, VAL_LANE);
                assert_eq!(prev_value.map(|m| m + 1).unwrap_or(0), n);
                prev_value = Some(n);
            }
            Event::MapCommand {
                name,
                cmd: MapMessage::Update { key, value },
            } => {
                assert_eq!(name, MAP_LANE);
                assert_eq!(key, "key");
                assert_eq!(prev_map.map(|m| m + 1).unwrap_or(0), value);
                prev_map = Some(value);
            }
            ow => panic!("Unexpected event: {:?}", ow),
        }
    }
    assert_eq!(prev_value, Some(99));
    assert_eq!(prev_map, Some(99));
}

#[tokio::test]
async fn reports_command_counts() {
    let (events, _) = run_test_case(DEFAULT_TIMEOUT, true, |context| async move {
        let TestContext {
            stop_sender,
            reg_tx,
            write_voter: _write_voter,
            http_voter: _http_voter,
            vote_rx: _vote_rx,
            mut event_rx,
            readers,
        } = context;

        let mut sender = attach_remote(&reg_tx).await;

        sender.value_command(VAL_LANE, 77).await;
        let event = event_rx.recv().await;
        match event {
            Some(Event::ValueCommand { name, n }) => {
                assert_eq!(name, VAL_LANE);
                assert_eq!(n, 77);
            }
            ow => panic!("Unexpected event: {:?}", ow),
        }

        sender.map_command(MAP_LANE, "key", 647).await;
        let event = event_rx.recv().await;
        match event {
            Some(Event::MapCommand {
                name,
                cmd: MapMessage::Update { key, value },
            }) => {
                assert_eq!(name, MAP_LANE);
                assert_eq!(key, "key");
                assert_eq!(value, 647);
            }
            ow => panic!("Unexpected event: {:?}", ow),
        }

        let Snapshots { aggregate, lanes } = readers
            .as_ref()
            .and_then(ReportReaders::snapshot)
            .expect("Report readers not initialized or dropped.");

        assert_eq!(
            aggregate,
            UplinkSnapshot {
                link_count: 0,
                event_count: 0,
                command_count: 2
            }
        );
        assert_eq!(
            lanes[VAL_LANE],
            UplinkSnapshot {
                link_count: 0,
                event_count: 0,
                command_count: 1
            }
        );
        assert_eq!(
            lanes[MAP_LANE],
            UplinkSnapshot {
                link_count: 0,
                event_count: 0,
                command_count: 1
            }
        );
        stop_sender.trigger();
    })
    .await;
    assert_eq!(events.len(), 2);
}
