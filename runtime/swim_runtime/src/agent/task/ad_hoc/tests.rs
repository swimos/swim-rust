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

use std::{collections::HashMap, num::NonZeroUsize, time::Duration};

use crate::{
    agent::{task::AdHocChannelRequest, CommanderKey, CommanderRequest, LinkRequest},
    net::SchemeHostPort,
};
use futures::{
    future::join,
    stream::{unfold, SelectAll},
    Future, SinkExt, Stream, StreamExt,
};
use rand::Rng;
use swim_api::protocol::{
    agent::{AdHocCommand, AdHocCommandEncoder},
    WithLenReconEncoder,
};
use swim_form::structural::write::StructuralWritable;
use swim_messages::protocol::{Operation, RawRequestMessageDecoder, RequestMessage};
use swim_model::address::{Address, RelativeAddress};
use swim_recon::printer::print_recon_compact;
use swim_utilities::{
    future::retryable::RetryStrategy,
    io::byte_channel::{self, ByteReader, ByteWriter},
    non_zero_usize,
};
use tokio::sync::{mpsc, oneshot};
use tokio_util::codec::{FramedRead, FramedWrite};
use uuid::Uuid;

use super::{ad_hoc_commands_task, AdHocTaskConfig, AdHocTaskState};

struct TestContext {
    chan_tx: mpsc::Sender<AdHocChannelRequest>,
    links_rx: mpsc::Receiver<LinkRequest>,
}

const ID: Uuid = Uuid::from_u128(1);
const CHAN_SIZE: usize = 8;
const BUFFER_SIZE: NonZeroUsize = non_zero_usize!(1024);
const TIMEOUT: Duration = Duration::from_secs(1);
const TEST_TIMEOUT: Duration = Duration::from_secs(5);

async fn run_test<F, Fut>(test_case: F) -> (AdHocTaskState, Fut::Output)
where
    F: FnOnce(TestContext) -> Fut,
    Fut: Future,
{
    let (chan_tx, chan_rx) = mpsc::channel(CHAN_SIZE);
    let (links_tx, links_rx) = mpsc::channel(CHAN_SIZE);
    let state = AdHocTaskState::new(links_tx);
    let config = AdHocTaskConfig {
        buffer_size: BUFFER_SIZE,
        retry_strategy: RetryStrategy::none(),
        timeout_delay: TIMEOUT,
    };
    let task = ad_hoc_commands_task(ID, chan_rx, state, config);
    let context = TestContext { chan_tx, links_rx };
    let test_task = test_case(context);
    tokio::time::timeout(TEST_TIMEOUT, join(task, test_task))
        .await
        .expect("Test timed out.")
}

#[tokio::test]
async fn clean_shutdown_no_registration() {
    let (state, _) = run_test(|context| async move {
        let TestContext { chan_tx, links_rx } = context;
        drop(chan_tx);
        links_rx
    })
    .await;

    assert!(state.outputs.is_empty());
    assert!(state.reader.is_none());
}

async fn register(chan_tx: &mpsc::Sender<AdHocChannelRequest>) -> ByteWriter {
    let (tx, rx) = oneshot::channel();
    assert!(chan_tx.send(AdHocChannelRequest::new(tx)).await.is_ok());
    rx.await
        .expect("Request dropped.")
        .expect("Registering channel failed.")
}

#[tokio::test]
async fn clean_shutdown_after_registration() {
    let (state, (writer, _)) = run_test(|context| async move {
        let TestContext { chan_tx, links_rx } = context;
        let writer = register(&chan_tx).await;
        drop(chan_tx);
        (writer, links_rx)
    })
    .await;

    assert!(state.outputs.is_empty());
    if let Some(reader) = state.reader.as_ref() {
        assert!(byte_channel::are_connected(&writer, reader.get_ref()));
    } else {
        panic!("Reader not registered.");
    }
}

#[tokio::test]
async fn replace_channel() {
    let (state, (_writer1, writer2, _)) = run_test(|context| async move {
        let TestContext { chan_tx, links_rx } = context;
        let writer1 = register(&chan_tx).await;
        let writer2 = register(&chan_tx).await;
        drop(chan_tx);
        (writer1, writer2, links_rx)
    })
    .await;

    assert!(state.outputs.is_empty());
    if let Some(reader) = state.reader.as_ref() {
        assert!(byte_channel::are_connected(&writer2, reader.get_ref()));
    } else {
        panic!("Reader not registered.");
    }
}

type CommandSender = FramedWrite<ByteWriter, AdHocCommandEncoder<WithLenReconEncoder>>;

const ADDRS: &[(Option<&str>, &str, &str)] = &[
    (Some("localhost:8080"), "/node", "lane"),
    (Some("localhost:8080"), "/node", "lane2"),
    (Some("other:8080"), "/node", "lane"),
    (None, "/node", "lane"),
];

fn make_key(i: usize) -> CommanderKey {
    let (host, node, lane) = &ADDRS[i];
    if let Some(host) = host {
        CommanderKey::Remote(host.parse::<SchemeHostPort>().unwrap())
    } else {
        CommanderKey::Local(RelativeAddress::text(node, lane))
    }
}

async fn send_command<T>(tx: &mut CommandSender, target: usize, value: T, overwrite_permitted: bool)
where
    T: StructuralWritable,
{
    let (host, node, lane) = &ADDRS[target];
    let addr = Address::new(*host, node, lane);
    let cmd = AdHocCommand::new(addr, value, overwrite_permitted);
    assert!(tx.send(cmd).await.is_ok());
}

async fn open_link(rx: &mut mpsc::Receiver<LinkRequest>) -> (CommanderKey, ByteReader) {
    match rx.recv().await.expect("Channel dropped.") {
        LinkRequest::Downlink(_) => panic!("Downlink requested."),
        LinkRequest::Commander(CommanderRequest {
            agent_id,
            key,
            promise,
        }) => {
            assert_eq!(agent_id, ID);
            let (tx, rx) = byte_channel::byte_channel(BUFFER_SIZE);
            promise.send(Ok(tx)).expect("Request dropped.");
            (key, rx)
        }
    }
}

type RequestReader = FramedRead<ByteReader, RawRequestMessageDecoder>;

async fn get_msg(reader: &mut RequestReader, target: usize) -> Vec<u8> {
    let RequestMessage {
        origin,
        path,
        envelope,
    } = reader
        .next()
        .await
        .expect("Reader closed.")
        .expect("Reader failed.");
    assert_eq!(origin, ID);
    let (_, node, lane) = &ADDRS[target];
    assert_eq!(&&path.node, node);
    assert_eq!(&&path.lane, lane);
    match envelope {
        Operation::Command(body) => body.into(),
        ow => panic!("Unexpected op: {:?}", ow),
    }
}

fn get_msgs(
    rx: ByteReader,
    key: CommanderKey,
) -> impl Stream<Item = (CommanderKey, RelativeAddress<String>, Vec<u8>)> {
    let reader = RequestReader::new(rx, Default::default());
    unfold((key, reader), |(key, mut reader)| async move {
        let RequestMessage {
            origin,
            path: RelativeAddress { node, lane },
            envelope,
        } = reader.next().await?.expect("Reader failed.");
        assert_eq!(origin, ID);
        let item = match envelope {
            Operation::Command(body) => body.into(),
            ow => panic!("Unexpected op: {:?}", ow),
        };
        let addr = RelativeAddress::new(node.as_str().to_string(), lane.as_str().to_string());
        Some(((key.clone(), addr, item), (key, reader)))
    })
}

async fn expect_msg<T>(reader: &mut RequestReader, target: usize, value: T)
where
    T: StructuralWritable,
{
    let bytes = get_msg(reader, target).await;
    let expected_body = format!("{}", print_recon_compact(&value));
    assert_eq!(&bytes, expected_body.as_bytes());
}

#[tokio::test]
async fn route_single_command() {
    let target = 0;
    let test_value = 5;

    let (state, _) = run_test(|context| async move {
        let TestContext {
            chan_tx,
            mut links_rx,
        } = context;
        let writer = register(&chan_tx).await;
        let mut sender = CommandSender::new(writer, Default::default());

        let recv_task = async move {
            let (key, rx) = open_link(&mut links_rx).await;
            assert_eq!(key, make_key(target));

            let mut channel = RequestReader::new(rx, Default::default());
            expect_msg(&mut channel, target, test_value).await;
            links_rx
        };

        let send_task = async move {
            send_command(&mut sender, target, test_value, true).await;
            sender
        };

        let (links_rx, sender) = join(recv_task, send_task).await;
        drop(chan_tx);
        (sender, links_rx)
    })
    .await;

    let expected_key = make_key(target);
    assert!(state.reader.is_some());
    assert_eq!(state.outputs.len(), 1);
    assert!(state.outputs.contains_key(&expected_key));
}

#[tokio::test]
async fn multiple_commands_same_target() {
    let target = 0;
    let test_values = [5, 6, 7, 8, 9, 10];
    let last_index = test_values.len() - 1;

    let (state, _) = run_test(|context| async move {
        let TestContext {
            chan_tx,
            mut links_rx,
        } = context;
        let writer = register(&chan_tx).await;
        let mut sender = CommandSender::new(writer, Default::default());

        let recv_task = async move {
            let (key, rx) = open_link(&mut links_rx).await;
            assert_eq!(key, make_key(target));
            let mut channel = RequestReader::new(rx, Default::default());
            let mut prev = None;
            loop {
                let body = get_msg(&mut channel, target).await;
                let n = std::str::from_utf8(&body)
                    .expect("Bad UTF")
                    .parse::<i32>()
                    .expect("Invalid integer.");
                if let Some(p) = prev {
                    assert!(n > p);
                }
                if n == test_values[last_index] {
                    break;
                } else {
                    prev = Some(n);
                }
            }
            links_rx
        };

        let send_task = async move {
            for v in test_values {
                send_command(&mut sender, target, v, true).await;
            }
            sender
        };

        let (links_rx, sender) = join(recv_task, send_task).await;
        drop(chan_tx);
        (sender, links_rx)
    })
    .await;

    let expected_key = make_key(target);
    assert!(state.reader.is_some());
    assert_eq!(state.outputs.len(), 1);
    assert!(state.outputs.contains_key(&expected_key));
}

#[tokio::test]
async fn multiple_commands_different_targets() {
    let mut to_send = vec![];
    let mut expected = HashMap::new();
    for (id, (host, node, lane)) in ADDRS.iter().enumerate() {
        let key = if let Some(host) = host {
            CommanderKey::Remote(host.parse::<SchemeHostPort>().unwrap())
        } else {
            CommanderKey::Local(RelativeAddress::text(node, lane))
        };
        let addr = RelativeAddress::new(node.to_string(), lane.to_string());
        let n = rand::thread_rng().gen::<i32>();
        to_send.push((id, n));
        expected.insert((key, addr), n);
    }

    let (state, _) = run_test(|context| async move {
        let TestContext {
            chan_tx,
            mut links_rx,
        } = context;
        let writer = register(&chan_tx).await;
        let mut sender = CommandSender::new(writer, Default::default());

        let mut links = SelectAll::new();

        let recv_task = async move {
            loop {
                let (key, addr, body) = tokio::select! {
                    (key, rx) = open_link(&mut links_rx) => {
                        links.push(get_msgs(rx, key).boxed());
                        continue;
                    }
                    maybe_value = links.next(), if !links.is_empty() => {
                        if let Some(v) = maybe_value {
                            v
                        } else {
                            continue;
                        }
                    }
                };

                let expected_value = expected
                    .remove(&(key, addr))
                    .expect("Message received twice.");
                let n = std::str::from_utf8(&body)
                    .expect("Bad UTF")
                    .parse::<i32>()
                    .expect("Invalid integer.");
                assert_eq!(n, expected_value);

                if expected.is_empty() {
                    break;
                }
            }
            links_rx
        };

        let send_task = async move {
            for (target, v) in to_send {
                send_command(&mut sender, target, v, true).await;
            }
            sender
        };

        let (links_rx, sender) = join(recv_task, send_task).await;
        drop(chan_tx);
        (sender, links_rx)
    })
    .await;

    assert!(state.reader.is_some());
    assert_eq!(state.outputs.len(), 3);
}
