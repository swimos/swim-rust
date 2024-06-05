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

use std::{collections::HashMap, num::NonZeroUsize, sync::Arc, time::Duration};

use crate::{
    agent::{
        task::{external_links, AdHocChannelRequest, ExternalLinkRequest},
        CommanderKey, CommanderRequest, DownlinkRequest, LinkRequest,
    },
    downlink::{DownlinkOptions, Io},
};
use bytes::Bytes;
use futures::{
    future::join,
    stream::{unfold, SelectAll},
    Future, SinkExt, Stream, StreamExt, TryStreamExt,
};
use rand::Rng;
use swimos_agent_protocol::encoding::ad_hoc::AdHocCommandEncoder;
use swimos_agent_protocol::AdHocCommand;
use swimos_api::{
    address::{Address, RelativeAddress},
    agent::DownlinkKind,
    error::{DownlinkFailureReason, DownlinkRuntimeError},
};
use swimos_form::write::StructuralWritable;
use swimos_messages::protocol::{Operation, RawRequestMessageDecoder, RequestMessage};
use swimos_net::SchemeHostPort;
use swimos_recon::print_recon_compact;
use swimos_utilities::{
    encoding::BytesStr,
    errors::Recoverable,
    future::{Quantity, RetryStrategy},
    io::byte_channel::{self, ByteReader, ByteWriter},
    non_zero_usize, trigger,
};
use tokio::sync::{mpsc, oneshot};
use tokio_util::codec::{Decoder, FramedRead, FramedWrite};
use uuid::Uuid;

use super::{
    external_links_task, AdHocOutput, AdHocSender, LinksTaskConfig, LinksTaskState, PendingWrites,
    ReportFailed,
};

struct TestContext {
    chan_tx: mpsc::Sender<ExternalLinkRequest>,
    links_rx: mpsc::Receiver<LinkRequest>,
    failures: mpsc::UnboundedReceiver<PendingWrites>,
}

const ID: Uuid = Uuid::from_u128(1);
const CHAN_SIZE: usize = 8;
const BUFFER_SIZE: NonZeroUsize = non_zero_usize!(1024);
const OUTPUT_TIMEOUT: Duration = Duration::from_secs(1);
const TEST_TIMEOUT: Duration = Duration::from_secs(5);

const RETRY_LIMIT: NonZeroUsize = non_zero_usize!(3);
const WITHIN_LIMIT: usize = 1;

async fn run_test<F, Fut>(test_case: F) -> (LinksTaskState, Fut::Output)
where
    F: FnOnce(TestContext) -> Fut,
    Fut: Future,
{
    let (chan_tx, chan_rx) = mpsc::channel(CHAN_SIZE);
    let (links_tx, links_rx) = mpsc::channel(CHAN_SIZE);
    let state = LinksTaskState::new(links_tx);
    let config = LinksTaskConfig {
        buffer_size: BUFFER_SIZE,
        retry_strategy: RetryStrategy::interval(
            Duration::from_millis(100),
            Quantity::Finite(RETRY_LIMIT),
        ),
        timeout_delay: OUTPUT_TIMEOUT,
    };
    let (fail_tx, fail_rx) = mpsc::unbounded_channel();
    let failure_report = FailureReport::new(fail_tx);
    let task = external_links_task(ID, chan_rx, state, config, Some(failure_report));
    let context = TestContext {
        chan_tx,
        links_rx,
        failures: fail_rx,
    };
    let test_task = test_case(context);
    tokio::time::timeout(TEST_TIMEOUT, join(task, test_task))
        .await
        .expect("Test timed out.")
}

#[tokio::test]
async fn clean_shutdown_no_registration() {
    let (state, _) = run_test(|context| async move {
        let TestContext {
            chan_tx, links_rx, ..
        } = context;
        drop(chan_tx);
        links_rx
    })
    .await;

    assert!(state.outputs.is_empty());
    assert!(state.reader.is_none());
}

async fn register(chan_tx: &mpsc::Sender<ExternalLinkRequest>) -> ByteWriter {
    let (tx, rx) = oneshot::channel();
    assert!(chan_tx
        .send(ExternalLinkRequest::AdHoc(AdHocChannelRequest::new(tx)))
        .await
        .is_ok());
    rx.await
        .expect("Request dropped.")
        .expect("Registering channel failed.")
}

#[tokio::test]
async fn clean_shutdown_after_registration() {
    let (state, (writer, _)) = run_test(|context| async move {
        let TestContext {
            chan_tx, links_rx, ..
        } = context;
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
        let TestContext {
            chan_tx, links_rx, ..
        } = context;
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

type CommandSender = FramedWrite<ByteWriter, AdHocCommandEncoder>;

const ADDRS: &[(Option<&str>, &str, &str)] = &[
    (Some("ws://localhost:8080"), "/node", "lane"),
    (Some("ws://localhost:8080"), "/node", "lane2"),
    (Some("ws://other:8080"), "/node", "lane"),
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
    open_link_with_failures(rx, 0).await
}

async fn open_link_with_failures(
    rx: &mut mpsc::Receiver<LinkRequest>,
    failures: usize,
) -> (CommanderKey, ByteReader) {
    let mut k = None;
    for _ in 0..failures {
        match rx.recv().await.expect("Channel dropped.") {
            LinkRequest::Downlink(_) => panic!("Downlink requested."),
            LinkRequest::Commander(CommanderRequest {
                agent_id,
                key,
                promise,
            }) => {
                assert_eq!(agent_id, ID);
                let io_err = std::io::Error::from(std::io::ErrorKind::ConnectionReset);
                promise
                    .send(Err(DownlinkRuntimeError::DownlinkConnectionFailed(
                        DownlinkFailureReason::ConnectionFailed(Arc::new(io_err)),
                    )))
                    .expect("Request dropped.");
                if let Some(k) = &k {
                    assert_eq!(&key, k);
                }
                k = Some(key);
            }
        }
    }
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
            if let Some(k) = &k {
                assert_eq!(&key, k);
            }
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
            ..
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
            ..
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
            ..
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

#[tokio::test(start_paused = true)]
async fn drop_output_on_timeout() {
    let target = 0;
    let test_value = 5;

    let (state, _) = run_test(|context| async move {
        let TestContext {
            chan_tx,
            mut links_rx,
            ..
        } = context;
        let writer = register(&chan_tx).await;
        let mut sender = CommandSender::new(writer, Default::default());

        let recv_task = async move {
            let (key, rx) = open_link(&mut links_rx).await;
            assert_eq!(key, make_key(target));

            let mut channel = RequestReader::new(rx, Default::default());
            //Consume the output.
            assert!(matches!(channel.next().await, Some(Ok(_))));
            //Wait for the output to be dropped (after the runtime auto advances time triggering the timeout).
            assert!(channel.next().await.is_none());
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

    assert!(state.reader.is_some());
    assert!(state.outputs.is_empty());
}

#[test]
fn output_replace_writer_no_data() {
    let mut output = super::AdHocOutput::new(ID, RetryStrategy::none());
    assert!(output.write().is_none());

    check_no_data(&mut output);
}

#[tokio::test]
async fn output_replace_writer_pending_record() {
    let (tx, rx) = byte_channel::byte_channel(BUFFER_SIZE);
    let mut output = external_links::AdHocOutput::new(ID, RetryStrategy::none());

    output.append(RelativeAddress::text("/node", "lane"), b"content", true);
    assert!(output.write().is_none());

    let sender = external_links::AdHocSender::new(tx);
    output.replace_writer(sender);

    let fut = output.write().expect("Writer should be scheduled.");

    let requests = run_write_fut(fut, rx).await;

    match requests.as_slice() {
        [RequestMessage {
            origin,
            path: RelativeAddress { node, lane },
            envelope: Operation::Command(body),
        }] => {
            assert_eq!(*origin, ID);
            assert_eq!(node, "/node");
            assert_eq!(lane, "lane");
            assert_eq!(body.as_ref(), b"content");
        }
        ow => panic!("Unexpected responses: {:?}", ow),
    }
    check_no_data(&mut output);
}

fn check_no_data(output: &mut AdHocOutput) {
    let (tx, _rx) = byte_channel::byte_channel(BUFFER_SIZE);
    let sender = super::AdHocSender::new(tx);
    output.replace_writer(sender);
    assert!(output.write().is_none());
}

async fn run_write_fut<Fut>(f: Fut, rx: ByteReader) -> Vec<RequestMessage<BytesStr, Bytes>>
where
    Fut: Future<Output = Result<AdHocSender, std::io::Error>>,
{
    let write = async move {
        let writer = f.await.expect("Write failed");
        drop(writer);
    };
    let read = async move {
        let reader = RequestReader::new(rx, Default::default());
        reader.try_collect::<Vec<_>>().await.expect("Read failed.")
    };
    join(write, read).await.1
}

#[tokio::test]
async fn output_overwrite_record() {
    let (tx, rx) = byte_channel::byte_channel(BUFFER_SIZE);
    let mut output = external_links::AdHocOutput::new(ID, RetryStrategy::none());

    output.append(RelativeAddress::text("/node", "lane"), b"content1", true);
    assert!(output.write().is_none());
    output.append(RelativeAddress::text("/node", "lane"), b"content2", true);
    assert!(output.write().is_none());

    let sender = external_links::AdHocSender::new(tx);
    output.replace_writer(sender);

    let fut = output.write().expect("Writer should be scheduled.");

    let requests = run_write_fut(fut, rx).await;

    match requests.as_slice() {
        [RequestMessage {
            origin,
            path: RelativeAddress { node, lane },
            envelope: Operation::Command(body),
        }] => {
            assert_eq!(*origin, ID);
            assert_eq!(node, "/node");
            assert_eq!(lane, "lane");
            assert_eq!(body.as_ref(), b"content2");
        }
        ow => panic!("Unexpected responses: {:?}", ow),
    }
    check_no_data(&mut output);
}

#[tokio::test]
async fn output_queue_records() {
    let (tx, rx) = byte_channel::byte_channel(BUFFER_SIZE);
    let mut output = external_links::AdHocOutput::new(ID, RetryStrategy::none());

    output.append(RelativeAddress::text("/node", "lane"), b"content1", false);
    assert!(output.write().is_none());
    output.append(RelativeAddress::text("/node", "lane"), b"content2", false);
    assert!(output.write().is_none());
    output.append(RelativeAddress::text("/node", "lane"), b"content3", false);
    assert!(output.write().is_none());

    let sender = external_links::AdHocSender::new(tx);
    output.replace_writer(sender);

    let fut = output.write().expect("Writer should be scheduled.");

    let requests = run_write_fut(fut, rx).await;

    match requests.as_slice() {
        [r1, r2, r3] => {
            if let RequestMessage {
                origin,
                path: RelativeAddress { node, lane },
                envelope: Operation::Command(body),
            } = r1
            {
                assert_eq!(*origin, ID);
                assert_eq!(node, "/node");
                assert_eq!(lane, "lane");
                assert_eq!(body.as_ref(), b"content1");
            } else {
                panic!("Incorrect message kind.");
            }

            if let RequestMessage {
                origin,
                path: RelativeAddress { node, lane },
                envelope: Operation::Command(body),
            } = r2
            {
                assert_eq!(*origin, ID);
                assert_eq!(node, "/node");
                assert_eq!(lane, "lane");
                assert_eq!(body.as_ref(), b"content2");
            } else {
                panic!("Incorrect message kind.");
            }

            if let RequestMessage {
                origin,
                path: RelativeAddress { node, lane },
                envelope: Operation::Command(body),
            } = r3
            {
                assert_eq!(*origin, ID);
                assert_eq!(node, "/node");
                assert_eq!(lane, "lane");
                assert_eq!(body.as_ref(), b"content3");
            } else {
                panic!("Incorrect message kind.");
            }
        }
        ow => panic!("Unexpected responses: {:?}", ow),
    }
    check_no_data(&mut output);
}

#[tokio::test]
async fn output_multiple_targets() {
    let (tx, rx) = byte_channel::byte_channel(BUFFER_SIZE);
    let mut output = external_links::AdHocOutput::new(ID, RetryStrategy::none());

    output.append(RelativeAddress::text("/node", "lane"), b"content1", true);
    assert!(output.write().is_none());
    output.append(RelativeAddress::text("/node", "lane2"), b"content2", true);
    assert!(output.write().is_none());
    output.append(RelativeAddress::text("/node2", "lane"), b"content3", true);
    assert!(output.write().is_none());

    let sender = external_links::AdHocSender::new(tx);
    output.replace_writer(sender);

    let fut = output.write().expect("Writer should be scheduled.");
    let requests = run_write_fut(fut, rx).await;
    assert_eq!(requests.len(), 3);
    let requests = requests
        .into_iter()
        .map(|r| {
            let key = RelativeAddress::new(r.path.node.to_string(), r.path.lane.to_string());
            (key, r)
        })
        .collect::<HashMap<_, _>>();

    let r1 = &requests[&RelativeAddress::new("/node".to_string(), "lane".to_string())];
    if let RequestMessage {
        origin,
        envelope: Operation::Command(body),
        ..
    } = r1
    {
        assert_eq!(*origin, ID);
        assert_eq!(body.as_ref(), b"content1");
    } else {
        panic!("Incorrect message kind.");
    }

    let r2 = &requests[&RelativeAddress::new("/node".to_string(), "lane2".to_string())];
    if let RequestMessage {
        origin,
        envelope: Operation::Command(body),
        ..
    } = r2
    {
        assert_eq!(*origin, ID);
        assert_eq!(body.as_ref(), b"content2");
    } else {
        panic!("Incorrect message kind.");
    }

    let r3 = &requests[&RelativeAddress::new("/node2".to_string(), "lane".to_string())];
    if let RequestMessage {
        origin,
        envelope: Operation::Command(body),
        ..
    } = r3
    {
        assert_eq!(*origin, ID);
        assert_eq!(body.as_ref(), b"content3");
    } else {
        panic!("Incorrect message kind.");
    }

    check_no_data(&mut output);
}

#[tokio::test(start_paused = true)]
async fn output_never_times_out_pending_write() {
    let (tx, _rx) = byte_channel::byte_channel(BUFFER_SIZE);

    let timeout = Duration::from_secs(30);
    let mut output = external_links::AdHocOutput::new(ID, RetryStrategy::none());

    //New output that has never written.

    assert!(!output.timed_out(timeout));

    tokio::time::advance(timeout + Duration::from_secs(1)).await;
    assert!(!output.timed_out(timeout));

    let sender = external_links::AdHocSender::new(tx);
    output.replace_writer(sender);

    output.append(RelativeAddress::text("/node", "lane"), b"content", true);

    //Output with pending write.

    let fut = output.write();
    assert!(fut.is_some());

    assert!(!output.timed_out(timeout));

    tokio::time::advance(timeout + Duration::from_secs(1)).await;
    assert!(!output.timed_out(timeout));
}

#[tokio::test(start_paused = true)]
async fn output_times_out() {
    let (tx, _rx) = byte_channel::byte_channel(BUFFER_SIZE);

    let timeout = Duration::from_secs(30);
    let mut output = external_links::AdHocOutput::new(ID, RetryStrategy::none());

    let sender = external_links::AdHocSender::new(tx);
    output.replace_writer(sender);

    //New output that has never written.

    tokio::time::advance(timeout + Duration::from_secs(1)).await;

    assert!(output.timed_out(timeout));

    //Output with pending write.

    output.append(RelativeAddress::text("/node", "lane"), b"content", true);
    assert!(!output.timed_out(timeout));

    let fut = output.write().expect("Write should be staged.");
    drop(fut);

    let (tx, _rx) = byte_channel::byte_channel(BUFFER_SIZE);
    let sender = external_links::AdHocSender::new(tx);
    output.replace_writer(sender);

    assert!(!output.timed_out(timeout));

    tokio::time::advance(timeout + Duration::from_secs(1)).await;
    assert!(output.timed_out(timeout));
}

#[tokio::test]
async fn route_single_command_with_retry() {
    let target = 0;
    let test_value = 5;

    let (state, _) = run_test(|context| async move {
        let TestContext {
            chan_tx,
            mut links_rx,
            ..
        } = context;
        let writer = register(&chan_tx).await;
        let mut sender = CommandSender::new(writer, Default::default());

        let recv_task = async move {
            let (key, rx) = open_link_with_failures(&mut links_rx, WITHIN_LIMIT).await;
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

#[derive(Debug, Clone)]
struct FailureReport {
    inner: Option<mpsc::UnboundedSender<PendingWrites>>,
}

impl FailureReport {
    fn new(tx: mpsc::UnboundedSender<PendingWrites>) -> Self {
        FailureReport { inner: Some(tx) }
    }
}

impl ReportFailed for FailureReport {
    fn failed(&mut self, pending: PendingWrites) {
        let FailureReport { inner } = self;
        if let Some(tx) = inner {
            if tx.send(pending).is_err() {
                *inner = None;
            }
        }
    }
}

async fn fail_link_fatal(
    rx: &mut mpsc::Receiver<LinkRequest>,
    mut failures: mpsc::UnboundedReceiver<PendingWrites>,
) -> PendingWrites {
    match rx.recv().await.expect("Channel dropped.") {
        LinkRequest::Downlink(_) => panic!("Downlink requested."),
        LinkRequest::Commander(CommanderRequest {
            agent_id, promise, ..
        }) => {
            assert_eq!(agent_id, ID);
            promise
                .send(Err(DownlinkRuntimeError::DownlinkConnectionFailed(
                    DownlinkFailureReason::InvalidUrl, //Fatal error.
                )))
                .expect("Request dropped.");
        }
    }

    tokio::select! {
        _ = rx.recv() => panic!("Unexpected retry."),
        writes = failures.recv() => writes.expect("Failure channel dropped.")
    }
}

#[tokio::test]
async fn route_single_command_fatal_error() {
    let target = 0;
    let test_value = 5;

    let (state, pending) = run_test(|context| async move {
        let TestContext {
            chan_tx,
            mut links_rx,
            failures,
        } = context;
        let writer = register(&chan_tx).await;
        let mut sender = CommandSender::new(writer, Default::default());

        let recv_task = async move {
            let pending = fail_link_fatal(&mut links_rx, failures).await;
            (links_rx, pending)
        };

        let send_task = async move {
            send_command(&mut sender, target, test_value, true).await;
            sender
        };

        let ((_, pending), _) = join(recv_task, send_task).await;
        drop(chan_tx);
        pending
    })
    .await;
    check_pending_single(pending, target, test_value);

    assert!(state.reader.is_some());
    assert!(state.outputs.is_empty());
}

fn check_pending_single(mut pending: PendingWrites, target: usize, expected_value: i32) {
    let (_, node, lane) = &ADDRS[target];
    let expected = RelativeAddress::new(*node, *lane);
    match pending.as_mut_slice() {
        [(addr, buffer)] => {
            assert_eq!(*addr, expected);
            let mut decoder = RawRequestMessageDecoder;
            let RequestMessage {
                origin,
                path,
                envelope,
            } = decoder
                .decode_eof(buffer)
                .expect("Decoding failed.")
                .expect("Incomplete record.");
            assert_eq!(origin, ID);
            assert_eq!(path, expected);
            if let Operation::Command(body) = envelope {
                let value = std::str::from_utf8(body.as_ref())
                    .expect("Invalid UTF8")
                    .parse::<i32>()
                    .expect("Invalid integer.");
                assert_eq!(value, expected_value);
            } else {
                panic!("Unexpected envelope: {:?}", envelope);
            }
        }
        _ => panic!("Expected exactly one record."),
    }
}

async fn fail_link_non_fatal(
    rx: &mut mpsc::Receiver<LinkRequest>,
    mut failures: mpsc::UnboundedReceiver<PendingWrites>,
) -> (usize, PendingWrites) {
    let mut attempts = 0;
    let pending = loop {
        let request = tokio::select! {
            maybe_request = rx.recv() => maybe_request.expect("Request channel dropped."),
            writes = failures.recv() => break writes.expect("Failure channel dropped."),
        };
        match request {
            LinkRequest::Downlink(_) => panic!("Downlink requested."),
            LinkRequest::Commander(CommanderRequest {
                agent_id, promise, ..
            }) => {
                attempts += 1;
                assert_eq!(agent_id, ID);
                promise
                    .send(Err(DownlinkRuntimeError::DownlinkConnectionFailed(
                        DownlinkFailureReason::DownlinkStopped, //Non-fatal error.
                    )))
                    .expect("Request dropped.");
            }
        }
    };
    (attempts, pending)
}

#[tokio::test]
async fn route_single_command_repeated_errors() {
    let target = 0;
    let test_value = 5;

    let (state, (attempts, pending)) = run_test(|context| async move {
        let TestContext {
            chan_tx,
            mut links_rx,
            failures,
        } = context;
        let writer = register(&chan_tx).await;
        let mut sender = CommandSender::new(writer, Default::default());

        let recv_task = async move {
            let (attempts, pending) = fail_link_non_fatal(&mut links_rx, failures).await;
            (links_rx, attempts, pending)
        };

        let send_task = async move {
            send_command(&mut sender, target, test_value, true).await;
            sender
        };

        let ((_, attempts, pending), _) = join(recv_task, send_task).await;
        drop(chan_tx);
        (attempts, pending)
    })
    .await;
    assert_eq!(attempts, RETRY_LIMIT.get() + 1);
    check_pending_single(pending, target, test_value);

    assert!(state.reader.is_some());
    assert!(state.outputs.is_empty());
}

async fn provide_downlink(rx: &mut mpsc::Receiver<LinkRequest>, target: usize) -> Io {
    provide_downlink_with_failures(rx, 0, target).await
}

async fn provide_downlink_with_failures(
    rx: &mut mpsc::Receiver<LinkRequest>,
    failures: usize,
    target: usize,
) -> Io {
    let (host, node, lane) = &ADDRS[target];
    let expected_remote = host.map(|h| h.parse::<SchemeHostPort>().unwrap());
    let expected_address = RelativeAddress::text(node, lane);
    for _ in 0..failures {
        match rx.recv().await.expect("Channel dropped.") {
            LinkRequest::Downlink(DownlinkRequest {
                remote,
                address,
                promise,
                ..
            }) => {
                assert_eq!(remote, expected_remote);
                assert_eq!(address, expected_address);
                let io_err = std::io::Error::from(std::io::ErrorKind::ConnectionReset);
                promise
                    .send(Err(DownlinkRuntimeError::DownlinkConnectionFailed(
                        DownlinkFailureReason::ConnectionFailed(Arc::new(io_err)),
                    )))
                    .expect("Request dropped.");
            }
            LinkRequest::Commander(_) => panic!("Command channel requested."),
        }
    }
    match rx.recv().await.expect("Channel dropped.") {
        LinkRequest::Downlink(DownlinkRequest {
            remote,
            address,
            promise,
            ..
        }) => {
            assert_eq!(remote, expected_remote);
            assert_eq!(address, expected_address);
            let (in_tx, in_rx) = byte_channel::byte_channel(BUFFER_SIZE);
            let (out_tx, out_rx) = byte_channel::byte_channel(BUFFER_SIZE);
            promise.send(Ok((in_tx, out_rx))).expect("Request dropped.");
            (out_tx, in_rx)
        }
        LinkRequest::Commander(_) => panic!("Command channel requested."),
    }
}

async fn open_downlink(
    chan_tx: &mpsc::Sender<ExternalLinkRequest>,
    target: usize,
) -> Result<Io, DownlinkRuntimeError> {
    let (host, node, lane) = &ADDRS[target];
    let remote = host.map(|h| h.parse::<SchemeHostPort>().unwrap());
    let address = RelativeAddress::text(node, lane);

    let (promise_tx, promise_rx) = oneshot::channel();

    let req = DownlinkRequest::new(
        remote,
        address,
        DownlinkKind::Value,
        DownlinkOptions::DEFAULT,
        promise_tx,
    );
    chan_tx
        .send(ExternalLinkRequest::Downlink(req))
        .await
        .expect("Request channel closed.");
    promise_rx.await.expect("Request dropped.")
}

#[tokio::test]
async fn open_downlink_no_failures() {
    let target = 0;

    run_test(|context| async move {
        let TestContext {
            chan_tx,
            mut links_rx,
            ..
        } = context;

        let recv_task = async move {
            let io = provide_downlink(&mut links_rx, target).await;
            (links_rx, io)
        };

        let send_task = open_downlink(&chan_tx, target);

        let ((links_rx, (sock_tx, sock_rx)), result) = join(recv_task, send_task).await;

        let (dl_tx, dl_rx) = result.expect("Downlink failed.");
        assert!(byte_channel::are_connected(&dl_tx, &sock_rx));
        assert!(byte_channel::are_connected(&sock_tx, &dl_rx));

        drop(chan_tx);
        links_rx
    })
    .await;
}

#[tokio::test]
async fn open_downlink_recover_after_failures() {
    let target = 0;

    run_test(|context| async move {
        let TestContext {
            chan_tx,
            mut links_rx,
            ..
        } = context;

        let recv_task = async move {
            let io = provide_downlink_with_failures(&mut links_rx, WITHIN_LIMIT, target).await;
            (links_rx, io)
        };

        let send_task = open_downlink(&chan_tx, target);

        let ((links_rx, (sock_tx, sock_rx)), result) = join(recv_task, send_task).await;

        let (dl_tx, dl_rx) = result.expect("Downlink failed.");
        assert!(byte_channel::are_connected(&dl_tx, &sock_rx));
        assert!(byte_channel::are_connected(&sock_tx, &dl_rx));

        drop(chan_tx);
        links_rx
    })
    .await;
}

async fn fail_downlink_fatal(rx: &mut mpsc::Receiver<LinkRequest>, done: trigger::Receiver) {
    match rx.recv().await.expect("Channel dropped.") {
        LinkRequest::Downlink(DownlinkRequest { promise, .. }) => {
            promise
                .send(Err(DownlinkRuntimeError::DownlinkConnectionFailed(
                    DownlinkFailureReason::InvalidUrl, //Fatal error.
                )))
                .expect("Request dropped.");
        }
        LinkRequest::Commander(_) => panic!("Command channel requested."),
    }

    tokio::select! {
        _ = rx.recv() => panic!("Unexpected retry."),
        _ = done => {},
    }
}

#[tokio::test]
async fn open_downlink_fatal_error() {
    let target = 0;

    run_test(|context| async move {
        let TestContext {
            chan_tx,
            mut links_rx,
            ..
        } = context;

        let (stop_tx, stop_rx) = trigger::trigger();

        let recv_task = fail_downlink_fatal(&mut links_rx, stop_rx);
        let chan_tx_ref = &chan_tx;
        let send_task = async move {
            let result = open_downlink(chan_tx_ref, target).await;
            stop_tx.trigger();
            result
        };

        let (_, result) = join(recv_task, send_task).await;

        assert!(matches!(result, Err(e) if e.is_fatal()));
        drop(chan_tx);
        links_rx
    })
    .await;
}

async fn fail_downlink_non_fatal(
    rx: &mut mpsc::Receiver<LinkRequest>,
    mut stop_rx: trigger::Receiver,
) -> usize {
    let mut attempts = 0;
    loop {
        let request = tokio::select! {
            maybe_request = rx.recv() => maybe_request.expect("Request channel dropped."),
            _ = &mut stop_rx => break,
        };
        match request {
            LinkRequest::Downlink(DownlinkRequest { promise, .. }) => {
                attempts += 1;
                promise
                    .send(Err(DownlinkRuntimeError::DownlinkConnectionFailed(
                        DownlinkFailureReason::DownlinkStopped, //Non-fatal error.
                    )))
                    .expect("Request dropped.");
            }
            LinkRequest::Commander(_) => panic!("Command channel requested."),
        }
    }
    attempts
}

#[tokio::test]
async fn open_downlink_retries_exceeded() {
    let target = 0;

    run_test(|context| async move {
        let TestContext {
            chan_tx,
            mut links_rx,
            ..
        } = context;

        let (stop_tx, stop_rx) = trigger::trigger();

        let recv_task = fail_downlink_non_fatal(&mut links_rx, stop_rx);
        let chan_tx_ref = &chan_tx;
        let send_task = async move {
            let result = open_downlink(chan_tx_ref, target).await;
            stop_tx.trigger();
            result
        };

        let (attempts, result) = join(recv_task, send_task).await;
        assert_eq!(attempts, RETRY_LIMIT.get() + 1);

        assert!(matches!(result, Err(e) if !e.is_fatal()));
        drop(chan_tx);
        links_rx
    })
    .await;
}
