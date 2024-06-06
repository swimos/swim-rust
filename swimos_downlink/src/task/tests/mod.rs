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

use std::future::Future;
use std::num::NonZeroUsize;

use futures::future::join;
use futures::{SinkExt, StreamExt};
use tokio::time::{timeout, Duration};
use tokio_util::codec::{FramedRead, FramedWrite};

use swimos_agent_protocol::encoding::downlink::{
    DownlinkNotificationEncoder, DownlinkOperationDecoder,
};
use swimos_agent_protocol::{DownlinkNotification, DownlinkOperation};
use swimos_api::{
    downlink::{Downlink, DownlinkConfig},
    error::DownlinkTaskError,
};
use swimos_form::structural::{read::recognizer::RecognizerReadable, write::StructuralWritable};
use swimos_model::address::Address;
use swimos_recon::parser::parse_recognize;
use swimos_recon::print_recon_compact;
use swimos_utilities::{
    io::byte_channel::{byte_channel, ByteReader, ByteWriter},
    non_zero_usize,
};

mod event;
mod map;
mod value;

const CHANNEL_SIZE: NonZeroUsize = non_zero_usize!(1024);
const TEST_TIMEOUT: Duration = Duration::from_secs(5);

struct TestValueWriter(FramedWrite<ByteWriter, DownlinkNotificationEncoder>);

impl TestValueWriter {
    fn new(tx: ByteWriter) -> Self {
        TestValueWriter(FramedWrite::new(tx, DownlinkNotificationEncoder))
    }
}

struct TestReader(FramedRead<ByteReader, DownlinkOperationDecoder>);

impl TestReader {
    fn new(rx: ByteReader) -> Self {
        TestReader(FramedRead::new(rx, DownlinkOperationDecoder))
    }
}

#[derive(Debug, PartialEq, Eq)]
struct ReadFailed;

impl From<std::io::Error> for ReadFailed {
    fn from(_: std::io::Error) -> Self {
        ReadFailed
    }
}

impl From<std::str::Utf8Error> for ReadFailed {
    fn from(_: std::str::Utf8Error) -> Self {
        ReadFailed
    }
}

impl TestReader {
    async fn recv<T: RecognizerReadable>(&mut self) -> Result<Option<T>, ReadFailed> {
        let TestReader(inner) = self;
        let op = inner.next().await.transpose()?;
        if let Some(DownlinkOperation { body }) = op {
            let body_str = std::str::from_utf8(body.as_ref())?;
            if let Ok(v) = parse_recognize(body_str, false) {
                Ok(Some(v))
            } else {
                Err(ReadFailed)
            }
        } else {
            Ok(None)
        }
    }
}

const BAD_UTF8: &[u8] = &[0xf0, 0x28, 0x8c, 0x28, 0x00, 0x00, 0x00];

impl TestValueWriter {
    async fn send_value<T>(&mut self, notification: DownlinkNotification<T>)
    where
        T: StructuralWritable,
    {
        let TestValueWriter(writer) = self;
        let raw = match notification {
            DownlinkNotification::Linked => DownlinkNotification::Linked,
            DownlinkNotification::Synced => DownlinkNotification::Synced,
            DownlinkNotification::Unlinked => DownlinkNotification::Unlinked,
            DownlinkNotification::Event { body } => {
                let body_bytes = format!("{}", print_recon_compact(&body)).into_bytes();
                DownlinkNotification::Event { body: body_bytes }
            }
        };
        assert!(writer.send(raw).await.is_ok());
    }

    async fn send_corrupted_frame(&mut self) {
        let TestValueWriter(writer) = self;
        let bad = DownlinkNotification::Event { body: BAD_UTF8 };
        assert!(writer.send(bad).await.is_ok());
    }
}

async fn run_value_downlink_task<D, F, Fut>(
    task: D,
    config: DownlinkConfig,
    test_block: F,
) -> Result<Fut::Output, DownlinkTaskError>
where
    D: Downlink,
    F: FnOnce(TestValueWriter, TestReader) -> Fut,
    Fut: Future,
{
    run_downlink_task(task, config, test_block, TestValueWriter::new).await
}

async fn run_downlink_task<D, F, Fut, Fac, W>(
    task: D,
    config: DownlinkConfig,
    test_block: F,
    make_writer: Fac,
) -> Result<Fut::Output, DownlinkTaskError>
where
    D: Downlink,
    F: FnOnce(W, TestReader) -> Fut,
    Fut: Future,
    Fac: FnOnce(ByteWriter) -> W,
{
    let path = Address::text(None, "node", "lane");

    let (in_tx, in_rx) = byte_channel(CHANNEL_SIZE);
    let (out_tx, out_rx) = byte_channel(CHANNEL_SIZE);

    let dl_task = task.run(path, config, in_rx, out_tx);
    let test_body = test_block(make_writer(in_tx), TestReader::new(out_rx));
    let (result, out) = timeout(TEST_TIMEOUT, join(dl_task, test_body))
        .await
        .expect("Test timed out.");
    result.map(|_| out)
}
