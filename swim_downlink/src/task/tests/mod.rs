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

use std::future::Future;
use std::num::NonZeroUsize;

use futures::future::join;
use futures::{SinkExt, StreamExt};
use swim_api::protocol::downlink::SimpleMessageEncoder;
use swim_api::protocol::map::{MapMessage, MapMessageEncoder, MapOperationEncoder};
use swim_api::{
    downlink::{Downlink, DownlinkConfig},
    error::DownlinkTaskError,
    protocol::downlink::{
        DownlinkNotification, DownlinkNotificationEncoder, DownlinkOperation,
        DownlinkOperationDecoder,
    },
};
use swim_form::structural::{read::recognizer::RecognizerReadable, write::StructuralWritable};
use swim_model::path::{Path, RelativePath};
use swim_recon::parser::{parse_recognize, Span};
use swim_recon::printer::print_recon_compact;
use swim_utilities::{
    algebra::non_zero_usize,
    io::byte_channel::{byte_channel, ByteReader, ByteWriter},
};
use tokio::time::{timeout, Duration};
use tokio_util::codec::{FramedRead, FramedWrite};

mod event;
mod map;
mod value;

const CHANNEL_SIZE: NonZeroUsize = non_zero_usize!(1024);
const TEST_TIMEOUT: Duration = Duration::from_secs(5);

struct TestWriter<E>(FramedWrite<ByteWriter, DownlinkNotificationEncoder<E>>);

impl<E> TestWriter<E> {
    fn new(tx: ByteWriter, body_encoder: E) -> Self {
        TestWriter(FramedWrite::new(
            tx,
            DownlinkNotificationEncoder::new(body_encoder),
        ))
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
            let input = Span::new(body_str);
            if let Ok(v) = parse_recognize(input, false) {
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

impl TestWriter<MapMessageEncoder<MapOperationEncoder>> {
    async fn send_value<K, V>(&mut self, notification: DownlinkNotification<MapMessage<K, V>>)
    where
        K: StructuralWritable,
        V: StructuralWritable,
    {
        let TestWriter(writer) = self;
        let raw = match notification {
            DownlinkNotification::Linked => DownlinkNotification::Linked,
            DownlinkNotification::Synced => DownlinkNotification::Synced,
            DownlinkNotification::Unlinked => DownlinkNotification::Unlinked,
            DownlinkNotification::Event { body } => DownlinkNotification::Event { body },
        };

        assert!(writer.send(raw).await.is_ok());
    }
}

impl TestWriter<SimpleMessageEncoder> {
    async fn send_value<T>(&mut self, notification: DownlinkNotification<T>)
    where
        T: StructuralWritable,
    {
        let TestWriter(writer) = self;
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
        let TestWriter(writer) = self;
        let bad = DownlinkNotification::Event { body: BAD_UTF8 };
        assert!(writer.send(bad).await.is_ok());
    }
}

async fn run_downlink_task<D, F, Fut, E>(
    task: D,
    config: DownlinkConfig,
    test_block: F,
    body_encoder: E,
) -> Result<Fut::Output, DownlinkTaskError>
where
    D: Downlink,
    F: FnOnce(TestWriter<E>, TestReader) -> Fut,
    Fut: Future,
{
    let path = Path::Local(RelativePath::new("node", "lane"));

    let (in_tx, in_rx) = byte_channel(CHANNEL_SIZE);
    let (out_tx, out_rx) = byte_channel(CHANNEL_SIZE);

    let dl_task = task.run(path, config, in_rx, out_tx);
    let test_body = test_block(
        TestWriter::new(in_tx, body_encoder),
        TestReader::new(out_rx),
    );
    let (result, out) = timeout(TEST_TIMEOUT, join(dl_task, test_body))
        .await
        .expect("Test timed out.");
    result.map(|_| out)
}
