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
use tokio::sync::mpsc;
use tokio_util::codec::{FramedRead, FramedWrite};

use crate::model::lifecycle::{for_value_downlink, ValueDownlinkLifecycle};
use crate::{DownlinkTask, ValueDownlinkModel};

const CHANNEL_SIZE: NonZeroUsize = non_zero_usize!(1024);

struct TestWriter(FramedWrite<ByteWriter, DownlinkNotificationEncoder>);

impl TestWriter {
    fn new(tx: ByteWriter) -> Self {
        TestWriter(FramedWrite::new(tx, DownlinkNotificationEncoder))
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

impl TestWriter {
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
}

async fn run_downlink_task<D, F, Fut>(
    task: D,
    config: DownlinkConfig,
    test_block: F,
) -> Result<Fut::Output, DownlinkTaskError>
where
    D: Downlink,
    F: FnOnce(TestWriter, TestReader) -> Fut,
    Fut: Future,
{
    let path = Path::Local(RelativePath::new("node", "lane"));

    let (in_tx, in_rx) = byte_channel(CHANNEL_SIZE);
    let (out_tx, out_rx) = byte_channel(CHANNEL_SIZE);

    let dl_task = task.run(path, config, in_rx, out_tx);
    let test_body = test_block(TestWriter::new(in_tx), TestReader::new(out_rx));
    let (result, out) = join(dl_task, test_body).await;
    result.map(|_| out)
}

#[derive(Debug, PartialEq, Eq)]
enum Event<T> {
    OnLinked,
    OnSynced(T),
    OnEvent(T),
    OnSet(Option<T>, T),
    OnUnlinked,
}

fn make_lifecycle<T>(tx: mpsc::UnboundedSender<Event<T>>) -> impl ValueDownlinkLifecycle<T>
where
    T: Clone + Send + Sync + 'static,
{
    for_value_downlink::<T>()
        .with(tx)
        .on_linked_blocking(|tx| {
            assert!(tx.send(Event::OnLinked).is_ok());
        })
        .on_synced_blocking(|tx, v| {
            assert!(tx.send(Event::OnSynced(v.clone())).is_ok());
        })
        .on_event_blocking(|tx, v| {
            assert!(tx.send(Event::OnEvent(v.clone())).is_ok());
        })
        .on_set_blocking(|tx, before, after| {
            assert!(tx
                .send(Event::OnSet(before.cloned(), after.clone()))
                .is_ok());
        })
        .on_unlinked_blocking(|tx| {
            assert!(tx.send(Event::OnUnlinked).is_ok());
        })
}

async fn expect_event<T: Eq + std::fmt::Debug>(
    event_rx: &mut mpsc::UnboundedReceiver<Event<T>>,
    expected: Event<T>,
) {
    assert_eq!(event_rx.recv().await, Some(expected))
}

#[tokio::test]
async fn link_downlink() {
    let (event_tx, mut event_rx) = mpsc::unbounded_channel::<Event<i32>>();
    let (_set_tx, set_rx) = mpsc::channel(16);
    let lifecycle = make_lifecycle(event_tx);
    let model = ValueDownlinkModel::new(set_rx, lifecycle);

    let config = DownlinkConfig {
        events_when_not_synced: false,
        terminate_on_unlinked: true,
    };

    let result = run_downlink_task(
        DownlinkTask::new(model),
        config,
        |mut writer, reader| async move {
            let _reader = reader;
            writer.send_value::<i32>(DownlinkNotification::Linked).await;
            expect_event(&mut event_rx, Event::OnLinked).await;
        },
    )
    .await;
    assert!(result.is_ok());
}

#[tokio::test]
async fn invalid_sync_downlink() {
    let (event_tx, mut event_rx) = mpsc::unbounded_channel::<Event<i32>>();
    let (_set_tx, set_rx) = mpsc::channel(16);
    let lifecycle = make_lifecycle(event_tx);
    let model = ValueDownlinkModel::new(set_rx, lifecycle);

    let config = DownlinkConfig {
        events_when_not_synced: false,
        terminate_on_unlinked: true,
    };

    let result = run_downlink_task(
        DownlinkTask::new(model),
        config,
        |mut writer, reader| async move {
            let _reader = reader;
            writer.send_value::<i32>(DownlinkNotification::Linked).await;
            writer.send_value::<i32>(DownlinkNotification::Synced).await;
            expect_event(&mut event_rx, Event::OnLinked).await;
        },
    )
    .await;
    assert!(matches!(result, Err(DownlinkTaskError::SyncedWithNoValue)));
}

#[tokio::test]
async fn sync_downlink() {
    let (event_tx, mut event_rx) = mpsc::unbounded_channel::<Event<i32>>();
    let (_set_tx, set_rx) = mpsc::channel(16);
    let lifecycle = make_lifecycle(event_tx);
    let model = ValueDownlinkModel::new(set_rx, lifecycle);

    let config = DownlinkConfig {
        events_when_not_synced: false,
        terminate_on_unlinked: true,
    };

    let result = run_downlink_task(
        DownlinkTask::new(model),
        config,
        |mut writer, reader| async move {
            let _reader = reader;
            writer.send_value::<i32>(DownlinkNotification::Linked).await;
            writer
                .send_value::<i32>(DownlinkNotification::Event { body: 5 })
                .await;
            writer.send_value::<i32>(DownlinkNotification::Synced).await;
            expect_event(&mut event_rx, Event::OnLinked).await;
            expect_event(&mut event_rx, Event::OnSynced(5)).await;
        },
    )
    .await;
    assert!(result.is_ok());
}

#[tokio::test]
async fn report_events_before_sync() {
    let (event_tx, mut event_rx) = mpsc::unbounded_channel::<Event<i32>>();
    let (_set_tx, set_rx) = mpsc::channel(16);
    let lifecycle = make_lifecycle(event_tx);
    let model = ValueDownlinkModel::new(set_rx, lifecycle);

    let config = DownlinkConfig {
        events_when_not_synced: true,
        terminate_on_unlinked: true,
    };

    let result = run_downlink_task(
        DownlinkTask::new(model),
        config,
        |mut writer, reader| async move {
            let _reader = reader;
            writer.send_value::<i32>(DownlinkNotification::Linked).await;
            writer
                .send_value::<i32>(DownlinkNotification::Event { body: 5 })
                .await;
            writer
                .send_value::<i32>(DownlinkNotification::Event { body: 67 })
                .await;
            expect_event(&mut event_rx, Event::OnLinked).await;
            expect_event(&mut event_rx, Event::OnEvent(5)).await;
            expect_event(&mut event_rx, Event::OnSet(None, 5)).await;
            expect_event(&mut event_rx, Event::OnEvent(67)).await;
            expect_event(&mut event_rx, Event::OnSet(Some(5), 67)).await;
        },
    )
    .await;
    assert!(result.is_ok());
}

#[tokio::test]
async fn report_events_after_sync() {
    let (event_tx, mut event_rx) = mpsc::unbounded_channel::<Event<i32>>();
    let (_set_tx, set_rx) = mpsc::channel(16);
    let lifecycle = make_lifecycle(event_tx);
    let model = ValueDownlinkModel::new(set_rx, lifecycle);

    let config = DownlinkConfig {
        events_when_not_synced: false,
        terminate_on_unlinked: true,
    };

    let result = run_downlink_task(
        DownlinkTask::new(model),
        config,
        |mut writer, reader| async move {
            let _reader = reader;
            writer.send_value::<i32>(DownlinkNotification::Linked).await;
            writer
                .send_value::<i32>(DownlinkNotification::Event { body: 5 })
                .await;
            writer.send_value::<i32>(DownlinkNotification::Synced).await;
            writer
                .send_value::<i32>(DownlinkNotification::Event { body: 67 })
                .await;
            expect_event(&mut event_rx, Event::OnLinked).await;
            expect_event(&mut event_rx, Event::OnSynced(5)).await;
            expect_event(&mut event_rx, Event::OnEvent(67)).await;
            expect_event(&mut event_rx, Event::OnSet(Some(5), 67)).await;
        },
    )
    .await;
    assert!(result.is_ok());
}

#[tokio::test]
async fn terminate_after_unlinked() {
    let (event_tx, mut event_rx) = mpsc::unbounded_channel::<Event<i32>>();
    let (_set_tx, set_rx) = mpsc::channel(16);
    let lifecycle = make_lifecycle(event_tx);
    let model = ValueDownlinkModel::new(set_rx, lifecycle);

    let config = DownlinkConfig {
        events_when_not_synced: false,
        terminate_on_unlinked: true,
    };

    let result = run_downlink_task(
        DownlinkTask::new(model),
        config,
        |mut writer, reader| async move {
            writer.send_value::<i32>(DownlinkNotification::Linked).await;
            writer
                .send_value::<i32>(DownlinkNotification::Event { body: 5 })
                .await;
            writer.send_value::<i32>(DownlinkNotification::Synced).await;
            writer
                .send_value::<i32>(DownlinkNotification::Unlinked)
                .await;
            expect_event(&mut event_rx, Event::OnLinked).await;
            expect_event(&mut event_rx, Event::OnSynced(5)).await;
            expect_event(&mut event_rx, Event::OnUnlinked).await;
            (writer, reader)
        },
    )
    .await;
    assert!(result.is_ok());
}

#[tokio::test]
async fn unlink_discards_value() {
    let (event_tx, mut event_rx) = mpsc::unbounded_channel::<Event<i32>>();
    let (_set_tx, set_rx) = mpsc::channel(16);
    let lifecycle = make_lifecycle(event_tx);
    let model = ValueDownlinkModel::new(set_rx, lifecycle);

    let config = DownlinkConfig {
        events_when_not_synced: false,
        terminate_on_unlinked: false,
    };

    let result = run_downlink_task(
        DownlinkTask::new(model),
        config,
        |mut writer, reader| async move {
            let _reader = reader;
            writer.send_value::<i32>(DownlinkNotification::Linked).await;
            writer
                .send_value::<i32>(DownlinkNotification::Event { body: 5 })
                .await;
            writer.send_value::<i32>(DownlinkNotification::Synced).await;
            writer
                .send_value::<i32>(DownlinkNotification::Unlinked)
                .await;
            writer.send_value::<i32>(DownlinkNotification::Linked).await;
            writer.send_value::<i32>(DownlinkNotification::Synced).await;
            expect_event(&mut event_rx, Event::OnLinked).await;
            expect_event(&mut event_rx, Event::OnSynced(5)).await;
            expect_event(&mut event_rx, Event::OnUnlinked).await;
            expect_event(&mut event_rx, Event::OnLinked).await;
        },
    )
    .await;
    assert!(matches!(result, Err(DownlinkTaskError::SyncedWithNoValue)));
}

#[tokio::test]
async fn relink_downlink() {
    let (event_tx, mut event_rx) = mpsc::unbounded_channel::<Event<i32>>();
    let (_set_tx, set_rx) = mpsc::channel(16);
    let lifecycle = make_lifecycle(event_tx);
    let model = ValueDownlinkModel::new(set_rx, lifecycle);

    let config = DownlinkConfig {
        events_when_not_synced: false,
        terminate_on_unlinked: false,
    };

    let result = run_downlink_task(
        DownlinkTask::new(model),
        config,
        |mut writer, reader| async move {
            let _reader = reader;
            writer.send_value::<i32>(DownlinkNotification::Linked).await;
            writer
                .send_value::<i32>(DownlinkNotification::Event { body: 5 })
                .await;
            writer.send_value::<i32>(DownlinkNotification::Synced).await;
            writer
                .send_value::<i32>(DownlinkNotification::Unlinked)
                .await;
            writer.send_value::<i32>(DownlinkNotification::Linked).await;
            writer
                .send_value::<i32>(DownlinkNotification::Event { body: 7 })
                .await;
            writer.send_value::<i32>(DownlinkNotification::Synced).await;
            expect_event(&mut event_rx, Event::OnLinked).await;
            expect_event(&mut event_rx, Event::OnSynced(5)).await;
            expect_event(&mut event_rx, Event::OnUnlinked).await;
            expect_event(&mut event_rx, Event::OnLinked).await;
            expect_event(&mut event_rx, Event::OnSynced(7)).await;
        },
    )
    .await;
    assert!(result.is_ok());
}

#[tokio::test]
async fn send_on_downlink() {
    let (event_tx, _event_rx) = mpsc::unbounded_channel::<Event<i32>>();
    let (set_tx, set_rx) = mpsc::channel(16);
    let lifecycle = make_lifecycle(event_tx);
    let model = ValueDownlinkModel::new(set_rx, lifecycle);

    let config = DownlinkConfig {
        events_when_not_synced: false,
        terminate_on_unlinked: true,
    };

    let result = run_downlink_task(
        DownlinkTask::new(model),
        config,
        |writer, mut reader| async move {
            let _writer = writer;
            assert!(set_tx.send(12).await.is_ok());
            assert_eq!(reader.recv::<i32>().await, Ok(Some(12)));
        },
    )
    .await;
    assert!(result.is_ok());
}
