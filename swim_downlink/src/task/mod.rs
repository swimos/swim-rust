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

use futures::future::{join, BoxFuture};
use futures::{FutureExt, Sink, SinkExt, StreamExt};
use swim_api::downlink::{Downlink, DownlinkConfig, DownlinkKind};
use swim_api::error::DownlinkTaskError;
use swim_api::lifecycle::downlink::ValueDownlinkLifecycle;
use swim_api::protocol::downlink::{
    DownlinkNotifiationDecoder, DownlinkNotification, DownlinkOperation, DownlinkOperationEncoder,
};
use swim_form::structural::read::recognizer::RecognizerReadable;
use swim_form::Form;
use swim_model::path::AbsolutePath;
use swim_utilities::future::immediate_or_join;
use swim_utilities::io::byte_channel::{ByteReader, ByteWriter};
use tokio::sync::mpsc;
use tokio_util::codec::{FramedRead, FramedWrite};

use crate::ValueDownlinkModel;

pub struct DownlinkTask<Model>(Model);

impl<T, LC> Downlink for DownlinkTask<ValueDownlinkModel<T, LC>>
where
    T: Form + Send + Sync + 'static,
    <T as RecognizerReadable>::Rec: Send,
    LC: for<'a> ValueDownlinkLifecycle<'a, T> + 'static,
{
    fn kind(&self) -> DownlinkKind {
        DownlinkKind::Value
    }

    fn run(
        self,
        path: AbsolutePath,
        config: DownlinkConfig,
        input: ByteReader,
        output: ByteWriter,
    ) -> BoxFuture<'static, Result<(), DownlinkTaskError>> {
        let DownlinkTask(model) = self;
        value_dowinlink_task(model, path, config, input, output).boxed()
    }
}

async fn value_dowinlink_task<T, LC>(
    model: ValueDownlinkModel<T, LC>,
    _path: AbsolutePath,
    config: DownlinkConfig,
    input: ByteReader,
    output: ByteWriter,
) -> Result<(), DownlinkTaskError>
where
    T: Form + Send + Sync + 'static,
    LC: for<'a> ValueDownlinkLifecycle<'a, T>,
{
    let ValueDownlinkModel {
        set_value,
        lifecycle,
    } = model;
    let read = read_task(config, input, lifecycle);
    let framed_writer = FramedWrite::new(output, DownlinkOperationEncoder);
    let write = write_task(framed_writer, set_value);
    let (read_result, _) = join(read, write).await;
    read_result
}

enum State<T> {
    Unlinked,
    Linked(Option<T>),
    Synced(T),
}

async fn read_task<T, LC>(
    config: DownlinkConfig,
    input: ByteReader,
    mut lifecycle: LC,
) -> Result<(), DownlinkTaskError>
where
    T: Form + Send + Sync + 'static,
    LC: for<'a> ValueDownlinkLifecycle<'a, T>,
{
    let DownlinkConfig {
        events_when_not_synced,
        terminate_on_unlinked,
    } = config;
    let mut state: State<T> = State::Unlinked;
    let mut framed_read =
        FramedRead::new(input, DownlinkNotifiationDecoder::new(T::make_recognizer()));

    while let Some(result) = framed_read.next().await {
        match result? {
            DownlinkNotification::Linked => {
                if matches!(&state, State::Unlinked) {
                    lifecycle.on_linked().await;
                    state = State::Linked(None);
                }
            }
            DownlinkNotification::Synced => match state {
                State::Linked(Some(value)) => {
                    lifecycle.on_synced(&value).await;
                    state = State::Synced(value);
                }
                _ => {
                    return Err(DownlinkTaskError::SyncedWithNoValue);
                }
            },
            DownlinkNotification::Event { body } => match &mut state {
                State::Linked(value) => {
                    if events_when_not_synced {
                        lifecycle.on_event(&body).await;
                        lifecycle.on_set(value.as_ref(), &body).await;
                    }
                    *value = Some(body);
                }
                State::Synced(value) => {
                    lifecycle.on_event(&body).await;
                    lifecycle.on_set(Some(value), &body).await;
                    *value = body;
                }
                _ => {}
            },
            DownlinkNotification::Unlinked => {
                lifecycle.on_unlinked().await;
                if terminate_on_unlinked {
                    break;
                } else {
                    state = State::Unlinked;
                }
            }
        }
    }
    Ok(())
}

async fn write_task<Snk, T>(mut framed: Snk, mut set_value: mpsc::Receiver<T>)
where
    Snk: Sink<DownlinkOperation<T>> + Unpin,
    T: Form + Send + 'static,
{
    while let (Some(value), Some(Ok(_)) | None) =
        immediate_or_join(set_value.recv(), framed.flush()).await
    {
        let op = DownlinkOperation::new(value);
        if framed.feed(op).await.is_err() {
            break;
        }
    }
}
