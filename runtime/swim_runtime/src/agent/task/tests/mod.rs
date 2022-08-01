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

use std::{
    collections::HashSet,
    num::NonZeroUsize,
    pin::Pin,
    task::{Context, Poll},
    time::Duration,
};

use bytes::Bytes;
use futures::{future::Either, ready, SinkExt, Stream, StreamExt};
use swim_api::{
    agent::{LaneConfig, UplinkKind},
    error::FrameIoError,
    protocol::{
        agent::{
            LaneRequest, LaneRequestDecoder, LaneResponseKind, MapLaneResponse,
            MapLaneResponseEncoder, ValueLaneResponse, ValueLaneResponseEncoder,
        },
        map::{MapMessage, MapMessageDecoder, MapOperation, MapOperationDecoder},
        WithLenRecognizerDecoder,
    },
};
use swim_form::structural::read::recognizer::primitive::I32Recognizer;
use swim_messages::{protocol::{RawResponseMessageDecoder, Notification, ResponseMessage, RawRequestMessageEncoder, RequestMessage, Path}, bytes_str::BytesStr};
use swim_model::Text;
use swim_recon::{
    parser::{parse_recognize, Span},
    printer::print_recon_compact,
};
use swim_utilities::{
    algebra::non_zero_usize,
    io::byte_channel::{ByteReader, ByteWriter},
    trigger::promise,
};
use tokio::sync::mpsc;
use tokio_util::codec::{FramedRead, FramedWrite};
use uuid::Uuid;

use crate::{
    agent::{AgentRuntimeConfig, DisconnectionReason},
};

use super::{LaneEndpoint, RwCoorindationMessage};

mod coordination;
mod read;
mod write;

const QUEUE_SIZE: NonZeroUsize = non_zero_usize!(8);
const BUFFER_SIZE: NonZeroUsize = non_zero_usize!(4096);
const SHUTDOWN_TIMEOUT: Duration = Duration::from_secs(2);

fn make_config(inactive_timeout: Duration) -> AgentRuntimeConfig {
    make_prune_config(inactive_timeout, inactive_timeout)
}

fn make_prune_config(
    inactive_timeout: Duration,
    prune_remote_delay: Duration,
) -> AgentRuntimeConfig {
    AgentRuntimeConfig {
        default_lane_config: LaneConfig {
            input_buffer_size: BUFFER_SIZE,
            output_buffer_size: BUFFER_SIZE,
        },
        attachment_queue_size: non_zero_usize!(8),
        inactive_timeout,
        prune_remote_delay,
        shutdown_timeout: SHUTDOWN_TIMEOUT,
    }
}

const VAL_LANE: &str = "value_lane";
const MAP_LANE: &str = "map_lane";

const TEST_TIMEOUT: Duration = Duration::from_secs(10);
const DEFAULT_TIMEOUT: Duration = Duration::from_secs(5);
const INACTIVE_TEST_TIMEOUT: Duration = Duration::from_millis(100);

#[derive(Debug, Clone)]
enum Event {
    Sync {
        name: Text,
        id: Uuid,
    },
    ValueCommand {
        name: Text,
        n: i32,
    },
    MapCommand {
        name: Text,
        cmd: MapMessage<Text, i32>,
    },
    Coord(RwCoorindationMessage),
}

enum Instruction {
    ValueEvent {
        lane: Text,
        value: i32,
    },
    MapEvent {
        lane: Text,
        key: Text,
        value: i32,
        id: Option<Uuid>,
    },
    ValueSynced {
        lane: Text,
        value: i32,
        id: Uuid,
    },
    MapSynced {
        lane: Text,
        id: Uuid,
    },
}

struct Instructions(mpsc::UnboundedSender<Instruction>);

impl Instructions {
    fn new(inner: mpsc::UnboundedSender<Instruction>) -> Self {
        Instructions(inner)
    }

    fn value_event(&self, lane: &str, value: i32) {
        let Instructions(inner) = self;
        assert!(inner
            .send(Instruction::ValueEvent {
                lane: Text::new(lane),
                value
            })
            .is_ok());
    }

    fn map_event(&self, lane: &str, key: &str, value: i32) {
        let Instructions(inner) = self;
        assert!(inner
            .send(Instruction::MapEvent {
                lane: Text::new(lane),
                key: Text::new(key),
                value,
                id: None,
            })
            .is_ok());
    }

    fn map_syncing_event(&self, id: Uuid, lane: &str, key: &str, value: i32) {
        let Instructions(inner) = self;
        assert!(inner
            .send(Instruction::MapEvent {
                lane: Text::new(lane),
                key: Text::new(key),
                value,
                id: Some(id),
            })
            .is_ok());
    }

    fn map_synced_event(&self, id: Uuid, lane: &str) {
        let Instructions(inner) = self;
        assert!(inner
            .send(Instruction::MapSynced {
                lane: Text::new(lane),
                id
            })
            .is_ok());
    }

    fn value_synced_event(&self, remote_id: Uuid, lane: &str, value: i32) {
        let Instructions(inner) = self;
        assert!(inner
            .send(Instruction::ValueSynced {
                id: remote_id,
                lane: Text::new(lane),
                value
            })
            .is_ok());
    }
}

struct ValueLaneSender {
    inner: FramedWrite<ByteWriter, ValueLaneResponseEncoder>,
}

impl ValueLaneSender {
    fn new(writer: ByteWriter) -> Self {
        ValueLaneSender {
            inner: FramedWrite::new(writer, Default::default()),
        }
    }

    async fn event(&mut self, n: i32) {
        let ValueLaneSender { inner } = self;
        assert!(inner.send(ValueLaneResponse::event(n)).await.is_ok());
    }

    async fn synced(&mut self, id: Uuid, n: i32) {
        let ValueLaneSender { inner } = self;
        assert!(inner.send(ValueLaneResponse::synced(id, n)).await.is_ok());
    }
}

struct MapLaneSender {
    inner: FramedWrite<ByteWriter, MapLaneResponseEncoder>,
}

impl MapLaneSender {
    fn new(writer: ByteWriter) -> Self {
        MapLaneSender {
            inner: FramedWrite::new(writer, Default::default()),
        }
    }

    async fn update_event(&mut self, key: Text, value: i32) {
        let MapLaneSender { inner } = self;
        assert!(inner
            .send(MapLaneResponse::Event {
                kind: LaneResponseKind::StandardEvent,
                operation: MapOperation::Update { key, value }
            })
            .await
            .is_ok());
    }

    async fn remove_event(&mut self, key: Text) {
        let MapLaneSender { inner } = self;
        let operation: MapOperation<Text, i32> = MapOperation::Remove { key };
        assert!(inner
            .send(MapLaneResponse::Event {
                kind: LaneResponseKind::StandardEvent,
                operation,
            })
            .await
            .is_ok());
    }

    async fn clear_event(&mut self) {
        let MapLaneSender { inner } = self;
        let operation: MapOperation<Text, i32> = MapOperation::Clear;
        assert!(inner
            .send(MapLaneResponse::Event {
                kind: LaneResponseKind::StandardEvent,
                operation
            })
            .await
            .is_ok());
    }

    async fn sync_event(&mut self, id: Uuid, key: Text, value: i32) {
        let MapLaneSender { inner } = self;
        assert!(inner
            .send(MapLaneResponse::Event {
                kind: LaneResponseKind::SyncEvent(id),
                operation: MapOperation::Update { key, value }
            })
            .await
            .is_ok());
    }

    async fn synced(&mut self, id: Uuid) {
        let MapLaneSender { inner } = self;
        assert!(inner
            .send(MapLaneResponse::<Text, i32>::SyncComplete(id))
            .await
            .is_ok());
    }
}

type ValueDecoder = LaneRequestDecoder<WithLenRecognizerDecoder<I32Recognizer>>;
type MapDecoder = LaneRequestDecoder<MapMessageDecoder<MapOperationDecoder<Text, i32>>>;

enum LaneReader {
    Value {
        name: Text,
        read: FramedRead<ByteReader, ValueDecoder>,
    },
    Map {
        name: Text,
        read: FramedRead<ByteReader, MapDecoder>,
    },
}

impl LaneReader {
    fn new(endpoint: LaneEndpoint<ByteReader>) -> Self {
        let LaneEndpoint { name, kind, io } = endpoint;
        match kind {
            UplinkKind::Value => LaneReader::Value {
                name,
                read: FramedRead::new(
                    io,
                    LaneRequestDecoder::new(WithLenRecognizerDecoder::new(I32Recognizer)),
                ),
            },
            UplinkKind::Map => LaneReader::Map {
                name,
                read: FramedRead::new(io, LaneRequestDecoder::new(Default::default())),
            },
        }
    }
}

impl Stream for LaneReader {
    type Item = (
        Text,
        Result<Either<LaneRequest<i32>, LaneRequest<MapMessage<Text, i32>>>, FrameIoError>,
    );

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        Poll::Ready(match self.get_mut() {
            LaneReader::Value { name, read } => {
                let maybe_result = ready!(read.poll_next_unpin(cx));
                maybe_result.map(|r| (name.clone(), r.map(Either::Left).map_err(Into::into)))
            }
            LaneReader::Map { name, read } => {
                let maybe_result = ready!(read.poll_next_unpin(cx));
                maybe_result.map(|r| (name.clone(), r.map(Either::Right)))
            }
        })
    }
}

#[derive(Debug)]
struct RemoteReceiver {
    expected_agent: Uuid,
    expected_node: String,
    inner: FramedRead<ByteReader, RawResponseMessageDecoder>,
    completion_rx: promise::Receiver<DisconnectionReason>,
}

impl RemoteReceiver {
    fn new(
        expected_agent: Uuid,
        expected_node: String,
        rx: ByteReader,
        completion_rx: promise::Receiver<DisconnectionReason>,
    ) -> Self {
        RemoteReceiver {
            expected_agent,
            expected_node,
            inner: FramedRead::new(rx, Default::default()),
            completion_rx,
        }
    }

    async fn expect_envelope<F>(&mut self, lane: &str, f: F)
    where
        F: FnOnce(Notification<Bytes, Bytes>),
    {
        let next = self.inner.next().await;

        match next {
            Some(Ok(ResponseMessage {
                origin,
                path,
                envelope,
            })) => {
                assert_eq!(origin, self.expected_agent);
                assert_eq!(path, Path::new(BytesStr::from(self.expected_node.as_str()), BytesStr::from(lane)));
                f(envelope);
            }
            ow => {
                panic!("Unexpected result: {:?}", ow);
            }
        }
    }

    async fn expect_linked(&mut self, lane: &str) {
        self.expect_envelope(lane, |envelope| {
            assert!(matches!(envelope, Notification::Linked));
        })
        .await
    }

    async fn expect_value_event(&mut self, lane: &str, value: i32) {
        self.expect_envelope(lane, |envelope| {
            if let Notification::Event(body) = envelope {
                let expected_body = format!("{}", value);
                let body_str = std::str::from_utf8(body.as_ref()).expect("Corrupted body.");
                assert_eq!(body_str, expected_body);
            } else {
                panic!("Unexpected envelope: {:?}", envelope);
            }
        })
        .await
    }

    async fn expect_any_map_event<F>(&mut self, lane: &str, mut f: F)
    where
        F: FnMut(MapMessage<Text, i32>),
    {
        self.expect_envelope(lane, |envelope| {
            if let Notification::Event(body) = envelope {
                let body_str = std::str::from_utf8(body.as_ref()).expect("Corrupted body.");
                let message = parse_recognize::<MapMessage<Text, i32>>(Span::new(body_str), false)
                    .expect("Invalid map mesage.");
                f(message)
            } else {
                panic!("Unexpected envelope: {:?}", envelope);
            }
        })
        .await
    }

    async fn expect_map_event(&mut self, lane: &str, key: &str, value: i32) {
        self.expect_envelope(lane, |envelope| {
            if let Notification::Event(body) = envelope {
                let op = MapOperation::Update {
                    key: Text::new(key),
                    value,
                };
                let expected_body = format!("{}", print_recon_compact(&op));
                let body_str = std::str::from_utf8(body.as_ref()).expect("Corrupted body.");
                assert_eq!(body_str, expected_body);
            } else {
                panic!("Unexpected envelope: {:?}", envelope);
            }
        })
        .await
    }

    async fn expect_value_synced(&mut self, lane: &str, value: i32) {
        self.expect_envelope(lane, |envelope| {
            if let Notification::Event(body) = envelope {
                let expected_body = format!("{}", value);
                let body_str = std::str::from_utf8(body.as_ref()).expect("Corrupted body.");
                assert_eq!(body_str, expected_body);
            } else {
                panic!("Unexpected envelope: {:?}", envelope);
            }
        })
        .await;
        self.expect_envelope(lane, |envelope| {
            assert!(matches!(envelope, Notification::Synced));
        })
        .await;
    }

    async fn expect_map_synced(&mut self, lane: &str) {
        self.expect_envelope(lane, |envelope| {
            assert!(matches!(envelope, Notification::Synced));
        })
        .await
    }

    async fn expect_unlinked(&mut self, lane: &str) {
        self.expect_envelope(lane, |envelope| {
            assert!(matches!(envelope, Notification::Unlinked(_)));
        })
        .await
    }

    async fn expect_clean_shutdown(
        self,
        expected_lanes: Vec<&str>,
        expected_reason: Option<DisconnectionReason>,
    ) {
        let mut lanes: HashSet<&str> = expected_lanes.into_iter().collect();
        let RemoteReceiver {
            inner,
            expected_agent,
            expected_node,
            completion_rx,
        } = self;
        let results = inner.collect::<Vec<_>>().await;
        for result in results {
            match result {
                Ok(ResponseMessage {
                    origin,
                    path,
                    envelope: Notification::Unlinked(_),
                }) => {
                    assert_eq!(origin, expected_agent);
                    assert_eq!(&path.node, &expected_node);
                    let lane = &path.lane;
                    assert!(lanes.remove(lane.as_str()));
                }
                ow => {
                    panic!("Unexpected result: {:?}", ow);
                }
            }
        }
        if !lanes.is_empty() {
            panic!("Some lanes were not unlinked: {:?}", lanes);
        }
        let reason = completion_rx
            .await
            .map(|arc| *arc)
            .unwrap_or(DisconnectionReason::Failed);

        assert_eq!(
            reason,
            expected_reason.unwrap_or(DisconnectionReason::AgentStoppedExternally)
        );
    }
}

struct RemoteSender {
    node: String,
    rid: Uuid,
    inner: FramedWrite<ByteWriter, RawRequestMessageEncoder>,
}

impl RemoteSender {
    fn new(node: String, rid: Uuid, writer: ByteWriter) -> Self {
        RemoteSender {
            node,
            rid,
            inner: FramedWrite::new(writer, Default::default()),
        }
    }

    async fn link(&mut self, lane: &str) {
        let RemoteSender { node, rid, inner } = self;
        let path = Path::new(node.as_str(), lane);
        let msg: RequestMessage<&str, &[u8]> = RequestMessage::link(*rid, path);
        assert!(inner.send(msg).await.is_ok());
    }

    async fn unlink(&mut self, lane: &str) {
        let RemoteSender { node, rid, inner } = self;
        let path = Path::new(node.as_str(), lane);
        let msg: RequestMessage<&str, &[u8]> = RequestMessage::unlink(*rid, path);
        assert!(inner.send(msg).await.is_ok());
    }

    async fn sync(&mut self, lane: &str) {
        let RemoteSender { node, rid, inner } = self;
        let path = Path::new(node.as_str(), lane);
        let msg: RequestMessage<&str, &[u8]> = RequestMessage::sync(*rid, path);
        assert!(inner.send(msg).await.is_ok());
    }

    async fn value_command(&mut self, lane: &str, n: i32) {
        let RemoteSender { node, rid, inner } = self;
        let path = Path::new(node.as_str(), lane);
        let body = format!("{}", n);
        let msg: RequestMessage<&str, &[u8]> = RequestMessage::command(*rid, path, body.as_bytes());
        assert!(inner
            .send(msg)
            .await
            .is_ok());
    }

    async fn map_command(&mut self, lane: &str, key: &str, value: i32) {
        let RemoteSender { node, rid, inner } = self;
        let path = Path::new(node.as_str(), lane);
        let body = format!("@update(key:\"{}\") {}", key, value);
        let msg: RequestMessage<&str, &[u8]> = RequestMessage::command(*rid, path, body.as_bytes());
        assert!(inner
            .send(msg)
            .await
            .is_ok());
    }
}
