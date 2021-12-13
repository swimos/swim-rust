// Copyright 2015-2021 SWIM.AI inc.
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

mod fixture;

use crate::byte_routing::remote::transport;
use crate::byte_routing::remote::transport::read::ReadError;
use crate::byte_routing::remote::transport::tests::fixture::MockPlaneRouter;
use crate::byte_routing::routing::router::ServerRouter;
use crate::byte_routing::routing::RawRoute;
use crate::compat::{
    AgentMessageDecoder, Operation, RawRequestMessageEncoder, RawResponseMessageDecoder,
    RequestMessage, ResponseMessage, ResponseMessageEncoder,
};
use crate::routing::RoutingAddr;
use bytes::BytesMut;
use futures_util::future::{join, join3};
use futures_util::{FutureExt, SinkExt, StreamExt};
use ratchet::{Message, NegotiatedExtension, NoExt, NoExtEncoder, Role, WebSocketConfig};
use std::collections::{HashMap, HashSet};
use std::future::Future;
use std::str::FromStr;
use std::time::Duration;
use swim_form::structural::read::from_model::ValueMaterializer;
use swim_form::structural::read::recognizer::RecognizerReadable;
use swim_model::path::RelativePath;
use swim_model::Value;
use swim_utilities::algebra::non_zero_usize;
use swim_utilities::io::byte_channel::byte_channel;
use swim_utilities::routing::uri::RelativeUri;
use swim_utilities::trigger;
use tokio::io::{duplex, AsyncWriteExt, DuplexStream};
use tokio::sync::mpsc;
use tokio::task::JoinHandle;
use tokio_stream::wrappers::ReceiverStream;
use tokio_util::codec::{FramedRead, FramedWrite};

const LOCAL_ADDR: RoutingAddr = RoutingAddr::plane(13);

fn make_websocket(channel: DuplexStream) -> ratchet::WebSocket<DuplexStream, NoExt> {
    ratchet::WebSocket::from_upgraded(
        WebSocketConfig::default(),
        channel,
        NegotiatedExtension::from(NoExt),
        BytesMut::default(),
        Role::Server,
    )
}

struct ReadFixture {
    _router: JoinHandle<()>,
    socket_tx: DuplexStream,
    socket_sender: ratchet::Sender<DuplexStream, NoExtEncoder>,
    downlink_tx: mpsc::Sender<(RelativePath, RawRoute)>,
    stop_tx: trigger::Sender,
}

fn make_read_task(
    lut: HashMap<RelativeUri, RoutingAddr>,
    resolver: HashMap<RoutingAddr, Option<RawRoute>>,
) -> (ReadFixture, impl Future<Output = Result<(), ReadError>>) {
    let (socket_tx, socket_rx) = duplex(128);
    let socket = make_websocket(socket_rx);
    let (socket_sender, socket_receiver) = socket.split().unwrap();
    let (downlink_tx, downlink_rx) = mpsc::channel(8);
    let (stop_tx, stop_rx) = trigger::trigger();

    let (plane_tx, plane_rx) = mpsc::channel(16);
    let mock_plane_router = MockPlaneRouter::new(plane_rx, lut, resolver);
    let router = ServerRouter::new(plane_tx, mpsc::channel(1).0);

    let fixture = ReadFixture {
        _router: tokio::spawn(mock_plane_router.run()),
        socket_tx,
        socket_sender,
        downlink_tx,
        stop_tx,
    };

    let read_task = transport::read::task(
        LOCAL_ADDR,
        router,
        socket_receiver,
        ReceiverStream::new(downlink_rx),
        stop_rx,
    );
    (fixture, read_task)
}

#[tokio::test]
async fn read_stops() {
    let (ReadFixture { stop_tx, .. }, read_task) =
        make_read_task(HashMap::default(), HashMap::default());

    assert!(stop_tx.trigger());
    assert!(tokio::time::timeout(Duration::from_secs(5), read_task)
        .await
        .is_ok());
}

#[tokio::test]
async fn read_websocket_err() {
    let (
        ReadFixture {
            mut socket_tx,
            stop_tx: _stop_tx,
            downlink_tx: _downlink_tx,
            ..
        },
        read_task,
    ) = make_read_task(HashMap::default(), HashMap::default());

    assert!(socket_tx
        .write(&[1, 2, 3, 4, 5, 6, 7, 8, 9, 10])
        .await
        .is_ok());

    let result = tokio::time::timeout(Duration::from_secs(5), read_task).await;

    match result {
        Ok(Err(e)) => {
            assert!(matches!(e, ReadError::WebSocket(_)));
        }
        r => panic!("Expected a websocket read error, got: {:?}", r),
    }
}

fn create_frame<A>(payload: A) -> BytesMut
where
    A: AsRef<[u8]>,
{
    let payload = payload.as_ref();
    let mut buf = BytesMut::new();
    ratchet::fixture::write_text_frame_header(&mut buf, Some(0), payload.len());

    buf.extend_from_slice(payload);

    buf
}

#[tokio::test]
async fn read_message() {
    let (writer, reader) = byte_channel(non_zero_usize!(128));

    let lut = HashMap::from_iter(vec![(RelativeUri::from_str("/node").unwrap(), LOCAL_ADDR)]);
    let resolver = HashMap::from_iter(vec![(LOCAL_ADDR, Some(RawRoute { writer }))]);

    let msg = "@event(node:\"/node\",lane:lane)13";
    let (
        ReadFixture {
            _router,
            mut socket_tx,
            stop_tx,
            downlink_tx: _downlink_tx,
            ..
        },
        read_task,
    ) = make_read_task(lut, resolver);

    let main_task = async move {
        let frame = create_frame(msg);
        assert!(socket_tx.write(frame.as_ref()).await.is_ok());
        assert!(read_task.await.is_ok());
    };

    let reader_task = async move {
        let decoder = AgentMessageDecoder::new(Value::make_recognizer());
        let mut framed = FramedRead::new(reader, decoder);
        match framed.next().await {
            Some(Ok(message)) => {
                let expected = RequestMessage {
                    origin: LOCAL_ADDR,
                    path: RelativePath::new("/node", "lane"),
                    envelope: Operation::Command("13".into()),
                };
                assert_eq!(expected, message);
                assert!(stop_tx.trigger());
            }
            r => panic!("Expected an item to be produced. Got: {:?}", r),
        }
    };

    join(main_task, reader_task).await;
}

#[tokio::test]
async fn write_ok() {
    let (socket_tx, socket_rx) = duplex(128);
    let socket = make_websocket(socket_tx);
    let (socket_sender, _socket_receiver) = socket.split().unwrap();
    let (stop_tx, stop_rx) = trigger::trigger();

    let (downlink_tx, downlink_rx) = mpsc::channel(8);
    let (agent_tx, agent_rx) = mpsc::channel(8);

    let downlink_tx_guard = downlink_tx.clone();
    let agent_tx_guard = agent_tx.clone();

    let write_task = transport::write::task(
        socket_sender,
        non_zero_usize!(4096),
        downlink_rx,
        agent_rx,
        stop_rx,
    );

    let receive_task = async move {
        let mut peer_rx = ratchet::WebSocket::from_upgraded(
            Default::default(),
            socket_rx,
            NegotiatedExtension::from(NoExt),
            BytesMut::default(),
            Role::Client,
        );

        let mut expected = HashSet::from([
            "@synced(node:node, lane:lane)",
            "@sync(node:node, lane:lane)",
            "@link(node:node, lane:lane)",
        ]);
        let mut read_buf = BytesMut::default();

        loop {
            match peer_rx.read(&mut read_buf).await.unwrap() {
                Message::Text => match std::str::from_utf8(read_buf.as_ref()) {
                    Ok(received) => {
                        assert!(expected.remove(received));
                        if expected.is_empty() {
                            break;
                        }

                        read_buf.clear();
                    }
                    r => panic!("Expected one of: {:?}, got: {:?}", expected, r),
                },
                m => {
                    panic!("Expected one of: {:?}, got: {:?}", expected, m)
                }
            }
        }

        assert!(peer_rx.read(&mut read_buf).now_or_never().is_none());
        stop_tx.trigger();

        // guard to prevent the write task from completing until we have read every message
        let _g1 = downlink_tx_guard;
        let _g2 = agent_tx_guard;
    };

    let produce_task = async move {
        let (downlink_write_tx, downlink_read_rx) = byte_channel(non_zero_usize!(256));
        let read_framed = FramedRead::new(downlink_read_rx, RawResponseMessageDecoder::default());
        assert!(downlink_tx.send(read_framed).await.is_ok());

        let mut write_framed = FramedWrite::new(downlink_write_tx, ResponseMessageEncoder);
        assert!(write_framed
            .send(ResponseMessage::<Value>::synced(
                RoutingAddr::plane(1),
                RelativePath::new("node", "lane")
            ))
            .await
            .is_ok());

        let (agent_write_tx, agent_read_rx) = byte_channel(non_zero_usize!(256));
        let read_framed = FramedRead::new(
            agent_read_rx,
            AgentMessageDecoder::new(ValueMaterializer::default()),
        );
        assert!(agent_tx.send(read_framed).await.is_ok());

        let mut write_framed = FramedWrite::new(agent_write_tx, RawRequestMessageEncoder);
        assert!(write_framed
            .send(RequestMessage::sync(
                RoutingAddr::plane(1),
                RelativePath::new("node", "lane")
            ))
            .await
            .is_ok());

        assert!(write_framed
            .send(RequestMessage::link(
                RoutingAddr::plane(1),
                RelativePath::new("node", "lane"),
            ))
            .await
            .is_ok());
    };

    let fut = join3(write_task, receive_task, produce_task);
    let result = tokio::time::timeout(Duration::from_secs(5), fut).await;
    assert!(result.is_ok());
}
