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
use crate::compat::{AgentMessageDecoder, Operation, RequestMessage};
use crate::routing::RoutingAddr;
use bytes::BytesMut;
use futures_util::future::join;
use futures_util::StreamExt;
use ratchet::{NegotiatedExtension, NoExt, NoExtEncoder, Role, WebSocketConfig};
use std::collections::HashMap;
use std::future::Future;
use std::str::FromStr;
use std::time::Duration;
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
use tokio_util::codec::FramedRead;

fn make_websocket(rx: DuplexStream) -> ratchet::WebSocket<DuplexStream, NoExt> {
    ratchet::WebSocket::from_upgraded(
        WebSocketConfig::default(),
        rx,
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

fn make_task(
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
        RoutingAddr::remote(13),
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
        make_task(HashMap::default(), HashMap::default());

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
    ) = make_task(HashMap::default(), HashMap::default());

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

    let lut = HashMap::from_iter(vec![(
        RelativeUri::from_str("/node").unwrap(),
        RoutingAddr::plane(13),
    )]);
    let resolver = HashMap::from_iter(vec![(RoutingAddr::plane(13), Some(RawRoute { writer }))]);

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
    ) = make_task(lut, resolver);

    let main_task = async move {
        let frame = create_frame(msg);
        assert!(socket_tx.write(frame.as_ref()).await.is_ok());
        println!("{:?}", read_task.await);
    };

    let reader_task = async move {
        let decoder = AgentMessageDecoder::new(Value::make_recognizer());
        let mut framed = FramedRead::new(reader, decoder);
        match framed.next().await {
            Some(Ok(message)) => {
                let expected = RequestMessage {
                    origin: RoutingAddr::plane(13),
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

#[test]
fn t() {
    let tag = RoutingAddr::plane(0x1a);
    println!("{}", tag);
    println!("{}", tag.is_plane());

    let string = format!("{}", RoutingAddr::plane(0x1a));
    assert_eq!(string, "Plane(1A)");
}
