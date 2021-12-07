// // Copyright 2015-2021 SWIM.AI inc.
// //
// // Licensed under the Apache License, Version 2.0 (the "License");
// // you may not use this file except in compliance with the License.
// // You may obtain a copy of the License at
// //
// //     http://www.apache.org/licenses/LICENSE-2.0
// //
// // Unless required by applicable law or agreed to in writing, software
// // distributed under the License is distributed on an "AS IS" BASIS,
// // WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// // See the License for the specific language governing permissions and
// // limitations under the License.
//
// use crate::byte_routing::remote::transport;
// use crate::byte_routing::remote::transport::read::{ReadError, ReadErrorKind};
// use crate::byte_routing::routing::{RawRoute};
// use crate::compat::{AgentMessageDecoder, Operation, RequestMessage};
// use crate::error::{ResolutionError, ResolutionErrorKind, RouterError};
// use crate::routing::RoutingAddr;
// use bytes::BytesMut;
// use futures::future::BoxFuture;
// use futures_util::future::{join, ready};
// use futures_util::{FutureExt, StreamExt};
// use parking_lot::Mutex;
// use ratchet::{NegotiatedExtension, NoExt, NoExtEncoder, Role, WebSocketConfig};
// use std::collections::HashMap;
// use std::future::Future;
// use std::str::FromStr;
// use std::sync::Arc;
// use std::time::Duration;
// use swim_form::structural::read::recognizer::RecognizerReadable;
// use swim_model::path::RelativePath;
// use swim_model::Value;
// use swim_utilities::algebra::non_zero_usize;
// use swim_utilities::io::byte_channel::byte_channel;
// use swim_utilities::routing::uri::RelativeUri;
// use swim_utilities::trigger;
// use tokio::io::{duplex, AsyncWriteExt, DuplexStream};
// use tokio::sync::mpsc;
// use tokio_stream::wrappers::ReceiverStream;
// use tokio_util::codec::FramedRead;
// use url::Url;
//
// #[derive(Default)]
// struct RouterInner {
//     lut: HashMap<RelativeUri, RoutingAddr>,
//     resolver: HashMap<RoutingAddr, Option<RawRoute>>,
// }
//
// #[derive(Default, Clone)]
// struct MockRouter {
//     inner: Arc<Mutex<RouterInner>>,
// }
//
// impl Router for MockRouter {
//     fn resolve_sender(
//         &mut self,
//         addr: RoutingAddr,
//     ) -> BoxFuture<Result<RawRoute, ResolutionError>> {
//         let inner = &mut *self.inner.lock();
//         let route = match inner.resolver.get_mut(&addr) {
//             Some(addr) => Ok(addr.take().expect("Route already taken")),
//             None => Err(ResolutionError::new(
//                 ResolutionErrorKind::Unresolvable,
//                 None,
//             )),
//         };
//         ready(route).boxed()
//     }
//
//     fn lookup(
//         &mut self,
//         _host: Option<Url>,
//         route: RelativeUri,
//     ) -> BoxFuture<Result<RoutingAddr, RouterError>> {
//         let inner = &mut *self.inner.lock();
//         let addr = match inner.lut.get(&route) {
//             Some(addr) => Ok(*addr),
//             None => Err(RouterError::NoAgentAtRoute(route)),
//         };
//         ready(addr).boxed()
//     }
// }
//
// fn make_websocket(rx: DuplexStream) -> ratchet::WebSocket<DuplexStream, NoExt> {
//     ratchet::WebSocket::from_upgraded(
//         WebSocketConfig::default(),
//         rx,
//         NegotiatedExtension::from(NoExt),
//         BytesMut::default(),
//         Role::Server,
//     )
// }
//
// struct ReadFixture {
//     socket_tx: DuplexStream,
//     socket_sender: ratchet::Sender<DuplexStream, NoExtEncoder>,
//     downlink_tx: mpsc::Sender<(RelativePath, RawRoute)>,
//     stop_tx: trigger::Sender,
// }
//
// fn make_task<R>(router: R) -> (ReadFixture, impl Future<Output = Result<(), ReadError>>)
// where
//     R: Router,
// {
//     let (socket_tx, socket_rx) = duplex(128);
//     let socket = make_websocket(socket_rx);
//     let (socket_sender, socket_receiver) = socket.split().unwrap();
//     let (downlink_tx, downlink_rx) = mpsc::channel(8);
//     let (stop_tx, stop_rx) = trigger::trigger();
//
//     let fixture = ReadFixture {
//         socket_tx,
//         socket_sender,
//         downlink_tx,
//         stop_tx,
//     };
//
//     let read_task = transport::read::read_task(
//         RoutingAddr::remote(13),
//         router,
//         socket_receiver,
//         ReceiverStream::new(downlink_rx),
//         stop_rx,
//     );
//     (fixture, read_task)
// }
//
// #[tokio::test]
// async fn read_stops() {
//     let (ReadFixture { stop_tx, .. }, read_task) = make_task(MockRouter::default());
//
//     assert!(stop_tx.trigger());
//     assert!(tokio::time::timeout(Duration::from_secs(5), read_task)
//         .await
//         .is_ok());
// }
//
// #[tokio::test]
// async fn read_websocket_err() {
//     let (
//         ReadFixture {
//             mut socket_tx,
//             stop_tx: _stop_tx,
//             downlink_tx: _downlink_tx,
//             ..
//         },
//         read_task,
//     ) = make_task(MockRouter::default());
//
//     assert!(socket_tx
//         .write(&[1, 2, 3, 4, 5, 6, 7, 8, 9, 10])
//         .await
//         .is_ok());
//
//     let result = tokio::time::timeout(Duration::from_secs(5), read_task).await;
//
//     match result {
//         Ok(Err(e)) => {
//             assert_eq!(e.kind, ReadErrorKind::Websocket)
//         }
//         r => panic!("Expected a websocket read error, got: {:?}", r),
//     }
// }
//
// fn create_frame<A>(payload: A) -> BytesMut
// where
//     A: AsRef<[u8]>,
// {
//     let payload = payload.as_ref();
//     let mut buf = BytesMut::new();
//     ratchet::fixture::write_text_frame_header(&mut buf, Some(0), payload.len());
//
//     buf.extend_from_slice(payload);
//
//     buf
// }
//
// #[tokio::test]
// async fn read_message() {
//     let (writer, reader) = byte_channel(non_zero_usize!(128));
//
//     let router = MockRouter {
//         inner: Arc::new(Mutex::new(RouterInner {
//             lut: HashMap::from_iter(vec![(
//                 RelativeUri::from_str("/node").unwrap(),
//                 RoutingAddr::remote(13),
//             )]),
//             resolver: HashMap::from_iter(vec![(
//                 RoutingAddr::remote(13),
//                 Some(RawRoute { writer }),
//             )]),
//         })),
//     };
//
//     let msg = "@event(node:\"/node\",lane:lane)13";
//     let (
//         ReadFixture {
//             mut socket_tx,
//             stop_tx,
//             downlink_tx: _downlink_tx,
//             ..
//         },
//         read_task,
//     ) = make_task(router);
//
//     let main_task = async move {
//         let frame = create_frame(msg);
//         assert!(socket_tx.write(frame.as_ref()).await.is_ok());
//         assert!(read_task.await.is_ok());
//     };
//
//     let reader_task = async move {
//         let decoder = AgentMessageDecoder::new(Value::make_recognizer());
//         let mut framed = FramedRead::new(reader, decoder);
//         match framed.next().await {
//             Some(Ok(message)) => {
//                 let expected = RequestMessage {
//                     origin: RoutingAddr::remote(13),
//                     path: RelativePath::new("/node", "lane"),
//                     envelope: Operation::Command("13".into()),
//                 };
//                 assert_eq!(expected, message);
//                 assert!(stop_tx.trigger());
//             }
//             r => panic!("Expected an item to be produced. Got: {:?}", r),
//         }
//     };
//
//     join(main_task, reader_task).await;
// }
