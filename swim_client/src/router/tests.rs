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

use crate::utilities::sync::promise;
use std::collections::HashMap;
use swim_common::routing::error::ConnectionError;
use swim_common::routing::error::ResolutionError;
use swim_common::routing::remote::table::BidirectionalRegistrator;
use swim_common::routing::remote::RemoteRoutingRequest;
use swim_common::routing::{RoutingAddr, TaggedEnvelope, TaggedSender};
use tokio::sync::mpsc;
use url::Url;

pub(crate) struct FakeConnections {
    outgoing_channels: HashMap<Url, TaggedSender>,
    incoming_channels: HashMap<Url, mpsc::Receiver<TaggedEnvelope>>,
    addr: u32,
}

impl FakeConnections {
    pub(crate) fn new() -> Self {
        FakeConnections {
            outgoing_channels: HashMap::new(),
            incoming_channels: HashMap::new(),
            addr: 0,
        }
    }

    pub(crate) fn add_connection(
        &mut self,
        url: Url,
    ) -> (mpsc::Sender<TaggedEnvelope>, mpsc::Receiver<TaggedEnvelope>) {
        let (outgoing_tx, outgoing_rx) = mpsc::channel(8);
        let (incoming_tx, incoming_rx) = mpsc::channel(8);

        let _ = self.outgoing_channels.insert(
            url.clone(),
            TaggedSender::new(RoutingAddr::client(self.addr), outgoing_tx),
        );
        let _ = self.incoming_channels.insert(url, incoming_rx);
        self.addr += 1;

        (incoming_tx, outgoing_rx)
    }

    fn get_connection(
        &mut self,
        url: &Url,
    ) -> Option<(TaggedSender, mpsc::Receiver<TaggedEnvelope>)> {
        if let Some(conn_sender) = self.outgoing_channels.get(url) {
            Some((
                conn_sender.clone(),
                self.incoming_channels.remove(url).unwrap(),
            ))
        } else {
            None
        }
    }
}

pub(crate) struct MockRemoteRouterTask;

impl MockRemoteRouterTask {
    pub(crate) fn new(mut fake_conns: FakeConnections) -> mpsc::Sender<RemoteRoutingRequest> {
        let (tx, mut rx) = mpsc::channel(32);

        tokio::spawn(async move {
            let mut drops = vec![];

            while let Some(request) = rx.recv().await {
                match request {
                    RemoteRoutingRequest::Bidirectional { host, request } => {
                        if let Some((sender_tx, receiver_rx)) = fake_conns.get_connection(&host) {
                            let (tx, mut rx) = mpsc::channel(8);
                            let (on_drop_tx, on_drop_rx) = promise::promise();

                            drops.push(on_drop_tx);

                            let registrator =
                                BidirectionalRegistrator::new(sender_tx, tx, on_drop_rx);

                            request.send(Ok(registrator)).unwrap();

                            let receiver_request = rx.recv().await.unwrap();
                            receiver_request.send(receiver_rx).unwrap();
                        } else {
                            request
                                .send(Err(ConnectionError::Resolution(
                                    ResolutionError::unresolvable(host.to_string()),
                                )))
                                .unwrap();
                        }
                    }
                    _ => unreachable!(),
                }
            }
        });

        tx
    }
}

//
// use crate::router::{ClientRouterFactory, DownlinkRoutingRequest, Router};
// use std::convert::TryFrom;
// use std::num::NonZeroUsize;
// use std::sync::{Arc, Mutex};
// use swim_common::model::Value;
// use swim_common::request::Request;
// use swim_common::routing::error::{ResolutionError, Unresolvable};
// use swim_common::routing::remote::{RawRoute, RemoteRoutingRequest, Scheme, SchemeSocketAddr};
// use swim_common::routing::{
//     CloseSender, PlaneRoutingRequest, RouterFactory, RoutingAddr, TaggedEnvelope,
// };
// use swim_common::warp::envelope::Envelope;
// use swim_common::warp::path::{AbsolutePath, Path, RelativePath};
// use tokio::sync::mpsc::{Receiver, Sender};
// use tokio::sync::{mpsc, oneshot};
// use url::Url;
// use utilities::sync::promise;
// use utilities::uri::RelativeUri;
//
// async fn create_connection_manager() -> (
//     Sender<DownlinkRoutingRequest<Path>>,
//     Receiver<TaggedEnvelope>,
//     Receiver<TaggedEnvelope>,
//     (Arc<Mutex<Vec<Url>>>, Arc<Mutex<Vec<RelativeUri>>>),
//     CloseSender,
// ) {
//     let remote_requests: Arc<Mutex<Vec<Url>>> = Arc::new(Mutex::new(Vec::new()));
//     let local_requests: Arc<Mutex<Vec<RelativeUri>>> = Arc::new(Mutex::new(Vec::new()));
//
//     let remote_requests_copy = remote_requests.clone();
//     let local_requests_copy = local_requests.clone();
//
//     let (router_request_tx, router_request_rx) = mpsc::channel::<DownlinkRoutingRequest<Path>>(8);
//     let (remote_router_tx, mut remote_router_rx) = mpsc::channel(8);
//     let (plane_router_tx, mut plane_router_rx) = mpsc::channel(8);
//     let buffer_size = NonZeroUsize::new(8).unwrap();
//
//     let (remote_tx, remote_rx) = mpsc::channel(8);
//     let (local_tx, local_rx) = mpsc::channel(8);
//     let (close_tx, close_rx) = promise::promise();
//
//     let conns_manager = ClientConnectionsManager::new(
//         router_request_rx,
//         remote_router_tx,
//         Some(plane_router_tx),
//         buffer_size,
//         close_rx,
//     );
//
//     tokio::spawn(async move {
//         conns_manager.run().await.unwrap();
//     });
//
//     tokio::spawn(async move {
//         while let Some(remote_request) = remote_router_rx.recv().await {
//             match remote_request {
//                 RemoteRoutingRequest::Endpoint {
//                     addr: _addr,
//                     request,
//                 } => {
//                     let (_on_drop_tx, on_drop_rx) = promise::promise();
//                     request
//                         .send(Ok(RawRoute::new(remote_tx.clone(), on_drop_rx)))
//                         .unwrap();
//                 }
//                 RemoteRoutingRequest::ResolveUrl { host, request } => {
//                     (*remote_requests.lock().unwrap()).push(host);
//                     request.send(Ok(RoutingAddr::remote(0))).unwrap();
//                 }
//             }
//         }
//     });
//
//     tokio::spawn(async move {
//         while let Some(plane_request) = plane_router_rx.recv().await {
//             match plane_request {
//                 PlaneRoutingRequest::Endpoint { request, .. } => {
//                     let (_on_drop_tx, on_drop_rx) = promise::promise();
//                     request
//                         .send(Ok(RawRoute::new(local_tx.clone(), on_drop_rx)))
//                         .unwrap();
//                 }
//                 PlaneRoutingRequest::Resolve { request, name, .. } => {
//                     (*local_requests.lock().unwrap()).push(name);
//                     request.send(Ok(RoutingAddr::plane(0))).unwrap()
//                 }
//                 PlaneRoutingRequest::Agent { .. } => {
//                     unimplemented!()
//                 }
//                 PlaneRoutingRequest::Routes(_) => {
//                     unimplemented!()
//                 }
//             }
//         }
//     });
//
//     (
//         router_request_tx,
//         remote_rx,
//         local_rx,
//         (remote_requests_copy, local_requests_copy),
//         close_tx,
//     )
// }
//
// async fn register_connection(
//     router_request_tx: &Sender<DownlinkRoutingRequest<Path>>,
//     origin: Origin,
// ) -> RawRoute {
//     let (tx, rx) = oneshot::channel();
//     let request = Request::new(tx);
//
//     router_request_tx
//         .send(DownlinkRoutingRequest::Connect { request, origin })
//         .await
//         .unwrap();
//
//     rx.await.unwrap().unwrap()
// }
//
// async fn register_subscriber(
//     router_request_tx: &Sender<DownlinkRoutingRequest<Path>>,
//     path: Path,
// ) -> (RawRoute, Receiver<Envelope>) {
//     let (tx, rx) = oneshot::channel();
//     let request = Request::new(tx);
//
//     router_request_tx
//         .send(DownlinkRoutingRequest::Subscribe {
//             target: path,
//             request,
//         })
//         .await
//         .unwrap();
//
//     rx.await.unwrap().unwrap()
// }
//
// async fn send_message(sender: &RawRoute, message: Envelope) {
//     sender
//         .sender
//         .send(TaggedEnvelope(RoutingAddr::client(), message))
//         .await
//         .unwrap();
// }
//
// #[tokio::test]
// async fn test_client_router_lookup() {
//     let (request_tx, _request_rx) = mpsc::channel::<DownlinkRoutingRequest<AbsolutePath>>(8);
//     let routing_addr = RoutingAddr::remote(0);
//     let uri = RelativeUri::try_from("/foo/example".to_string()).unwrap();
//     let mut client_router = ClientRouterFactory::new(request_tx).create_for(routing_addr);
//
//     let result = client_router.lookup(None, uri, None).await.unwrap();
//
//     assert_eq!(result, RoutingAddr::client());
// }
//
// #[tokio::test]
// async fn test_client_router_resolve_sender() {
//     let (request_tx, mut request_rx) = mpsc::channel::<DownlinkRoutingRequest<AbsolutePath>>(8);
//     let routing_addr = RoutingAddr::remote(0);
//     let addr = SchemeSocketAddr::new(Scheme::Ws, "192.168.0.1:9001".parse().unwrap());
//     let mut client_router = ClientRouterFactory::new(request_tx).create_for(routing_addr);
//
//     tokio::spawn(async move {
//         match request_rx.recv().await.unwrap() {
//             DownlinkRoutingRequest::Connect { request, origin } => {
//                 assert_eq!(origin, Origin::Remote(addr));
//                 let (outgoing_tx, _outgoing_rx) = mpsc::channel(8);
//                 let (_on_drop_tx, on_drop_rx) = promise::promise();
//                 let _ = request.send(Ok(RawRoute::new(outgoing_tx, on_drop_rx)));
//             }
//             DownlinkRoutingRequest::Subscribe { .. } => {
//                 unreachable!()
//             }
//         }
//     });
//
//     let result = client_router
//         .resolve_sender(RoutingAddr::client(), Some(Origin::Remote(addr)))
//         .await;
//
//     assert!(result.is_ok());
// }
//
// #[tokio::test]
// async fn test_client_router_resolve_sender_router_dropped() {
//     let (request_tx, mut request_rx) = mpsc::channel::<DownlinkRoutingRequest<AbsolutePath>>(8);
//     let routing_addr = RoutingAddr::remote(0);
//     let addr = SchemeSocketAddr::new(Scheme::Ws, "192.168.0.1:9001".parse().unwrap());
//     let mut client_router = ClientRouterFactory::new(request_tx).create_for(routing_addr);
//
//     tokio::spawn(async move {
//         let _ = request_rx.recv().await.unwrap();
//     });
//
//     let result = client_router
//         .resolve_sender(RoutingAddr::client(), Some(Origin::Remote(addr)))
//         .await;
//
//     let router_dropped_err = ResolutionError::router_dropped();
//     assert!(matches!(result, Err(err) if err == router_dropped_err));
// }
//
// #[tokio::test]
// async fn test_client_router_resolve_sender_unresolvable() {
//     let (request_tx, mut request_rx) = mpsc::channel::<DownlinkRoutingRequest<AbsolutePath>>(8);
//     let routing_addr = RoutingAddr::remote(0);
//     let addr = SchemeSocketAddr::new(Scheme::Ws, "192.168.0.1:9001".parse().unwrap());
//     let mut client_router = ClientRouterFactory::new(request_tx).create_for(routing_addr);
//
//     tokio::spawn(async move {
//         match request_rx.recv().await.unwrap() {
//             DownlinkRoutingRequest::Connect { request, origin } => {
//                 assert_eq!(origin, Origin::Remote(addr));
//                 let _ = request.send(Err(Unresolvable(RoutingAddr::remote(0))));
//             }
//             DownlinkRoutingRequest::Subscribe { .. } => {
//                 unreachable!()
//             }
//         }
//     });
//
//     let result = client_router
//         .resolve_sender(RoutingAddr::client(), Some(Origin::Remote(addr)))
//         .await;
//
//     let router_dropped_err =
//         ResolutionError::unresolvable("No active endpoint with ID: Remote(0)".to_string());
//     assert!(matches!(result, Err(err) if err == router_dropped_err));
// }
//
// #[tokio::test]
// async fn test_route_single_remote_outgoing_message_to_single_subscriber() {
//     let host = url::Url::parse("ws://127.0.0.1:9001/").unwrap();
//     let remote_path = Path::Remote(AbsolutePath::new(host.clone(), "/foo", "/bar"));
//     let remote_addr = SchemeSocketAddr::new(Scheme::Ws, "127.0.0.1:9001".parse().unwrap());
//
//     let (router_request_tx, mut remote_rx, _local_rx, (remote_requests, _), _close_tx) =
//         create_connection_manager().await;
//     let _remote_tx = register_connection(&router_request_tx, Origin::Remote(remote_addr)).await;
//     let (sink, _stream) = register_subscriber(&router_request_tx, remote_path).await;
//
//     let envelope = Envelope::sync(String::from("/foo"), String::from("/bar"));
//     send_message(&sink, envelope.clone()).await;
//
//     assert_eq!(remote_rx.recv().await.unwrap().1, envelope);
//     let remote_requests = (*remote_requests.lock().unwrap()).clone();
//     assert_eq!(remote_requests, vec![host])
// }
//
// #[tokio::test]
// async fn test_route_single_local_outgoing_message_to_single_subscriber() {
//     let local_path = Path::Local(RelativePath::new("/foo", "/bar"));
//     let local_addr = RelativeUri::try_from("/foo".to_string()).unwrap();
//
//     let (router_request_tx, _remote_rx, mut local_rx, (_, local_requests), _close_tx) =
//         create_connection_manager().await;
//
//     let _local_tx =
//         register_connection(&router_request_tx, Origin::Local(local_addr.clone())).await;
//     let (sink, _stream) = register_subscriber(&router_request_tx, local_path).await;
//
//     let envelope = Envelope::sync(String::from("/foo"), String::from("/bar"));
//     send_message(&sink, envelope.clone()).await;
//
//     assert_eq!(local_rx.recv().await.unwrap().1, envelope);
//     let local_requests = (*local_requests.lock().unwrap()).clone();
//     assert_eq!(local_requests, vec![local_addr])
// }
//
// #[tokio::test]
// async fn test_route_single_remote_outgoing_message_to_multiple_subscribers_same_host() {
//     let host = url::Url::parse("ws://127.0.0.1:9001/").unwrap();
//     let remote_path = Path::Remote(AbsolutePath::new(host.clone(), "/foo", "/bar"));
//     let remote_addr = SchemeSocketAddr::new(Scheme::Ws, "127.0.0.1:9001".parse().unwrap());
//
//     let (router_request_tx, mut remote_rx, _local_rx, (remote_requests, _), _close_tx) =
//         create_connection_manager().await;
//     let _remote_tx = register_connection(&router_request_tx, Origin::Remote(remote_addr)).await;
//     let (first_sink, _stream) = register_subscriber(&router_request_tx, remote_path.clone()).await;
//     let (second_sink, _stream) = register_subscriber(&router_request_tx, remote_path).await;
//
//     let envelope = Envelope::make_event(
//         String::from("oof"),
//         String::from("rab"),
//         Some(Value::text("bye")),
//     );
//     send_message(&first_sink, envelope.clone()).await;
//     send_message(&second_sink, envelope.clone()).await;
//
//     assert_eq!(remote_rx.recv().await.unwrap().1, envelope);
//     assert_eq!(remote_rx.recv().await.unwrap().1, envelope);
//     let remote_requests = (*remote_requests.lock().unwrap()).clone();
//     assert_eq!(remote_requests, vec![host.clone(), host])
// }
//
// #[tokio::test]
// async fn test_route_single_local_outgoing_message_to_multiple_subscribers_same_node() {
//     let local_path = Path::Local(RelativePath::new("/foo", "/bar"));
//     let local_addr = RelativeUri::try_from("/foo".to_string()).unwrap();
//
//     let (router_request_tx, _remote_rx, mut local_rx, (_, local_requests), _close_tx) =
//         create_connection_manager().await;
//
//     let _local_tx =
//         register_connection(&router_request_tx, Origin::Local(local_addr.clone())).await;
//     let (first_sink, _stream) = register_subscriber(&router_request_tx, local_path.clone()).await;
//     let (second_sink, _stream) = register_subscriber(&router_request_tx, local_path).await;
//
//     let envelope = Envelope::make_event(
//         String::from("oof"),
//         String::from("rab"),
//         Some(Value::text("bye")),
//     );
//     send_message(&first_sink, envelope.clone()).await;
//     send_message(&second_sink, envelope.clone()).await;
//
//     assert_eq!(local_rx.recv().await.unwrap().1, envelope);
//     assert_eq!(local_rx.recv().await.unwrap().1, envelope);
//
//     let local_requests = (*local_requests.lock().unwrap()).clone();
//     assert_eq!(local_requests, vec![local_addr.clone(), local_addr])
// }
//
// #[tokio::test]
// async fn test_route_single_remote_outgoing_message_to_multiple_subscribers_different_hosts() {
//     let first_host = url::Url::parse("ws://127.0.0.1:9001/").unwrap();
//     let second_host = url::Url::parse("ws://127.0.0.2:9001/").unwrap();
//
//     let first_remote_path = Path::Remote(AbsolutePath::new(first_host.clone(), "/foo", "/bar"));
//     let second_remote_path = Path::Remote(AbsolutePath::new(second_host.clone(), "/foo", "/bar"));
//     let first_remote_addr = SchemeSocketAddr::new(Scheme::Ws, "127.0.0.1:9001".parse().unwrap());
//     let second_remote_addr = SchemeSocketAddr::new(Scheme::Ws, "127.0.0.1:9002".parse().unwrap());
//
//     let (router_request_tx, mut remote_rx, _local_rx, (remote_requests, _), _close_tx) =
//         create_connection_manager().await;
//     let _first_remote_tx =
//         register_connection(&router_request_tx, Origin::Remote(first_remote_addr)).await;
//     let _second_remote_tx =
//         register_connection(&router_request_tx, Origin::Remote(second_remote_addr)).await;
//     let (first_sink, _stream) = register_subscriber(&router_request_tx, first_remote_path).await;
//     let (second_sink, _stream) = register_subscriber(&router_request_tx, second_remote_path).await;
//
//     let envelope = Envelope::make_event(
//         String::from("/foo"),
//         String::from("/bar"),
//         Some(Value::text("hello")),
//     );
//     send_message(&first_sink, envelope.clone()).await;
//     send_message(&second_sink, envelope.clone()).await;
//
//     assert_eq!(remote_rx.recv().await.unwrap().1, envelope);
//     assert_eq!(remote_rx.recv().await.unwrap().1, envelope);
//
//     let remote_requests = (*remote_requests.lock().unwrap()).clone();
//     assert_eq!(remote_requests, vec![first_host, second_host])
// }
//
// #[tokio::test]
// async fn test_route_single_local_outgoing_message_to_multiple_subscribers_different_nodes() {
//     let first_local_path = Path::Local(RelativePath::new("/foo", "/bar"));
//     let second_local_path = Path::Local(RelativePath::new("/baz", "/qux"));
//     let first_local_addr = RelativeUri::try_from("/foo".to_string()).unwrap();
//     let second_local_addr = RelativeUri::try_from("/baz".to_string()).unwrap();
//
//     let (router_request_tx, _remote_rx, mut local_rx, (_, local_requests), _close_tx) =
//         create_connection_manager().await;
//
//     let _local_tx =
//         register_connection(&router_request_tx, Origin::Local(first_local_addr.clone())).await;
//     let _local_tx =
//         register_connection(&router_request_tx, Origin::Local(second_local_addr.clone())).await;
//     let (first_sink, _stream) = register_subscriber(&router_request_tx, first_local_path).await;
//     let (second_sink, _stream) = register_subscriber(&router_request_tx, second_local_path).await;
//
//     let first_envelope = Envelope::make_event(
//         String::from("/foo"),
//         String::from("/bar"),
//         Some(Value::text("hello")),
//     );
//     let second_envelope = Envelope::make_event(
//         String::from("baz"),
//         String::from("qux"),
//         Some(Value::text("hello")),
//     );
//     send_message(&first_sink, first_envelope.clone()).await;
//     send_message(&second_sink, second_envelope.clone()).await;
//
//     assert_eq!(local_rx.recv().await.unwrap().1, first_envelope);
//     assert_eq!(local_rx.recv().await.unwrap().1, second_envelope);
//
//     let local_requests = (*local_requests.lock().unwrap()).clone();
//     assert_eq!(local_requests, vec![first_local_addr, second_local_addr])
// }
//
// #[tokio::test]
// async fn test_route_multiple_remote_outgoing_messages_to_single_subscriber() {
//     let host = url::Url::parse("ws://127.0.0.1:9001/").unwrap();
//
//     let remote_path = Path::Remote(AbsolutePath::new(host.clone(), "/foo", "/bar"));
//     let remote_addr = SchemeSocketAddr::new(Scheme::Ws, "127.0.0.1:9001".parse().unwrap());
//
//     let (router_request_tx, mut remote_rx, _local_rx, (remote_requests, _), _close_tx) =
//         create_connection_manager().await;
//     let _remote_tx = register_connection(&router_request_tx, Origin::Remote(remote_addr)).await;
//     let (sink, _stream) = register_subscriber(&router_request_tx, remote_path).await;
//
//     let first_envelope = Envelope::make_event(
//         String::from("/foo"),
//         String::from("/bar"),
//         Some(Value::text("First_Subscriber")),
//     );
//
//     let second_envelope = Envelope::make_event(
//         String::from("/foo"),
//         String::from("/bar"),
//         Some(Value::text("Second_Subscriber")),
//     );
//     send_message(&sink, first_envelope.clone()).await;
//     send_message(&sink, second_envelope.clone()).await;
//
//     assert_eq!(remote_rx.recv().await.unwrap().1, first_envelope);
//     assert_eq!(remote_rx.recv().await.unwrap().1, second_envelope);
//
//     let remote_requests = (*remote_requests.lock().unwrap()).clone();
//     assert_eq!(remote_requests, vec![host])
// }
//
// #[tokio::test]
// async fn test_route_multiple_local_outgoing_messages_to_single_subscriber() {
//     let local_path = Path::Local(RelativePath::new("/foo", "/bar"));
//     let local_addr = RelativeUri::try_from("/foo".to_string()).unwrap();
//
//     let (router_request_tx, _remote_rx, mut local_rx, (_, local_requests), _close_tx) =
//         create_connection_manager().await;
//
//     let _local_tx =
//         register_connection(&router_request_tx, Origin::Local(local_addr.clone())).await;
//     let (sink, _stream) = register_subscriber(&router_request_tx, local_path).await;
//
//     let first_envelope = Envelope::make_event(
//         String::from("/foo"),
//         String::from("/bar"),
//         Some(Value::text("First_Subscriber")),
//     );
//
//     let second_envelope = Envelope::make_event(
//         String::from("/foo"),
//         String::from("/bar"),
//         Some(Value::text("Second_Subscriber")),
//     );
//     send_message(&sink, first_envelope.clone()).await;
//     send_message(&sink, second_envelope.clone()).await;
//
//     assert_eq!(local_rx.recv().await.unwrap().1, first_envelope);
//     assert_eq!(local_rx.recv().await.unwrap().1, second_envelope);
//
//     let local_requests = (*local_requests.lock().unwrap()).clone();
//     assert_eq!(local_requests, vec![local_addr])
// }
//
// #[tokio::test]
// async fn test_route_multiple_remote_outgoing_messages_to_multiple_subscribers_same_host() {
//     let host = url::Url::parse("ws://127.0.0.1:9001/").unwrap();
//
//     let remote_path = Path::Remote(AbsolutePath::new(host.clone(), "/foo", "/bar"));
//     let remote_addr = SchemeSocketAddr::new(Scheme::Ws, "127.0.0.1:9001".parse().unwrap());
//
//     let (router_request_tx, mut remote_rx, _local_rx, (remote_requests, _), _close_tx) =
//         create_connection_manager().await;
//     let _remote_tx = register_connection(&router_request_tx, Origin::Remote(remote_addr)).await;
//     let (first_sink, _stream) = register_subscriber(&router_request_tx, remote_path.clone()).await;
//     let (second_sink, _stream) = register_subscriber(&router_request_tx, remote_path).await;
//
//     let first_envelope = Envelope::make_event(
//         String::from("/foo"),
//         String::from("/first_bar"),
//         Some(Value::text("first_body")),
//     );
//
//     let second_envelope = Envelope::make_event(
//         String::from("/foo"),
//         String::from("/second_bar"),
//         Some(Value::text("second_body")),
//     );
//
//     let third_envelope = Envelope::make_event(
//         String::from("/foo"),
//         String::from("/third_bar"),
//         Some(Value::text("third_body")),
//     );
//
//     send_message(&first_sink, first_envelope.clone()).await;
//     send_message(&first_sink, second_envelope.clone()).await;
//     send_message(&second_sink, third_envelope.clone()).await;
//
//     assert_eq!(remote_rx.recv().await.unwrap().1, first_envelope);
//     assert_eq!(remote_rx.recv().await.unwrap().1, second_envelope);
//     assert_eq!(remote_rx.recv().await.unwrap().1, third_envelope);
//
//     let remote_requests = (*remote_requests.lock().unwrap()).clone();
//     assert_eq!(remote_requests, vec![host.clone(), host])
// }
//
// #[tokio::test]
// async fn test_route_multiple_local_outgoing_messages_to_multiple_subscribers_same_node() {
//     let local_path = Path::Local(RelativePath::new("/foo", "/bar"));
//     let local_addr = RelativeUri::try_from("/foo".to_string()).unwrap();
//
//     let (router_request_tx, _remote_rx, mut local_rx, (_, local_requests), _close_tx) =
//         create_connection_manager().await;
//
//     let _local_tx =
//         register_connection(&router_request_tx, Origin::Local(local_addr.clone())).await;
//     let (first_sink, _stream) = register_subscriber(&router_request_tx, local_path.clone()).await;
//     let (second_sink, _stream) = register_subscriber(&router_request_tx, local_path).await;
//
//     let first_envelope = Envelope::make_event(
//         String::from("/foo"),
//         String::from("/first_bar"),
//         Some(Value::text("first_body")),
//     );
//
//     let second_envelope = Envelope::make_event(
//         String::from("/foo"),
//         String::from("/second_bar"),
//         Some(Value::text("second_body")),
//     );
//
//     let third_envelope = Envelope::make_event(
//         String::from("/foo"),
//         String::from("/third_bar"),
//         Some(Value::text("third_body")),
//     );
//
//     send_message(&first_sink, first_envelope.clone()).await;
//     send_message(&first_sink, second_envelope.clone()).await;
//     send_message(&second_sink, third_envelope.clone()).await;
//
//     assert_eq!(local_rx.recv().await.unwrap().1, first_envelope);
//     assert_eq!(local_rx.recv().await.unwrap().1, second_envelope);
//     assert_eq!(local_rx.recv().await.unwrap().1, third_envelope);
//
//     let local_requests = (*local_requests.lock().unwrap()).clone();
//     assert_eq!(local_requests, vec![local_addr.clone(), local_addr])
// }
//
// #[tokio::test]
// async fn test_route_multiple_remote_outgoing_messages_to_multiple_subscribers_different_hosts() {
//     let first_host = url::Url::parse("wss://127.0.0.1:9001/").unwrap();
//     let second_host = url::Url::parse("wss://127.0.0.2:9001/").unwrap();
//
//     let first_remote_path = Path::Remote(AbsolutePath::new(first_host.clone(), "/foo", "/bar"));
//     let second_remote_path = Path::Remote(AbsolutePath::new(second_host.clone(), "/foo", "/bar"));
//
//     let first_remote_addr = SchemeSocketAddr::new(Scheme::Wss, "127.0.0.1:9001".parse().unwrap());
//     let second_remote_addr = SchemeSocketAddr::new(Scheme::Wss, "127.0.0.2:9001".parse().unwrap());
//
//     let (router_request_tx, mut remote_rx, _local_rx, (remote_requests, _), _close_tx) =
//         create_connection_manager().await;
//     let _first_remote_tx =
//         register_connection(&router_request_tx, Origin::Remote(first_remote_addr)).await;
//     let _second_remote_tx =
//         register_connection(&router_request_tx, Origin::Remote(second_remote_addr)).await;
//     let (first_sink, _stream) = register_subscriber(&router_request_tx, first_remote_path).await;
//     let (second_sink, _stream) = register_subscriber(&router_request_tx, second_remote_path).await;
//
//     let first_envelope = Envelope::make_event(
//         String::from("/foo"),
//         String::from("/first_bar"),
//         Some(Value::text("first_body")),
//     );
//
//     let second_envelope = Envelope::make_event(
//         String::from("/foo"),
//         String::from("/second_bar"),
//         Some(Value::text("second_body")),
//     );
//
//     let third_envelope = Envelope::make_event(
//         String::from("/foo"),
//         String::from("/third_bar"),
//         Some(Value::text("third_body")),
//     );
//
//     send_message(&first_sink, first_envelope.clone()).await;
//     send_message(&first_sink, second_envelope.clone()).await;
//     send_message(&second_sink, third_envelope.clone()).await;
//
//     assert_eq!(remote_rx.recv().await.unwrap().1, first_envelope);
//     assert_eq!(remote_rx.recv().await.unwrap().1, second_envelope);
//     assert_eq!(remote_rx.recv().await.unwrap().1, third_envelope);
//
//     let remote_requests = (*remote_requests.lock().unwrap()).clone();
//     assert_eq!(remote_requests, vec![first_host, second_host])
// }
//
// #[tokio::test]
// async fn test_route_multiple_local_outgoing_messages_to_multiple_subscribers_different_nodes() {
//     let first_local_path = Path::Local(RelativePath::new("/foo", "/bar"));
//     let second_local_path = Path::Local(RelativePath::new("/baz", "/qux"));
//     let first_local_addr = RelativeUri::try_from("/foo".to_string()).unwrap();
//     let second_local_addr = RelativeUri::try_from("/baz".to_string()).unwrap();
//
//     let (router_request_tx, _remote_rx, mut local_rx, (_, local_requests), _close_tx) =
//         create_connection_manager().await;
//
//     let _first_local_tx =
//         register_connection(&router_request_tx, Origin::Local(first_local_addr.clone())).await;
//     let _second_local_tx =
//         register_connection(&router_request_tx, Origin::Local(second_local_addr.clone())).await;
//     let (first_sink, _stream) = register_subscriber(&router_request_tx, first_local_path).await;
//     let (second_sink, _stream) = register_subscriber(&router_request_tx, second_local_path).await;
//
//     let first_envelope = Envelope::make_event(
//         String::from("/foo"),
//         String::from("/first_bar"),
//         Some(Value::text("first_body")),
//     );
//
//     let second_envelope = Envelope::make_event(
//         String::from("/foo"),
//         String::from("/second_bar"),
//         Some(Value::text("second_body")),
//     );
//
//     let third_envelope = Envelope::make_event(
//         String::from("/baz"),
//         String::from("/third_bar"),
//         Some(Value::text("third_body")),
//     );
//
//     send_message(&first_sink, first_envelope.clone()).await;
//     send_message(&first_sink, second_envelope.clone()).await;
//     send_message(&second_sink, third_envelope.clone()).await;
//
//     assert_eq!(local_rx.recv().await.unwrap().1, first_envelope);
//     assert_eq!(local_rx.recv().await.unwrap().1, second_envelope);
//     assert_eq!(local_rx.recv().await.unwrap().1, third_envelope);
//
//     let local_requests = (*local_requests.lock().unwrap()).clone();
//     assert_eq!(local_requests, vec![first_local_addr, second_local_addr]);
// }
//
// #[tokio::test]
// async fn test_route_single_remote_incoming_message_to_single_subscriber() {
//     let host = url::Url::parse("wss://127.0.0.1:9001/").unwrap();
//     let remote_path = Path::Remote(AbsolutePath::new(host.clone(), "/foo", "/bar"));
//
//     let remote_addr = SchemeSocketAddr::new(Scheme::Wss, "127.0.0.1:9001".parse().unwrap());
//
//     let (router_request_tx, _remote_rx, _local_rx, (remote_requests, _), _close_tx) =
//         create_connection_manager().await;
//
//     let remote_tx = register_connection(&router_request_tx, Origin::Remote(remote_addr)).await;
//     let (_sink, mut stream) = register_subscriber(&router_request_tx, remote_path).await;
//
//     let envelope = Envelope::make_event(
//         String::from("/foo"),
//         String::from("/bar"),
//         Some(Value::text("Hello")),
//     );
//
//     send_message(&remote_tx, envelope.clone()).await;
//
//     assert_eq!(stream.recv().await.unwrap(), envelope);
//     let remote_requests = (*remote_requests.lock().unwrap()).clone();
//     assert_eq!(remote_requests, vec![host]);
// }
//
// #[tokio::test]
// async fn test_route_single_local_incoming_message_to_single_node() {
//     let local_path = Path::Local(RelativePath::new("/foo", "/bar"));
//     let local_addr = RelativeUri::try_from("/foo".to_string()).unwrap();
//
//     let (router_request_tx, _remote_rx, _local_rx, (_, local_requests), _close_tx) =
//         create_connection_manager().await;
//
//     let local_tx = register_connection(&router_request_tx, Origin::Local(local_addr.clone())).await;
//     let (_sink, mut stream) = register_subscriber(&router_request_tx, local_path).await;
//
//     let envelope = Envelope::make_event(
//         String::from("/foo"),
//         String::from("/bar"),
//         Some(Value::text("Hello")),
//     );
//     send_message(&local_tx, envelope.clone()).await;
//
//     assert_eq!(stream.recv().await.unwrap(), envelope);
//     let local_requests = (*local_requests.lock().unwrap()).clone();
//     assert_eq!(local_requests, vec![local_addr]);
// }
//
// #[tokio::test]
// async fn test_route_single_remote_incoming_message_to_multiple_subscribers_same_host_same_path() {
//     let host = url::Url::parse("wss://127.0.0.1:9001/").unwrap();
//     let remote_path = Path::Remote(AbsolutePath::new(host.clone(), "/foo", "/bar"));
//
//     let remote_addr = SchemeSocketAddr::new(Scheme::Wss, "127.0.0.1:9001".parse().unwrap());
//
//     let (router_request_tx, _remote_rx, _local_rx, (remote_requests, _), _close_tx) =
//         create_connection_manager().await;
//
//     let remote_tx = register_connection(&router_request_tx, Origin::Remote(remote_addr)).await;
//     let (_sink, mut first_stream) =
//         register_subscriber(&router_request_tx, remote_path.clone()).await;
//     let (_sink, mut second_stream) = register_subscriber(&router_request_tx, remote_path).await;
//
//     let envelope = Envelope::make_event(
//         String::from("/foo"),
//         String::from("/bar"),
//         Some(Value::text("Goodbye")),
//     );
//
//     send_message(&remote_tx, envelope.clone()).await;
//
//     assert_eq!(first_stream.recv().await.unwrap(), envelope);
//     assert_eq!(second_stream.recv().await.unwrap(), envelope);
//     let remote_requests = (*remote_requests.lock().unwrap()).clone();
//     assert_eq!(remote_requests, vec![host.clone(), host]);
// }
//
// #[tokio::test]
// async fn test_route_single_local_incoming_message_to_multiple_nodes_same_path() {
//     let local_path = Path::Local(RelativePath::new("/foo", "/bar"));
//     let local_addr = RelativeUri::try_from("/foo".to_string()).unwrap();
//
//     let (router_request_tx, _remote_rx, _local_rx, (_, local_requests), _close_tx) =
//         create_connection_manager().await;
//
//     let local_tx = register_connection(&router_request_tx, Origin::Local(local_addr.clone())).await;
//     let (_sink, mut first_stream) =
//         register_subscriber(&router_request_tx, local_path.clone()).await;
//     let (_sink, mut second_stream) = register_subscriber(&router_request_tx, local_path).await;
//
//     let envelope = Envelope::make_event(
//         String::from("/foo"),
//         String::from("/bar"),
//         Some(Value::text("Goodbye")),
//     );
//
//     send_message(&local_tx, envelope.clone()).await;
//
//     assert_eq!(first_stream.recv().await.unwrap(), envelope);
//     assert_eq!(second_stream.recv().await.unwrap(), envelope);
//     let local_requests = (*local_requests.lock().unwrap()).clone();
//     assert_eq!(local_requests, vec![local_addr.clone(), local_addr]);
// }
//
// #[tokio::test]
// async fn test_route_single_remote_incoming_message_to_multiple_subscribers_same_host_different_paths(
// ) {
//     let host = url::Url::parse("ws://127.0.0.1:9001/").unwrap();
//     let first_remote_path = Path::Remote(AbsolutePath::new(host.clone(), "/foo", "/bar"));
//     let second_remote_path = Path::Remote(AbsolutePath::new(host.clone(), "/oof", "/rab"));
//
//     let remote_addr = SchemeSocketAddr::new(Scheme::Ws, "127.0.0.1:9001".parse().unwrap());
//
//     let (router_request_tx, _remote_rx, _local_rx, (remote_requests, _), _close_tx) =
//         create_connection_manager().await;
//
//     let remote_tx = register_connection(&router_request_tx, Origin::Remote(remote_addr)).await;
//     let (_sink, mut first_stream) =
//         register_subscriber(&router_request_tx, first_remote_path).await;
//     let (_sink, mut second_stream) =
//         register_subscriber(&router_request_tx, second_remote_path).await;
//
//     let envelope = Envelope::make_event(
//         String::from("/foo"),
//         String::from("/bar"),
//         Some(Value::text("tseT")),
//     );
//
//     send_message(&remote_tx, envelope.clone()).await;
//
//     assert_eq!(first_stream.recv().await.unwrap(), envelope);
//     assert_eq!(second_stream.recv().await.unwrap(), envelope);
//     let remote_requests = (*remote_requests.lock().unwrap()).clone();
//     assert_eq!(remote_requests, vec![host.clone(), host]);
// }
//
// #[tokio::test]
// async fn test_route_single_local_incoming_message_to_multiple_subscribers_same_node_different_paths(
// ) {
//     let first_local_path = Path::Local(RelativePath::new("/foo", "/bar"));
//     let second_local_path = Path::Local(RelativePath::new("/foo", "/qux"));
//     let local_addr = RelativeUri::try_from("/foo".to_string()).unwrap();
//
//     let (router_request_tx, _remote_rx, _local_rx, (_, local_requests), _close_tx) =
//         create_connection_manager().await;
//
//     let local_tx = register_connection(&router_request_tx, Origin::Local(local_addr.clone())).await;
//     let (_sink, mut first_stream) = register_subscriber(&router_request_tx, first_local_path).await;
//     let (_sink, mut second_stream) =
//         register_subscriber(&router_request_tx, second_local_path).await;
//
//     let envelope = Envelope::make_event(
//         String::from("/foo"),
//         String::from("/bar"),
//         Some(Value::text("tseT")),
//     );
//
//     send_message(&local_tx, envelope.clone()).await;
//
//     assert_eq!(first_stream.recv().await.unwrap(), envelope);
//     assert_eq!(second_stream.recv().await.unwrap(), envelope);
//     let local_requests = (*local_requests.lock().unwrap()).clone();
//     assert_eq!(local_requests, vec![local_addr.clone(), local_addr]);
// }
//
// #[tokio::test]
// async fn test_route_single_remote_incoming_message_to_multiple_subscribers_different_hosts_same_path(
// ) {
//     let first_host = url::Url::parse("ws://127.0.0.1:9001/").unwrap();
//     let second_host = url::Url::parse("ws://127.0.0.1:9002/").unwrap();
//
//     let first_remote_path = Path::Remote(AbsolutePath::new(first_host.clone(), "/foo", "/bar"));
//     let second_remote_path = Path::Remote(AbsolutePath::new(second_host.clone(), "/foo", "/bar"));
//
//     let first_remote_addr = SchemeSocketAddr::new(Scheme::Ws, "127.0.0.1:9001".parse().unwrap());
//     let second_remote_addr = SchemeSocketAddr::new(Scheme::Ws, "127.0.0.1:9002".parse().unwrap());
//
//     let (router_request_tx, _remote_rx, _local_rx, (remote_requests, _), _close_tx) =
//         create_connection_manager().await;
//
//     let first_remote_tx = register_connection(
//         &router_request_tx,
//         Origin::Remote(first_remote_addr.clone()),
//     )
//     .await;
//     let second_remote_tx = register_connection(
//         &router_request_tx,
//         Origin::Remote(second_remote_addr.clone()),
//     )
//     .await;
//
//     let (_sink, mut first_stream) =
//         register_subscriber(&router_request_tx, first_remote_path).await;
//     let (_sink, mut second_stream) =
//         register_subscriber(&router_request_tx, second_remote_path).await;
//
//     let first_envelope = Envelope::make_event(
//         String::from("/foo"),
//         String::from("/bar"),
//         Some(Value::text("First Hello")),
//     );
//
//     let second_envelope = Envelope::make_event(
//         String::from("/foo"),
//         String::from("/bar"),
//         Some(Value::text("Second Hello")),
//     );
//
//     send_message(&first_remote_tx, first_envelope.clone()).await;
//     send_message(&second_remote_tx, second_envelope.clone()).await;
//
//     assert_eq!(first_stream.recv().await.unwrap(), first_envelope);
//     assert_eq!(second_stream.recv().await.unwrap(), second_envelope);
//     let remote_requests = (*remote_requests.lock().unwrap()).clone();
//     assert_eq!(remote_requests, vec![first_host, second_host]);
// }
//
// #[tokio::test]
// async fn test_route_single_local_incoming_message_to_multiple_subscribers_different_nodes_same_path(
// ) {
//     let first_local_path = Path::Local(RelativePath::new("/foo", "/bar"));
//     let second_local_path = Path::Local(RelativePath::new("/baz", "/bar"));
//     let first_local_addr = RelativeUri::try_from("/foo".to_string()).unwrap();
//     let second_local_addr = RelativeUri::try_from("/baz".to_string()).unwrap();
//
//     let (router_request_tx, _remote_rx, _local_rx, (_, local_requests), _close_tx) =
//         create_connection_manager().await;
//
//     let first_local_tx =
//         register_connection(&router_request_tx, Origin::Local(first_local_addr.clone())).await;
//     let second_local_tx =
//         register_connection(&router_request_tx, Origin::Local(second_local_addr.clone())).await;
//     let (_sink, mut first_stream) = register_subscriber(&router_request_tx, first_local_path).await;
//     let (_sink, mut second_stream) =
//         register_subscriber(&router_request_tx, second_local_path).await;
//
//     let first_envelope = Envelope::make_event(
//         String::from("/foo"),
//         String::from("/bar"),
//         Some(Value::text("First Hello")),
//     );
//
//     let second_envelope = Envelope::make_event(
//         String::from("/baz"),
//         String::from("/bar"),
//         Some(Value::text("Second Hello")),
//     );
//
//     send_message(&first_local_tx, first_envelope.clone()).await;
//     send_message(&second_local_tx, second_envelope.clone()).await;
//
//     assert_eq!(first_stream.recv().await.unwrap(), first_envelope);
//     assert_eq!(second_stream.recv().await.unwrap(), second_envelope);
//     let local_requests = (*local_requests.lock().unwrap()).clone();
//     assert_eq!(local_requests, vec![first_local_addr, second_local_addr]);
// }
//
// #[tokio::test]
// async fn test_route_single_remote_incoming_message_to_multiple_subscribers_different_hosts_different_paths(
// ) {
//     let first_host = url::Url::parse("ws://127.0.0.1:9001/").unwrap();
//     let second_host = url::Url::parse("ws://127.0.0.2:9001/").unwrap();
//     let third_host = url::Url::parse("ws://127.0.0.3:9001/").unwrap();
//
//     let first_remote_path = Path::Remote(AbsolutePath::new(first_host.clone(), "/foo", "/bar"));
//     let second_remote_path = Path::Remote(AbsolutePath::new(second_host.clone(), "/oof", "/rab"));
//     let third_remote_path = Path::Remote(AbsolutePath::new(third_host.clone(), "/ofo", "/abr"));
//
//     let first_remote_addr = SchemeSocketAddr::new(Scheme::Ws, "127.0.0.1:9001".parse().unwrap());
//     let second_remote_addr = SchemeSocketAddr::new(Scheme::Ws, "127.0.0.2:9001".parse().unwrap());
//     let third_remote_addr = SchemeSocketAddr::new(Scheme::Ws, "127.0.0.3:9001".parse().unwrap());
//
//     let (router_request_tx, _remote_rx, _local_rx, (remote_requests, _), _close_tx) =
//         create_connection_manager().await;
//
//     let first_remote_tx = register_connection(
//         &router_request_tx,
//         Origin::Remote(first_remote_addr.clone()),
//     )
//     .await;
//     let second_remote_tx = register_connection(
//         &router_request_tx,
//         Origin::Remote(second_remote_addr.clone()),
//     )
//     .await;
//     let third_remote_tx = register_connection(
//         &router_request_tx,
//         Origin::Remote(third_remote_addr.clone()),
//     )
//     .await;
//
//     let (_sink, mut first_stream) =
//         register_subscriber(&router_request_tx, first_remote_path).await;
//     let (_sink, mut second_stream) =
//         register_subscriber(&router_request_tx, second_remote_path).await;
//     let (_sink, mut third_stream) =
//         register_subscriber(&router_request_tx, third_remote_path).await;
//
//     let first_envelope = Envelope::make_event(
//         String::from("/foo"),
//         String::from("/bar"),
//         Some(Value::text("Hello First")),
//     );
//
//     let second_envelope = Envelope::make_event(
//         String::from("/oof"),
//         String::from("/rab"),
//         Some(Value::text("Hello Second")),
//     );
//
//     let third_envelope = Envelope::make_event(
//         String::from("/ofo"),
//         String::from("/abr"),
//         Some(Value::text("Hello Third")),
//     );
//
//     send_message(&first_remote_tx, first_envelope.clone()).await;
//     send_message(&second_remote_tx, second_envelope.clone()).await;
//     send_message(&third_remote_tx, third_envelope.clone()).await;
//
//     assert_eq!(first_stream.recv().await.unwrap(), first_envelope);
//     assert_eq!(second_stream.recv().await.unwrap(), second_envelope);
//     assert_eq!(third_stream.recv().await.unwrap(), third_envelope);
//     let remote_requests = (*remote_requests.lock().unwrap()).clone();
//     assert_eq!(remote_requests, vec![first_host, second_host, third_host]);
// }
//
// #[tokio::test]
// async fn test_route_single_local_incoming_message_to_multiple_subscribers_different_nodes_different_paths(
// ) {
//     let first_local_path = Path::Local(RelativePath::new("/foo", "/bar"));
//     let second_local_path = Path::Local(RelativePath::new("/oof", "/rab"));
//     let third_local_path = Path::Local(RelativePath::new("/ofo", "/abr"));
//
//     let first_local_addr = RelativeUri::try_from("/foo".to_string()).unwrap();
//     let second_local_addr = RelativeUri::try_from("/oof".to_string()).unwrap();
//     let third_local_addr = RelativeUri::try_from("/ofo".to_string()).unwrap();
//
//     let (router_request_tx, _remote_rx, _local_rx, (_, local_requests), _close_tx) =
//         create_connection_manager().await;
//
//     let first_local_tx =
//         register_connection(&router_request_tx, Origin::Local(first_local_addr.clone())).await;
//     let second_local_tx =
//         register_connection(&router_request_tx, Origin::Local(second_local_addr.clone())).await;
//     let third_local_tx =
//         register_connection(&router_request_tx, Origin::Local(third_local_addr.clone())).await;
//
//     let (_sink, mut first_stream) = register_subscriber(&router_request_tx, first_local_path).await;
//     let (_sink, mut second_stream) =
//         register_subscriber(&router_request_tx, second_local_path).await;
//     let (_sink, mut third_stream) = register_subscriber(&router_request_tx, third_local_path).await;
//
//     let first_envelope = Envelope::make_event(
//         String::from("/foo"),
//         String::from("/bar"),
//         Some(Value::text("Hello First")),
//     );
//
//     let second_envelope = Envelope::make_event(
//         String::from("/oof"),
//         String::from("/rab"),
//         Some(Value::text("Hello Second")),
//     );
//
//     let third_envelope = Envelope::make_event(
//         String::from("/ofo"),
//         String::from("/abr"),
//         Some(Value::text("Hello Third")),
//     );
//
//     send_message(&first_local_tx, first_envelope.clone()).await;
//     send_message(&second_local_tx, second_envelope.clone()).await;
//     send_message(&third_local_tx, third_envelope.clone()).await;
//
//     assert_eq!(first_stream.recv().await.unwrap(), first_envelope);
//     assert_eq!(second_stream.recv().await.unwrap(), second_envelope);
//     assert_eq!(third_stream.recv().await.unwrap(), third_envelope);
//     let local_requests = (*local_requests.lock().unwrap()).clone();
//     assert_eq!(
//         local_requests,
//         vec![first_local_addr, second_local_addr, third_local_addr]
//     );
// }
//
// #[tokio::test]
// async fn test_route_multiple_remote_incoming_messages_to_single_subscriber() {
//     let host = url::Url::parse("ws://127.0.0.1:9001/").unwrap();
//
//     let remote_path = Path::Remote(AbsolutePath::new(host.clone(), "/foo", "/bar"));
//
//     let remote_addr = SchemeSocketAddr::new(Scheme::Ws, "127.0.0.1:9001".parse().unwrap());
//
//     let (router_request_tx, _remote_rx, _local_rx, (remote_requests, _), _close_tx) =
//         create_connection_manager().await;
//
//     let remote_tx =
//         register_connection(&router_request_tx, Origin::Remote(remote_addr.clone())).await;
//
//     let (_sink, mut stream) = register_subscriber(&router_request_tx, remote_path).await;
//
//     let first_envelope = Envelope::make_event(
//         String::from("/foo"),
//         String::from("/bar"),
//         Some(Value::text("First!")),
//     );
//
//     let second_envelope = Envelope::make_event(
//         String::from("/foo"),
//         String::from("/bar"),
//         Some(Value::text("Second!")),
//     );
//
//     send_message(&remote_tx, first_envelope.clone()).await;
//     send_message(&remote_tx, second_envelope.clone()).await;
//
//     assert_eq!(stream.recv().await.unwrap(), first_envelope);
//     assert_eq!(stream.recv().await.unwrap(), second_envelope);
//     let remote_requests = (*remote_requests.lock().unwrap()).clone();
//     assert_eq!(remote_requests, vec![host]);
// }
//
// #[tokio::test]
// async fn test_route_multiple_local_incoming_messages_to_single_subscriber() {
//     let local_path = Path::Local(RelativePath::new("/foo", "/bar"));
//     let local_addr = RelativeUri::try_from("/foo".to_string()).unwrap();
//
//     let (router_request_tx, _remote_rx, _local_rx, (_, local_requests), _close_tx) =
//         create_connection_manager().await;
//
//     let local_tx = register_connection(&router_request_tx, Origin::Local(local_addr.clone())).await;
//
//     let (_sink, mut stream) = register_subscriber(&router_request_tx, local_path).await;
//
//     let first_envelope = Envelope::make_event(
//         String::from("/foo"),
//         String::from("/bar"),
//         Some(Value::text("First!")),
//     );
//
//     let second_envelope = Envelope::make_event(
//         String::from("/foo"),
//         String::from("/bar"),
//         Some(Value::text("Second!")),
//     );
//
//     send_message(&local_tx, first_envelope.clone()).await;
//     send_message(&local_tx, second_envelope.clone()).await;
//
//     assert_eq!(stream.recv().await.unwrap(), first_envelope);
//     assert_eq!(stream.recv().await.unwrap(), second_envelope);
//     let local_requests = (*local_requests.lock().unwrap()).clone();
//     assert_eq!(local_requests, vec![local_addr]);
// }
//
// #[tokio::test]
// async fn test_route_multiple_remote_incoming_messages_to_multiple_subscribers_same_host_same_path()
// {
//     let host = url::Url::parse("ws://127.0.0.1:9001/").unwrap();
//
//     let remote_path = Path::Remote(AbsolutePath::new(host.clone(), "/room", "/five"));
//     let remote_addr = SchemeSocketAddr::new(Scheme::Ws, "127.0.0.1:9001".parse().unwrap());
//
//     let (router_request_tx, _remote_rx, _local_rx, (remote_requests, _), _close_tx) =
//         create_connection_manager().await;
//
//     let remote_tx =
//         register_connection(&router_request_tx, Origin::Remote(remote_addr.clone())).await;
//
//     let (_sink, mut first_stream) =
//         register_subscriber(&router_request_tx, remote_path.clone()).await;
//     let (_sink, mut second_stream) = register_subscriber(&router_request_tx, remote_path).await;
//
//     let first_envelope = Envelope::make_event(
//         String::from("/room"),
//         String::from("/five"),
//         Some(Value::text("John Doe")),
//     );
//
//     let second_envelope = Envelope::make_event(
//         String::from("/room"),
//         String::from("/five"),
//         Some(Value::text("Jane Doe")),
//     );
//
//     send_message(&remote_tx, first_envelope.clone()).await;
//     send_message(&remote_tx, second_envelope.clone()).await;
//
//     assert_eq!(first_stream.recv().await.unwrap(), first_envelope);
//     assert_eq!(first_stream.recv().await.unwrap(), second_envelope);
//     assert_eq!(second_stream.recv().await.unwrap(), first_envelope);
//     assert_eq!(second_stream.recv().await.unwrap(), second_envelope);
//     let remote_requests = (*remote_requests.lock().unwrap()).clone();
//     assert_eq!(remote_requests, vec![host.clone(), host]);
// }
//
// #[tokio::test]
// async fn test_route_multiple_local_incoming_messages_to_multiple_subscribers_same_node_same_path() {
//     let local_path = Path::Local(RelativePath::new("/room", "/five"));
//     let local_addr = RelativeUri::try_from("/room".to_string()).unwrap();
//
//     let (router_request_tx, _remote_rx, _local_rx, (_, local_requests), _close_tx) =
//         create_connection_manager().await;
//
//     let local_tx = register_connection(&router_request_tx, Origin::Local(local_addr.clone())).await;
//
//     let (_sink, mut first_stream) =
//         register_subscriber(&router_request_tx, local_path.clone()).await;
//     let (_sink, mut second_stream) = register_subscriber(&router_request_tx, local_path).await;
//
//     let first_envelope = Envelope::make_event(
//         String::from("/room"),
//         String::from("/five"),
//         Some(Value::text("John Doe")),
//     );
//
//     let second_envelope = Envelope::make_event(
//         String::from("/room"),
//         String::from("/five"),
//         Some(Value::text("Jane Doe")),
//     );
//
//     send_message(&local_tx, first_envelope.clone()).await;
//     send_message(&local_tx, second_envelope.clone()).await;
//
//     assert_eq!(first_stream.recv().await.unwrap(), first_envelope);
//     assert_eq!(first_stream.recv().await.unwrap(), second_envelope);
//     assert_eq!(second_stream.recv().await.unwrap(), first_envelope);
//     assert_eq!(second_stream.recv().await.unwrap(), second_envelope);
//     let local_requests = (*local_requests.lock().unwrap()).clone();
//     assert_eq!(local_requests, vec![local_addr.clone(), local_addr]);
// }
//
// #[tokio::test]
// async fn test_route_multiple_remote_incoming_messages_to_multiple_subscribers_same_host_different_paths(
// ) {
//     let host = url::Url::parse("ws://127.0.0.1:9001/").unwrap();
//
//     let first_remote_path = Path::Remote(AbsolutePath::new(host.clone(), "/room", "/five"));
//     let second_remote_path = Path::Remote(AbsolutePath::new(host.clone(), "/room", "/six"));
//     let remote_addr = SchemeSocketAddr::new(Scheme::Ws, "127.0.0.1:9001".parse().unwrap());
//
//     let (router_request_tx, _remote_rx, _local_rx, (remote_requests, _), _close_tx) =
//         create_connection_manager().await;
//
//     let remote_tx =
//         register_connection(&router_request_tx, Origin::Remote(remote_addr.clone())).await;
//
//     let (_sink, mut first_stream) =
//         register_subscriber(&router_request_tx, first_remote_path.clone()).await;
//     let (_sink, mut second_stream) =
//         register_subscriber(&router_request_tx, second_remote_path).await;
//
//     let first_envelope = Envelope::make_event(
//         String::from("/room"),
//         String::from("/five"),
//         Some(Value::text("John Doe")),
//     );
//
//     let second_envelope = Envelope::make_event(
//         String::from("/room"),
//         String::from("/six"),
//         Some(Value::text("Jane Doe")),
//     );
//
//     send_message(&remote_tx, first_envelope.clone()).await;
//     send_message(&remote_tx, second_envelope.clone()).await;
//
//     assert_eq!(first_stream.recv().await.unwrap(), first_envelope);
//     assert_eq!(first_stream.recv().await.unwrap(), second_envelope);
//     assert_eq!(second_stream.recv().await.unwrap(), first_envelope);
//     assert_eq!(second_stream.recv().await.unwrap(), second_envelope);
//     let remote_requests = (*remote_requests.lock().unwrap()).clone();
//     assert_eq!(remote_requests, vec![host.clone(), host]);
// }
//
// #[tokio::test]
// async fn test_route_multiple_local_incoming_messages_to_multiple_subscribers_same_node_different_paths(
// ) {
//     let first_local_path = Path::Local(RelativePath::new("/room", "/five"));
//     let second_local_path = Path::Local(RelativePath::new("/room", "/six"));
//     let local_addr = RelativeUri::try_from("/room".to_string()).unwrap();
//
//     let (router_request_tx, _remote_rx, _local_rx, (_, local_requests), _close_tx) =
//         create_connection_manager().await;
//
//     let local_tx = register_connection(&router_request_tx, Origin::Local(local_addr.clone())).await;
//
//     let (_sink, mut first_stream) = register_subscriber(&router_request_tx, first_local_path).await;
//     let (_sink, mut second_stream) =
//         register_subscriber(&router_request_tx, second_local_path).await;
//
//     let first_envelope = Envelope::make_event(
//         String::from("/room"),
//         String::from("/five"),
//         Some(Value::text("John Doe")),
//     );
//
//     let second_envelope = Envelope::make_event(
//         String::from("/room"),
//         String::from("/six"),
//         Some(Value::text("Jane Doe")),
//     );
//
//     send_message(&local_tx, first_envelope.clone()).await;
//     send_message(&local_tx, second_envelope.clone()).await;
//
//     assert_eq!(first_stream.recv().await.unwrap(), first_envelope);
//     assert_eq!(first_stream.recv().await.unwrap(), second_envelope);
//     assert_eq!(second_stream.recv().await.unwrap(), first_envelope);
//     assert_eq!(second_stream.recv().await.unwrap(), second_envelope);
//     let local_requests = (*local_requests.lock().unwrap()).clone();
//     assert_eq!(local_requests, vec![local_addr.clone(), local_addr]);
// }
//
// #[tokio::test]
// async fn test_route_multiple_remote_incoming_message_to_multiple_subscribers_different_hosts_same_path(
// ) {
//     let first_host = url::Url::parse("ws://127.0.0.1:9001/").unwrap();
//     let second_host = url::Url::parse("ws://127.0.0.2:9001/").unwrap();
//
//     let first_remote_path = Path::Remote(AbsolutePath::new(first_host.clone(), "/building", "/1"));
//     let second_remote_path =
//         Path::Remote(AbsolutePath::new(second_host.clone(), "/building", "/1"));
//
//     let first_remote_addr = SchemeSocketAddr::new(Scheme::Ws, "127.0.0.1:9001".parse().unwrap());
//     let second_remote_addr = SchemeSocketAddr::new(Scheme::Ws, "127.0.0.2:9001".parse().unwrap());
//
//     let (router_request_tx, _remote_rx, _local_rx, (remote_requests, _), _close_tx) =
//         create_connection_manager().await;
//
//     let first_remote_tx =
//         register_connection(&router_request_tx, Origin::Remote(first_remote_addr)).await;
//
//     let second_remote_tx =
//         register_connection(&router_request_tx, Origin::Remote(second_remote_addr)).await;
//
//     let (_sink, mut first_stream) =
//         register_subscriber(&router_request_tx, first_remote_path).await;
//     let (_sink, mut second_stream) =
//         register_subscriber(&router_request_tx, second_remote_path).await;
//
//     let envelope_101 = Envelope::make_event(
//         String::from("/building"),
//         String::from("/1"),
//         Some(Value::text("Room 101")),
//     );
//
//     let envelope_102 = Envelope::make_event(
//         String::from("/building"),
//         String::from("/1"),
//         Some(Value::text("Room 102")),
//     );
//
//     let envelope_201 = Envelope::make_event(
//         String::from("/building"),
//         String::from("/1"),
//         Some(Value::text("Room 201")),
//     );
//
//     let envelope_202 = Envelope::make_event(
//         String::from("/building"),
//         String::from("/1"),
//         Some(Value::text("Room 202")),
//     );
//
//     send_message(&first_remote_tx, envelope_101.clone()).await;
//     send_message(&first_remote_tx, envelope_102.clone()).await;
//     send_message(&second_remote_tx, envelope_201.clone()).await;
//     send_message(&second_remote_tx, envelope_202.clone()).await;
//
//     assert_eq!(first_stream.recv().await.unwrap(), envelope_101);
//     assert_eq!(first_stream.recv().await.unwrap(), envelope_102);
//     assert_eq!(second_stream.recv().await.unwrap(), envelope_201);
//     assert_eq!(second_stream.recv().await.unwrap(), envelope_202);
//     let remote_requests = (*remote_requests.lock().unwrap()).clone();
//     assert_eq!(remote_requests, vec![first_host, second_host]);
// }
//
// #[tokio::test]
// async fn test_route_multiple_local_incoming_message_to_multiple_subscribers_different_nodes_same_path(
// ) {
//     let first_local_path = Path::Local(RelativePath::new("/hotel", "/five"));
//     let second_local_path = Path::Local(RelativePath::new("/motel", "/six"));
//
//     let first_local_addr = RelativeUri::try_from("/hotel".to_string()).unwrap();
//     let second_local_addr = RelativeUri::try_from("/motel".to_string()).unwrap();
//
//     let (router_request_tx, _remote_rx, _local_rx, (_, local_requests), _close_tx) =
//         create_connection_manager().await;
//
//     let first_local_tx =
//         register_connection(&router_request_tx, Origin::Local(first_local_addr.clone())).await;
//     let second_local_tx =
//         register_connection(&router_request_tx, Origin::Local(second_local_addr.clone())).await;
//
//     let (_sink, mut first_stream) = register_subscriber(&router_request_tx, first_local_path).await;
//     let (_sink, mut second_stream) =
//         register_subscriber(&router_request_tx, second_local_path).await;
//
//     let envelope_101 = Envelope::make_event(
//         String::from("/hotel"),
//         String::from("/1"),
//         Some(Value::text("Room 101")),
//     );
//
//     let envelope_102 = Envelope::make_event(
//         String::from("/hotel"),
//         String::from("/1"),
//         Some(Value::text("Room 102")),
//     );
//
//     let envelope_201 = Envelope::make_event(
//         String::from("/motel"),
//         String::from("/1"),
//         Some(Value::text("Room 201")),
//     );
//
//     let envelope_202 = Envelope::make_event(
//         String::from("/motel"),
//         String::from("/1"),
//         Some(Value::text("Room 202")),
//     );
//
//     send_message(&first_local_tx, envelope_101.clone()).await;
//     send_message(&first_local_tx, envelope_102.clone()).await;
//     send_message(&second_local_tx, envelope_201.clone()).await;
//     send_message(&second_local_tx, envelope_202.clone()).await;
//
//     assert_eq!(first_stream.recv().await.unwrap(), envelope_101);
//     assert_eq!(first_stream.recv().await.unwrap(), envelope_102);
//     assert_eq!(second_stream.recv().await.unwrap(), envelope_201);
//     assert_eq!(second_stream.recv().await.unwrap(), envelope_202);
//     let local_requests = (*local_requests.lock().unwrap()).clone();
//     assert_eq!(local_requests, vec![first_local_addr, second_local_addr]);
// }
//
// #[tokio::test]
// async fn test_route_multiple_remote_incoming_message_to_multiple_subscribers_different_hosts_different_paths(
// ) {
//     let first_host = url::Url::parse("ws://127.0.0.1:9001/").unwrap();
//     let second_host = url::Url::parse("ws://127.0.0.2:9001/").unwrap();
//     let third_host = url::Url::parse("ws://127.0.0.3:9001/").unwrap();
//
//     let first_remote_path = Path::Remote(AbsolutePath::new(first_host.clone(), "/building", "/1"));
//     let second_remote_path = Path::Remote(AbsolutePath::new(second_host.clone(), "/room", "/2"));
//     let third_remote_path = Path::Remote(AbsolutePath::new(third_host.clone(), "/building", "/3"));
//
//     let first_remote_addr = SchemeSocketAddr::new(Scheme::Ws, "127.0.0.1:9001".parse().unwrap());
//     let second_remote_addr = SchemeSocketAddr::new(Scheme::Ws, "127.0.0.2:9001".parse().unwrap());
//     let third_remote_addr = SchemeSocketAddr::new(Scheme::Ws, "127.0.0.3:9001".parse().unwrap());
//
//     let (router_request_tx, _remote_rx, _local_rx, (remote_requests, _), _close_tx) =
//         create_connection_manager().await;
//
//     let first_remote_tx =
//         register_connection(&router_request_tx, Origin::Remote(first_remote_addr)).await;
//     let second_remote_tx =
//         register_connection(&router_request_tx, Origin::Remote(second_remote_addr)).await;
//     let third_remote_tx =
//         register_connection(&router_request_tx, Origin::Remote(third_remote_addr)).await;
//
//     let (_sink, mut first_stream) =
//         register_subscriber(&router_request_tx, first_remote_path).await;
//     let (_sink, mut second_stream) =
//         register_subscriber(&router_request_tx, second_remote_path).await;
//     let (_sink, mut third_stream) =
//         register_subscriber(&router_request_tx, third_remote_path).await;
//
//     let building_101 = Envelope::make_event(
//         String::from("/building"),
//         String::from("/1"),
//         Some(Value::text("Building 101")),
//     );
//
//     let building_102 = Envelope::make_event(
//         String::from("/building"),
//         String::from("/1"),
//         Some(Value::text("Building 102")),
//     );
//
//     let room_201 = Envelope::make_event(
//         String::from("/room"),
//         String::from("/2"),
//         Some(Value::text("Room 201")),
//     );
//
//     let room_202 = Envelope::make_event(
//         String::from("/room"),
//         String::from("/2"),
//         Some(Value::text("Room 202")),
//     );
//
//     let room_203 = Envelope::make_event(
//         String::from("/room"),
//         String::from("/2"),
//         Some(Value::text("Room 203")),
//     );
//
//     let building_301 = Envelope::make_event(
//         String::from("/building"),
//         String::from("/3"),
//         Some(Value::text("Building 301")),
//     );
//
//     let building_302 = Envelope::make_event(
//         String::from("/building"),
//         String::from("/3"),
//         Some(Value::text("Building 302")),
//     );
//
//     send_message(&first_remote_tx, building_101.clone()).await;
//     send_message(&first_remote_tx, building_102.clone()).await;
//     send_message(&second_remote_tx, room_201.clone()).await;
//     send_message(&second_remote_tx, room_202.clone()).await;
//     send_message(&second_remote_tx, room_203.clone()).await;
//     send_message(&third_remote_tx, building_301.clone()).await;
//     send_message(&third_remote_tx, building_302.clone()).await;
//
//     assert_eq!(first_stream.recv().await.unwrap(), building_101);
//     assert_eq!(first_stream.recv().await.unwrap(), building_102);
//     assert_eq!(second_stream.recv().await.unwrap(), room_201);
//     assert_eq!(second_stream.recv().await.unwrap(), room_202);
//     assert_eq!(second_stream.recv().await.unwrap(), room_203);
//     assert_eq!(third_stream.recv().await.unwrap(), building_301);
//     assert_eq!(third_stream.recv().await.unwrap(), building_302);
//     let remote_requests = (*remote_requests.lock().unwrap()).clone();
//     assert_eq!(remote_requests, vec![first_host, second_host, third_host]);
// }
//
// #[tokio::test]
// async fn test_route_multiple_local_incoming_message_to_multiple_subscribers_different_nodes_different_paths(
// ) {
//     let first_local_path = Path::Local(RelativePath::new("/hotel", "/five"));
//     let second_local_path = Path::Local(RelativePath::new("/motel", "/six"));
//     let third_local_path = Path::Local(RelativePath::new("/house", "/seven"));
//
//     let first_local_addr = RelativeUri::try_from("/hotel".to_string()).unwrap();
//     let second_local_addr = RelativeUri::try_from("/motel".to_string()).unwrap();
//     let third_local_addr = RelativeUri::try_from("/house".to_string()).unwrap();
//
//     let (router_request_tx, _remote_rx, _local_rx, (_, local_requests), _close_tx) =
//         create_connection_manager().await;
//
//     let first_local_tx =
//         register_connection(&router_request_tx, Origin::Local(first_local_addr.clone())).await;
//     let second_local_tx =
//         register_connection(&router_request_tx, Origin::Local(second_local_addr.clone())).await;
//     let third_local_tx =
//         register_connection(&router_request_tx, Origin::Local(third_local_addr.clone())).await;
//
//     let (_sink, mut first_stream) = register_subscriber(&router_request_tx, first_local_path).await;
//     let (_sink, mut second_stream) =
//         register_subscriber(&router_request_tx, second_local_path).await;
//     let (_sink, mut third_stream) = register_subscriber(&router_request_tx, third_local_path).await;
//
//     let building_101 = Envelope::make_event(
//         String::from("/hotel"),
//         String::from("/five"),
//         Some(Value::text("Building 101")),
//     );
//
//     let building_102 = Envelope::make_event(
//         String::from("/hotel"),
//         String::from("/five"),
//         Some(Value::text("Building 102")),
//     );
//
//     let room_201 = Envelope::make_event(
//         String::from("/motel"),
//         String::from("/six"),
//         Some(Value::text("Room 201")),
//     );
//
//     let room_202 = Envelope::make_event(
//         String::from("/motel"),
//         String::from("/six"),
//         Some(Value::text("Room 202")),
//     );
//
//     let room_203 = Envelope::make_event(
//         String::from("/motel"),
//         String::from("/six"),
//         Some(Value::text("Room 203")),
//     );
//
//     let building_301 = Envelope::make_event(
//         String::from("/house"),
//         String::from("/seven"),
//         Some(Value::text("Building 301")),
//     );
//
//     let building_302 = Envelope::make_event(
//         String::from("/house"),
//         String::from("/seven"),
//         Some(Value::text("Building 302")),
//     );
//
//     send_message(&first_local_tx, building_101.clone()).await;
//     send_message(&first_local_tx, building_102.clone()).await;
//     send_message(&second_local_tx, room_201.clone()).await;
//     send_message(&second_local_tx, room_202.clone()).await;
//     send_message(&second_local_tx, room_203.clone()).await;
//     send_message(&third_local_tx, building_301.clone()).await;
//     send_message(&third_local_tx, building_302.clone()).await;
//
//     assert_eq!(first_stream.recv().await.unwrap(), building_101);
//     assert_eq!(first_stream.recv().await.unwrap(), building_102);
//     assert_eq!(second_stream.recv().await.unwrap(), room_201);
//     assert_eq!(second_stream.recv().await.unwrap(), room_202);
//     assert_eq!(second_stream.recv().await.unwrap(), room_203);
//     assert_eq!(third_stream.recv().await.unwrap(), building_301);
//     assert_eq!(third_stream.recv().await.unwrap(), building_302);
//     let local_requests = (*local_requests.lock().unwrap()).clone();
//     assert_eq!(
//         local_requests,
//         vec![first_local_addr, second_local_addr, third_local_addr]
//     );
// }
