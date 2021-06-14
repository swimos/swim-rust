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

use crate::router::{ClientConnectionsManager, ClientRequest, ClientRouterFactory, Router};
use std::convert::TryFrom;
use std::num::NonZeroUsize;
use std::sync::{Arc, Mutex};
use swim_common::model::Value;
use swim_common::request::Request;
use swim_common::routing::error::{ResolutionError, Unresolvable};
use swim_common::routing::remote::{RawRoute, RemoteRoutingRequest, Scheme, SchemeSocketAddr};
use swim_common::routing::{
    Origin, PlaneRoutingRequest, RouterFactory, RoutingAddr, TaggedEnvelope,
};
use swim_common::warp::envelope::Envelope;
use swim_common::warp::path::{AbsolutePath, Path, RelativePath};
use tokio::sync::mpsc::{Receiver, Sender};
use tokio::sync::{mpsc, oneshot};
use url::Url;
use utilities::sync::promise;
use utilities::uri::RelativeUri;

async fn create_connection_manager() -> (
    Sender<ClientRequest<Path>>,
    Receiver<TaggedEnvelope>,
    Receiver<TaggedEnvelope>,
    (Arc<Mutex<Vec<Url>>>, Arc<Mutex<Vec<RelativeUri>>>),
) {
    let remote_requests: Arc<Mutex<Vec<Url>>> = Arc::new(Mutex::new(Vec::new()));
    let local_requests: Arc<Mutex<Vec<RelativeUri>>> = Arc::new(Mutex::new(Vec::new()));

    let remote_requests_copy = remote_requests.clone();
    let local_requests_copy = local_requests.clone();

    let (router_request_tx, router_request_rx) = mpsc::channel::<ClientRequest<Path>>(8);
    let (remote_router_tx, mut remote_router_rx) = mpsc::channel(8);
    let (plane_router_tx, mut plane_router_rx) = mpsc::channel(8);
    let buffer_size = NonZeroUsize::new(8).unwrap();

    let (remote_tx, remote_rx) = mpsc::channel(8);
    let (local_tx, local_rx) = mpsc::channel(8);

    let conns_manager = ClientConnectionsManager::new(
        router_request_rx,
        remote_router_tx,
        Some(plane_router_tx),
        buffer_size,
    );

    tokio::spawn(async move {
        conns_manager.run().await.unwrap();
    });

    tokio::spawn(async move {
        while let Some(remote_request) = remote_router_rx.recv().await {
            match remote_request {
                RemoteRoutingRequest::Endpoint {
                    addr: _addr,
                    request,
                } => {
                    let (_on_drop_tx, on_drop_rx) = promise::promise();
                    request
                        .send(Ok(RawRoute::new(remote_tx.clone(), on_drop_rx)))
                        .unwrap();
                }
                RemoteRoutingRequest::ResolveUrl { host, request } => {
                    (*remote_requests.lock().unwrap()).push(host);
                    request.send(Ok(RoutingAddr::remote(0))).unwrap();
                }
            }
        }
    });

    tokio::spawn(async move {
        while let Some(plane_request) = plane_router_rx.recv().await {
            match plane_request {
                PlaneRoutingRequest::Endpoint { request, .. } => {
                    let (_on_drop_tx, on_drop_rx) = promise::promise();
                    request
                        .send(Ok(RawRoute::new(local_tx.clone(), on_drop_rx)))
                        .unwrap();
                }
                PlaneRoutingRequest::Resolve { request, name, .. } => {
                    (*local_requests.lock().unwrap()).push(name);
                    request.send(Ok(RoutingAddr::local(0))).unwrap()
                }
                PlaneRoutingRequest::Agent { .. } => {
                    unimplemented!()
                }
                PlaneRoutingRequest::Routes(_) => {
                    unimplemented!()
                }
            }
        }
    });

    (
        router_request_tx,
        remote_rx,
        local_rx,
        (remote_requests_copy, local_requests_copy),
    )
}

async fn register_connection(
    router_request_tx: &Sender<ClientRequest<Path>>,
    origin: Origin,
) -> RawRoute {
    let (tx, rx) = oneshot::channel();
    let request = Request::new(tx);

    router_request_tx
        .send(ClientRequest::Connect { request, origin })
        .await
        .unwrap();

    rx.await.unwrap().unwrap()
}

async fn open_downlink(
    router_request_tx: &Sender<ClientRequest<Path>>,
    path: Path,
) -> (RawRoute, Receiver<Envelope>) {
    let (tx, rx) = oneshot::channel();
    let request = Request::new(tx);

    router_request_tx
        .send(ClientRequest::Subscribe {
            target: path,
            request,
        })
        .await
        .unwrap();

    rx.await.unwrap().unwrap()
}

async fn send_message(sender: &RawRoute, message: Envelope) {
    sender
        .sender
        .send(TaggedEnvelope(RoutingAddr::client(), message))
        .await
        .unwrap();
}

#[tokio::test]
async fn test_client_router_lookup() {
    let (request_tx, _request_rx) = mpsc::channel::<ClientRequest<AbsolutePath>>(8);
    let routing_addr = RoutingAddr::remote(0);
    let uri = RelativeUri::try_from("/foo/example".to_string()).unwrap();
    let mut client_router = ClientRouterFactory::new(request_tx).create_for(routing_addr);

    let result = client_router.lookup(None, uri, None).await.unwrap();

    assert_eq!(result, RoutingAddr::client());
}

#[tokio::test]
async fn test_client_router_resolve_sender() {
    let (request_tx, mut request_rx) = mpsc::channel::<ClientRequest<AbsolutePath>>(8);
    let routing_addr = RoutingAddr::remote(0);
    let addr = SchemeSocketAddr::new(Scheme::Ws, "192.168.0.1:9001".parse().unwrap());
    let mut client_router = ClientRouterFactory::new(request_tx).create_for(routing_addr);

    tokio::spawn(async move {
        match request_rx.recv().await.unwrap() {
            ClientRequest::Connect { request, origin } => {
                assert_eq!(origin, Origin::Remote(addr));
                let (outgoing_tx, _outgoing_rx) = mpsc::channel(8);
                let (_on_drop_tx, on_drop_rx) = promise::promise();
                let _ = request.send(Ok(RawRoute::new(outgoing_tx, on_drop_rx)));
            }
            ClientRequest::Subscribe { .. } => {
                unreachable!()
            }
        }
    });

    let result = client_router
        .resolve_sender(RoutingAddr::client(), Some(Origin::Remote(addr)))
        .await;

    assert!(result.is_ok());
}

#[tokio::test]
async fn test_client_router_resolve_sender_router_dropped() {
    let (request_tx, mut request_rx) = mpsc::channel::<ClientRequest<AbsolutePath>>(8);
    let routing_addr = RoutingAddr::remote(0);
    let addr = SchemeSocketAddr::new(Scheme::Ws, "192.168.0.1:9001".parse().unwrap());
    let mut client_router = ClientRouterFactory::new(request_tx).create_for(routing_addr);

    tokio::spawn(async move {
        let _ = request_rx.recv().await.unwrap();
    });

    let result = client_router
        .resolve_sender(RoutingAddr::client(), Some(Origin::Remote(addr)))
        .await;

    let router_dropped_err = ResolutionError::router_dropped();
    assert!(matches!(result, Err(err) if err == router_dropped_err));
}

#[tokio::test]
async fn test_client_router_resolve_sender_unresolvable() {
    let (request_tx, mut request_rx) = mpsc::channel::<ClientRequest<AbsolutePath>>(8);
    let routing_addr = RoutingAddr::remote(0);
    let addr = SchemeSocketAddr::new(Scheme::Ws, "192.168.0.1:9001".parse().unwrap());
    let mut client_router = ClientRouterFactory::new(request_tx).create_for(routing_addr);

    tokio::spawn(async move {
        match request_rx.recv().await.unwrap() {
            ClientRequest::Connect { request, origin } => {
                assert_eq!(origin, Origin::Remote(addr));
                let _ = request.send(Err(Unresolvable(RoutingAddr::remote(0))));
            }
            ClientRequest::Subscribe { .. } => {
                unreachable!()
            }
        }
    });

    let result = client_router
        .resolve_sender(RoutingAddr::client(), Some(Origin::Remote(addr)))
        .await;

    let router_dropped_err =
        ResolutionError::unresolvable("No active endpoint with ID: Remote(0)".to_string());
    assert!(matches!(result, Err(err) if err == router_dropped_err));
}

//Todo dm
#[tokio::test]
async fn test_foo() {
    let remote_host = url::Url::parse("ws://192.168.0.1:9001/").unwrap();
    let remote_node = "/foo";
    let remote_lane = "/bar";
    let remote_path = Path::Remote(AbsolutePath::new(
        remote_host.clone(),
        remote_node,
        remote_lane,
    ));
    let remote_addr = SchemeSocketAddr::new(Scheme::Ws, "192.168.0.1:9001".parse().unwrap());

    let local_node = "/baz";
    let local_lane = "/qux";
    let local_path = Path::Local(RelativePath::new(local_node, local_lane));
    let local_addr = RelativeUri::try_from(local_node).unwrap();

    let (router_request_tx, mut remote_rx, mut local_rx, (remote_requests, local_requests)) =
        create_connection_manager().await;

    let remote_tx = register_connection(&router_request_tx, Origin::Remote(remote_addr)).await;
    let local_tx = register_connection(&router_request_tx, Origin::Local(local_addr)).await;

    let (remote_subscriber_tx, mut remote_subscriber_rx) =
        open_downlink(&router_request_tx, remote_path).await;

    let (local_subscriber_tx, mut local_subscriber_rx) =
        open_downlink(&router_request_tx, local_path).await;

    // Receive remote
    send_message(
        &remote_tx,
        Envelope::make_event("/foo", "/bar", Some("Remote_Text_Incoming".into())),
    )
    .await;

    let message = remote_subscriber_rx.recv().await.unwrap();
    eprintln!("envelope = {:#?}", message);

    // Send remote
    send_message(
        &remote_subscriber_tx,
        Envelope::make_command("/foo", "/bar", Some("Remote_Text_Outgoing".into())),
    )
    .await;

    let envelope = remote_rx.recv().await.unwrap();
    eprintln!("envelope = {:#?}", envelope);

    // Receive local
    send_message(
        &local_tx,
        Envelope::make_event("/baz", "/qux", Some("Local_Text_Incoming".into())),
    )
    .await;

    let envelope = local_subscriber_rx.recv().await.unwrap();
    eprintln!("envelope = {:#?}", envelope);

    // Send local
    send_message(
        &local_subscriber_tx,
        Envelope::make_command("/baz", "/qux", Some("Local_Text_Outgoing".into())),
    )
    .await;

    let envelope = local_rx.recv().await.unwrap();
    eprintln!("envelope = {:#?}", envelope);

    eprintln!("local_requests = {:#?}", local_requests);
    eprintln!("remote_requests = {:#?}", remote_requests);
}

#[tokio::test]
async fn test_route_single_remote_outgoing_message_to_single_downlink() {
    let host = url::Url::parse("ws://127.0.0.1:9001/").unwrap();
    let remote_path = Path::Remote(AbsolutePath::new(host.clone(), "/foo", "/bar"));
    let remote_addr = SchemeSocketAddr::new(Scheme::Ws, "127.0.0.1:9001".parse().unwrap());

    let (router_request_tx, mut remote_rx, _local_rx, (remote_requests, _)) =
        create_connection_manager().await;
    let _remote_tx = register_connection(&router_request_tx, Origin::Remote(remote_addr)).await;
    let (sink, _stream) = open_downlink(&router_request_tx, remote_path).await;

    let envelope = Envelope::sync(String::from("/foo"), String::from("/bar"));
    send_message(&sink, envelope.clone()).await;

    assert_eq!(remote_rx.recv().await.unwrap().1, envelope);
    let remote_requests = (*remote_requests.lock().unwrap()).clone();
    assert_eq!(remote_requests, vec![host])
}

#[tokio::test]
async fn test_route_single_local_outgoing_message_to_single_downlink() {
    let local_path = Path::Local(RelativePath::new("/foo", "/bar"));
    let local_addr = RelativeUri::try_from("/foo".to_string()).unwrap();

    let (router_request_tx, _remote_rx, mut local_rx, (_, local_requests)) =
        create_connection_manager().await;

    let _local_rx =
        register_connection(&router_request_tx, Origin::Local(local_addr.clone())).await;
    let (sink, _stream) = open_downlink(&router_request_tx, local_path).await;

    let envelope = Envelope::sync(String::from("/foo"), String::from("/bar"));
    send_message(&sink, envelope.clone()).await;

    assert_eq!(local_rx.recv().await.unwrap().1, envelope);
    let local_requests = (*local_requests.lock().unwrap()).clone();
    assert_eq!(local_requests, vec![local_addr])
}

#[tokio::test]
async fn test_route_single_remote_outgoing_message_to_multiple_downlinks_same_host() {
    let host = url::Url::parse("ws://127.0.0.1:9001/").unwrap();
    let remote_path = Path::Remote(AbsolutePath::new(host.clone(), "/foo", "/bar"));
    let remote_addr = SchemeSocketAddr::new(Scheme::Ws, "127.0.0.1:9001".parse().unwrap());

    let (router_request_tx, mut remote_rx, _local_rx, (remote_requests, _)) =
        create_connection_manager().await;
    let _remote_tx = register_connection(&router_request_tx, Origin::Remote(remote_addr)).await;
    let (first_sink, _stream) = open_downlink(&router_request_tx, remote_path.clone()).await;
    let (second_sink, _stream) = open_downlink(&router_request_tx, remote_path).await;

    let envelope = Envelope::make_event(
        String::from("oof"),
        String::from("rab"),
        Some(Value::text("bye")),
    );
    send_message(&first_sink, envelope.clone()).await;
    send_message(&second_sink, envelope.clone()).await;

    assert_eq!(remote_rx.recv().await.unwrap().1, envelope);
    assert_eq!(remote_rx.recv().await.unwrap().1, envelope);
    let remote_requests = (*remote_requests.lock().unwrap()).clone();
    assert_eq!(remote_requests, vec![host.clone(), host])
}

#[tokio::test]
async fn test_route_single_local_outgoing_message_to_multiple_downlinks_same_node() {
    let local_path = Path::Local(RelativePath::new("/foo", "/bar"));
    let local_addr = RelativeUri::try_from("/foo".to_string()).unwrap();

    let (router_request_tx, _remote_rx, mut local_rx, (_, local_requests)) =
        create_connection_manager().await;

    let _local_rx =
        register_connection(&router_request_tx, Origin::Local(local_addr.clone())).await;
    let (first_sink, _stream) = open_downlink(&router_request_tx, local_path.clone()).await;
    let (second_sink, _stream) = open_downlink(&router_request_tx, local_path).await;

    let envelope = Envelope::make_event(
        String::from("oof"),
        String::from("rab"),
        Some(Value::text("bye")),
    );
    send_message(&first_sink, envelope.clone()).await;
    send_message(&second_sink, envelope.clone()).await;

    assert_eq!(local_rx.recv().await.unwrap().1, envelope);
    assert_eq!(local_rx.recv().await.unwrap().1, envelope);

    let local_requests = (*local_requests.lock().unwrap()).clone();
    assert_eq!(local_requests, vec![local_addr.clone(), local_addr])
}

#[tokio::test]
async fn test_route_single_remote_outgoing_message_to_multiple_downlinks_different_hosts() {
    let first_host = url::Url::parse("ws://127.0.0.1:9001/").unwrap();
    let second_host = url::Url::parse("ws://127.0.0.2:9001/").unwrap();

    let first_remote_path = Path::Remote(AbsolutePath::new(first_host.clone(), "/foo", "/bar"));
    let second_remote_path = Path::Remote(AbsolutePath::new(second_host.clone(), "/foo", "/bar"));
    let first_remote_addr = SchemeSocketAddr::new(Scheme::Ws, "127.0.0.1:9001".parse().unwrap());
    let second_remote_addr = SchemeSocketAddr::new(Scheme::Ws, "127.0.0.1:9002".parse().unwrap());

    let (router_request_tx, mut remote_rx, _local_rx, (remote_requests, _)) =
        create_connection_manager().await;
    let _first_remote_tx =
        register_connection(&router_request_tx, Origin::Remote(first_remote_addr)).await;
    let _second_remote_tx =
        register_connection(&router_request_tx, Origin::Remote(second_remote_addr)).await;
    let (first_sink, _stream) = open_downlink(&router_request_tx, first_remote_path).await;
    let (second_sink, _stream) = open_downlink(&router_request_tx, second_remote_path).await;

    let envelope = Envelope::make_event(
        String::from("/foo"),
        String::from("/bar"),
        Some(Value::text("hello")),
    );
    send_message(&first_sink, envelope.clone()).await;
    send_message(&second_sink, envelope.clone()).await;

    assert_eq!(remote_rx.recv().await.unwrap().1, envelope);
    assert_eq!(remote_rx.recv().await.unwrap().1, envelope);

    let remote_requests = (*remote_requests.lock().unwrap()).clone();
    assert_eq!(remote_requests, vec![first_host, second_host])
}

#[tokio::test]
async fn test_route_single_local_outgoing_message_to_multiple_downlinks_different_nodes() {
    let first_local_path = Path::Local(RelativePath::new("/foo", "/bar"));
    let second_local_path = Path::Local(RelativePath::new("/baz", "/qux"));
    let first_local_addr = RelativeUri::try_from("/foo".to_string()).unwrap();
    let second_local_addr = RelativeUri::try_from("/baz".to_string()).unwrap();

    let (router_request_tx, _remote_rx, mut local_rx, (_, local_requests)) =
        create_connection_manager().await;

    let _local_rx =
        register_connection(&router_request_tx, Origin::Local(first_local_addr.clone())).await;
    let _local_rx =
        register_connection(&router_request_tx, Origin::Local(second_local_addr.clone())).await;
    let (first_sink, _stream) = open_downlink(&router_request_tx, first_local_path).await;
    let (second_sink, _stream) = open_downlink(&router_request_tx, second_local_path).await;

    let first_envelope = Envelope::make_event(
        String::from("/foo"),
        String::from("/bar"),
        Some(Value::text("hello")),
    );
    let second_envelope = Envelope::make_event(
        String::from("baz"),
        String::from("qux"),
        Some(Value::text("hello")),
    );
    send_message(&first_sink, first_envelope.clone()).await;
    send_message(&second_sink, second_envelope.clone()).await;

    assert_eq!(local_rx.recv().await.unwrap().1, first_envelope);
    assert_eq!(local_rx.recv().await.unwrap().1, second_envelope);

    let local_requests = (*local_requests.lock().unwrap()).clone();
    assert_eq!(local_requests, vec![first_local_addr, second_local_addr])
}

#[tokio::test]
async fn test_route_multiple_remote_outgoing_messages_to_single_downlink() {
    let host = url::Url::parse("ws://127.0.0.1:9001/").unwrap();

    let remote_path = Path::Remote(AbsolutePath::new(host.clone(), "/foo", "/bar"));
    let remote_addr = SchemeSocketAddr::new(Scheme::Ws, "127.0.0.1:9001".parse().unwrap());

    let (router_request_tx, mut remote_rx, _local_rx, (remote_requests, _)) =
        create_connection_manager().await;
    let _remote_tx = register_connection(&router_request_tx, Origin::Remote(remote_addr)).await;
    let (sink, _stream) = open_downlink(&router_request_tx, remote_path).await;

    let first_envelope = Envelope::make_event(
        String::from("/foo"),
        String::from("/bar"),
        Some(Value::text("First_Downlink")),
    );

    let second_envelope = Envelope::make_event(
        String::from("/foo"),
        String::from("/bar"),
        Some(Value::text("Second_Downlink")),
    );
    send_message(&sink, first_envelope.clone()).await;
    send_message(&sink, second_envelope.clone()).await;

    assert_eq!(remote_rx.recv().await.unwrap().1, first_envelope);
    assert_eq!(remote_rx.recv().await.unwrap().1, second_envelope);

    let remote_requests = (*remote_requests.lock().unwrap()).clone();
    assert_eq!(remote_requests, vec![host])
}

#[tokio::test]
async fn test_route_multiple_local_outgoing_messages_to_single_downlink() {
    let local_path = Path::Local(RelativePath::new("/foo", "/bar"));
    let local_addr = RelativeUri::try_from("/foo".to_string()).unwrap();

    let (router_request_tx, _remote_rx, mut local_rx, (_, local_requests)) =
        create_connection_manager().await;

    let _local_rx =
        register_connection(&router_request_tx, Origin::Local(local_addr.clone())).await;
    let (sink, _stream) = open_downlink(&router_request_tx, local_path).await;

    let first_envelope = Envelope::make_event(
        String::from("/foo"),
        String::from("/bar"),
        Some(Value::text("First_Downlink")),
    );

    let second_envelope = Envelope::make_event(
        String::from("/foo"),
        String::from("/bar"),
        Some(Value::text("Second_Downlink")),
    );
    send_message(&sink, first_envelope.clone()).await;
    send_message(&sink, second_envelope.clone()).await;

    assert_eq!(local_rx.recv().await.unwrap().1, first_envelope);
    assert_eq!(local_rx.recv().await.unwrap().1, second_envelope);

    let local_requests = (*local_requests.lock().unwrap()).clone();
    assert_eq!(local_requests, vec![local_addr])
}

// #[tokio::test]
// async fn test_route_multiple_remote_outgoing_messages_to_multiple_downlinks_same_host() {
//     let host_url = url::Url::parse("ws://127.0.0.1:9001/").unwrap();
//
//     let remote_path = Path::Remote(AbsolutePath::new(
//         host_url,
//         "/foo",
//         "/bar",
//     ));
//     let remote_addr = SchemeSocketAddr::new(Scheme::Ws, "127.0.0.1:9001".parse().unwrap());
//
//     let (router_request_tx, mut remote_rx, _local_rx) = create_connection_manager().await;
//     let _remote_tx = register_connection(&router_request_tx, Origin::Remote(remote_addr)).await;
//     let (sink, _stream) = open_downlink(&router_request_tx, remote_path).await;
//
//     let first_envelope = Envelope::make_event(
//         String::from("/foo"),
//         String::from("/bar"),
//         Some(Value::text("First_Downlink")),
//     );
//
//     let second_envelope = Envelope::make_event(
//         String::from("/foo"),
//         String::from("/bar"),
//         Some(Value::text("Second_Downlink")),
//     );
//     send_message(&sink, first_envelope.clone()).await;
//     send_message(&sink, second_envelope.clone()).await;
//
//     assert_eq!(remote_rx.recv().await.unwrap().1, first_envelope);
//     assert_eq!(remote_rx.recv().await.unwrap().1, second_envelope);
//
//
//
//
//
//     let url = url::Url::parse("ws://127.0.0.1/").unwrap();
//
//     let (pool, pool_handlers_rx) = TestPool::new();
//     let mut router = SwimRouter::new(Default::default(), pool.clone());
//
//     let (first_sink, _) = open_connection(&mut router, &url, "foo_node", "foo_lane").await;
//     let (second_sink, _) = open_connection(&mut router, &url, "foo_node", "foo_lane").await;
//
//     let first_env = Envelope::make_event(
//         String::from("first_foo"),
//         String::from("first_bar"),
//         Some(Value::text("first_body")),
//     );
//
//     let second_env = Envelope::make_event(
//         String::from("second_foo"),
//         String::from("second_bar"),
//         Some(Value::text("second_body")),
//     );
//
//     let third_env = Envelope::make_event(
//         String::from("third_foo"),
//         String::from("third_bar"),
//         Some(Value::text("third_body")),
//     );
//
//     let _ = first_sink.send(first_env).await.unwrap();
//     let _ = first_sink.send(second_env).await.unwrap();
//     let _ = second_sink.send(third_env).await.unwrap();
//
//     let mut pool_handlers: HashMap<_, _> = ReceiverStream::new(pool_handlers_rx)
//         .take(1)
//         .collect()
//         .await;
//
//     assert!(router.close().await.is_ok());
//     assert_eq!(get_request_count(&pool), 3);
//
//     let mut expected_requests = HashMap::new();
//     expected_requests.insert((url.clone(), false), 3);
//
//     assert_eq!(get_requests(&pool), expected_requests);
//
//     assert_eq!(pool.connections.lock().unwrap().len(), 1);
//     assert_eq!(
//         get_message(&mut pool_handlers, &url).await.unwrap(),
//         "@event(node:first_foo,lane:first_bar){first_body}".into()
//     );
//     assert_eq!(
//         get_message(&mut pool_handlers, &url).await.unwrap(),
//         "@event(node:second_foo,lane:second_bar){second_body}".into()
//     );
//     assert_eq!(
//         get_message(&mut pool_handlers, &url).await.unwrap(),
//         "@event(node:third_foo,lane:third_bar){third_body}".into()
//     );
// }

// #[tokio::test]
// async fn test_route_multiple_outgoing_messages_to_multiple_downlinks_different_hosts() {
//     let first_url = url::Url::parse("ws://127.0.0.1/").unwrap();
//     let second_url = url::Url::parse("ws://127.0.0.2/").unwrap();
//
//     let (pool, pool_handlers_rx) = TestPool::new();
//     let mut router = SwimRouter::new(Default::default(), pool.clone());
//
//     let (first_sink, _) = open_connection(&mut router, &first_url, "foo_node", "foo_lane").await;
//     let (second_sink, _) = open_connection(&mut router, &second_url, "foo_node", "foo_lane").await;
//
//     let first_env = Envelope::make_event(
//         String::from("first_foo"),
//         String::from("first_bar"),
//         Some(Value::text("first_body")),
//     );
//
//     let second_env = Envelope::make_event(
//         String::from("second_foo"),
//         String::from("second_bar"),
//         Some(Value::text("second_body")),
//     );
//
//     let third_env = Envelope::make_event(
//         String::from("third_foo"),
//         String::from("third_bar"),
//         Some(Value::text("third_body")),
//     );
//
//     let _ = first_sink.send(first_env).await.unwrap();
//     let _ = first_sink.send(second_env).await.unwrap();
//     let _ = second_sink.send(third_env).await.unwrap();
//
//     let mut pool_handlers: HashMap<_, _> = ReceiverStream::new(pool_handlers_rx)
//         .take(2)
//         .collect()
//         .await;
//
//     assert!(router.close().await.is_ok());
//     assert_eq!(get_request_count(&pool), 3);
//
//     let mut expected_requests = HashMap::new();
//     expected_requests.insert((first_url.clone(), false), 2);
//     expected_requests.insert((second_url.clone(), false), 1);
//
//     assert_eq!(get_requests(&pool), expected_requests);
//
//     assert_eq!(pool.connections.lock().unwrap().len(), 2);
//     assert_eq!(
//         get_message(&mut pool_handlers, &first_url).await.unwrap(),
//         "@event(node:first_foo,lane:first_bar){first_body}".into()
//     );
//     assert_eq!(
//         get_message(&mut pool_handlers, &first_url).await.unwrap(),
//         "@event(node:second_foo,lane:second_bar){second_body}".into()
//     );
//     assert_eq!(
//         get_message(&mut pool_handlers, &second_url).await.unwrap(),
//         "@event(node:third_foo,lane:third_bar){third_body}".into()
//     );
// }
//
// #[tokio::test]
// async fn test_route_single_incoming_message_to_single_downlink() {
//     let url = url::Url::parse("ws://127.0.0.1/").unwrap();
//
//     let (pool, pool_handlers_rx) = TestPool::new();
//     let mut router = SwimRouter::new(Default::default(), pool.clone());
//
//     let (sink, mut stream) = open_connection(&mut router, &url, "foo", "bar").await;
//
//     let envelope = Envelope::sync(String::from("foo"), String::from("bar"));
//
//     let _ = sink.send(envelope.clone()).await.unwrap();
//
//     let mut pool_handlers: HashMap<_, _> = ReceiverStream::new(pool_handlers_rx)
//         .take(1)
//         .collect()
//         .await;
//
//     send_message(&mut pool_handlers, &url, "@event(node:foo,lane:bar){Hello}").await;
//
//     let expected_env = Envelope::make_event(
//         String::from("foo"),
//         String::from("bar"),
//         Some(Value::text("Hello")),
//     );
//
//     assert_eq!(
//         stream.recv().await.unwrap(),
//         RouterEvent::Message(expected_env.into_incoming().unwrap())
//     );
//
//     assert!(router.close().await.is_ok());
//     assert_eq!(get_request_count(&pool), 1);
//
//     let mut expected_requests = HashMap::new();
//     expected_requests.insert((url.clone(), false), 1);
//
//     assert_eq!(get_requests(&pool), expected_requests);
//     assert_eq!(pool.connections.lock().unwrap().len(), 1);
//
//     assert_eq!(stream.recv().await.unwrap(), RouterEvent::Stopping);
// }
//
// #[tokio::test]
// async fn test_route_single_incoming_message_to_multiple_downlinks_same_host_same_path() {
//     let url = url::Url::parse("ws://127.0.0.1/").unwrap();
//
//     let (pool, pool_handlers_rx) = TestPool::new();
//     let mut router = SwimRouter::new(Default::default(), pool.clone());
//
//     let (first_sink, mut first_stream) = open_connection(&mut router, &url, "foo", "bar").await;
//     let (_, mut second_stream) = open_connection(&mut router, &url, "foo", "bar").await;
//
//     let envelope = Envelope::sync(String::from("foo"), String::from("bar"));
//     let _ = first_sink.send(envelope).await.unwrap();
//
//     let mut pool_handlers: HashMap<_, _> = ReceiverStream::new(pool_handlers_rx)
//         .take(1)
//         .collect()
//         .await;
//
//     send_message(
//         &mut pool_handlers,
//         &url,
//         "@event(node:foo,lane:bar){Goodbye}",
//     )
//     .await;
//
//     let expected_env = Envelope::make_event(
//         String::from("foo"),
//         String::from("bar"),
//         Some(Value::text("Goodbye")),
//     );
//
//     assert_eq!(
//         first_stream.recv().await.unwrap(),
//         RouterEvent::Message(expected_env.clone().into_incoming().unwrap())
//     );
//
//     assert_eq!(
//         second_stream.recv().await.unwrap(),
//         RouterEvent::Message(expected_env.into_incoming().unwrap())
//     );
//
//     assert!(router.close().await.is_ok());
//     assert_eq!(get_request_count(&pool), 1);
//
//     let mut expected_requests = HashMap::new();
//     expected_requests.insert((url.clone(), false), 1);
//
//     assert_eq!(get_requests(&pool), expected_requests);
//     assert_eq!(pool.connections.lock().unwrap().len(), 1);
//
//     assert_eq!(first_stream.recv().await.unwrap(), RouterEvent::Stopping);
//     assert_eq!(second_stream.recv().await.unwrap(), RouterEvent::Stopping);
// }
//
// #[tokio::test]
// async fn test_route_single_incoming_message_to_multiple_downlinks_same_host_different_paths() {
//     let url = url::Url::parse("ws://127.0.0.1/").unwrap();
//
//     let (pool, pool_handlers_rx) = TestPool::new();
//     let mut router = SwimRouter::new(Default::default(), pool.clone());
//
//     let (first_sink, mut first_stream) = open_connection(&mut router, &url, "oof", "rab").await;
//     let (_, mut second_stream) = open_connection(&mut router, &url, "foo", "bar").await;
//
//     let envelope = Envelope::sync(String::from("foo"), String::from("bar"));
//     let _ = first_sink.send(envelope.clone()).await.unwrap();
//
//     let mut pool_handlers: HashMap<_, _> = ReceiverStream::new(pool_handlers_rx)
//         .take(1)
//         .collect()
//         .await;
//
//     send_message(&mut pool_handlers, &url, "@event(node:foo,lane:bar){tseT}").await;
//
//     let expected_env = Envelope::make_event(
//         String::from("foo"),
//         String::from("bar"),
//         Some(Value::text("tseT")),
//     );
//
//     assert!(timeout(Duration::from_secs(1), first_stream.recv())
//         .await
//         .is_err());
//
//     assert_eq!(
//         second_stream.recv().await.unwrap(),
//         RouterEvent::Message(expected_env.into_incoming().unwrap())
//     );
//
//     assert!(router.close().await.is_ok());
//     assert_eq!(get_request_count(&pool), 1);
//
//     let mut expected_requests = HashMap::new();
//     expected_requests.insert((url.clone(), false), 1);
//
//     assert_eq!(get_requests(&pool), expected_requests);
//     assert_eq!(pool.connections.lock().unwrap().len(), 1);
//
//     assert_eq!(first_stream.recv().await.unwrap(), RouterEvent::Stopping);
//     assert_eq!(second_stream.recv().await.unwrap(), RouterEvent::Stopping);
// }
//
// #[tokio::test]
// async fn test_route_single_incoming_message_to_multiple_downlinks_different_hosts_same_path() {
//     let first_url = url::Url::parse("ws://127.0.0.1/").unwrap();
//     let second_url = url::Url::parse("ws://127.0.0.2/").unwrap();
//
//     let (pool, pool_handlers_rx) = TestPool::new();
//     let mut router = SwimRouter::new(Default::default(), pool.clone());
//
//     let (first_sink, mut first_stream) =
//         open_connection(&mut router, &first_url, "foo", "bar").await;
//     let (second_sink, mut second_stream) =
//         open_connection(&mut router, &second_url, "foo", "bar").await;
//
//     let envelope = Envelope::sync(String::from("foo"), String::from("bar"));
//     let _ = first_sink.send(envelope.clone()).await.unwrap();
//     let _ = second_sink.send(envelope).await.unwrap();
//
//     let mut pool_handlers: HashMap<_, _> = ReceiverStream::new(pool_handlers_rx)
//         .take(2)
//         .collect()
//         .await;
//
//     send_message(
//         &mut pool_handlers,
//         &first_url,
//         "@event(node:foo,lane:bar){\"First Hello\"}",
//     )
//     .await;
//
//     send_message(
//         &mut pool_handlers,
//         &second_url,
//         "@event(node:foo,lane:bar){\"Second Hello\"}",
//     )
//     .await;
//
//     let first_env = Envelope::make_event(
//         String::from("foo"),
//         String::from("bar"),
//         Some(Value::text("First Hello")),
//     );
//
//     let second_env = Envelope::make_event(
//         String::from("foo"),
//         String::from("bar"),
//         Some(Value::text("Second Hello")),
//     );
//
//     assert_eq!(
//         first_stream.recv().await.unwrap(),
//         RouterEvent::Message(first_env.into_incoming().unwrap())
//     );
//
//     assert_eq!(
//         second_stream.recv().await.unwrap(),
//         RouterEvent::Message(second_env.into_incoming().unwrap())
//     );
//
//     assert!(router.close().await.is_ok());
//     assert_eq!(get_request_count(&pool), 2);
//
//     let mut expected_requests = HashMap::new();
//     expected_requests.insert((first_url.clone(), false), 1);
//     expected_requests.insert((second_url.clone(), false), 1);
//
//     assert_eq!(get_requests(&pool), expected_requests);
//     assert_eq!(pool.connections.lock().unwrap().len(), 2);
//
//     assert_eq!(first_stream.recv().await.unwrap(), RouterEvent::Stopping);
//     assert_eq!(second_stream.recv().await.unwrap(), RouterEvent::Stopping);
// }
//
// #[tokio::test]
// async fn test_route_single_incoming_message_to_multiple_downlinks_different_hosts_different_paths()
// {
//     let first_url = url::Url::parse("ws://127.0.0.1/").unwrap();
//     let second_url = url::Url::parse("ws://127.0.0.2/").unwrap();
//     let third_url = url::Url::parse("ws://127.0.0.3/").unwrap();
//
//     let (pool, pool_handlers_rx) = TestPool::new();
//     let mut router = SwimRouter::new(Default::default(), pool.clone());
//
//     let (first_sink, mut first_stream) =
//         open_connection(&mut router, &first_url, "foo", "bar").await;
//
//     let (second_sink, mut second_stream) =
//         open_connection(&mut router, &second_url, "oof", "rab").await;
//
//     let (third_sink, mut third_stream) =
//         open_connection(&mut router, &third_url, "ofo", "abr").await;
//
//     let envelope = Envelope::sync(String::from("foo"), String::from("bar"));
//     let _ = first_sink.send(envelope.clone()).await.unwrap();
//     let _ = second_sink.send(envelope.clone()).await.unwrap();
//     let _ = third_sink.send(envelope).await.unwrap();
//
//     let mut pool_handlers: HashMap<_, _> = ReceiverStream::new(pool_handlers_rx)
//         .take(3)
//         .collect()
//         .await;
//
//     send_message(
//         &mut pool_handlers,
//         &first_url,
//         "@event(node:foo,lane:bar){\"Hello First\"}",
//     )
//     .await;
//
//     send_message(
//         &mut pool_handlers,
//         &second_url,
//         "@event(node:oof,lane:rab){\"Hello Second\"}",
//     )
//     .await;
//
//     send_message(
//         &mut pool_handlers,
//         &third_url,
//         "@event(node:ofo,lane:abr){\"Hello Third\"}",
//     )
//     .await;
//
//     let first_env = Envelope::make_event(
//         String::from("foo"),
//         String::from("bar"),
//         Some(Value::text("Hello First")),
//     );
//
//     let second_env = Envelope::make_event(
//         String::from("oof"),
//         String::from("rab"),
//         Some(Value::text("Hello Second")),
//     );
//
//     let third_env = Envelope::make_event(
//         String::from("ofo"),
//         String::from("abr"),
//         Some(Value::text("Hello Third")),
//     );
//
//     assert_eq!(
//         first_stream.recv().await.unwrap(),
//         RouterEvent::Message(first_env.into_incoming().unwrap())
//     );
//
//     assert_eq!(
//         second_stream.recv().await.unwrap(),
//         RouterEvent::Message(second_env.into_incoming().unwrap())
//     );
//
//     assert_eq!(
//         third_stream.recv().await.unwrap(),
//         RouterEvent::Message(third_env.into_incoming().unwrap())
//     );
//
//     assert!(router.close().await.is_ok());
//     assert_eq!(get_request_count(&pool), 3);
//
//     let mut expected_requests = HashMap::new();
//     expected_requests.insert((first_url.clone(), false), 1);
//     expected_requests.insert((second_url.clone(), false), 1);
//     expected_requests.insert((third_url.clone(), false), 1);
//
//     assert_eq!(get_requests(&pool), expected_requests);
//     assert_eq!(pool.connections.lock().unwrap().len(), 3);
//
//     assert_eq!(first_stream.recv().await.unwrap(), RouterEvent::Stopping);
//     assert_eq!(second_stream.recv().await.unwrap(), RouterEvent::Stopping);
//     assert_eq!(third_stream.recv().await.unwrap(), RouterEvent::Stopping);
// }
//
// #[tokio::test]
// async fn test_route_multiple_incoming_messages_to_single_downlink() {
//     let url = url::Url::parse("ws://127.0.0.1/").unwrap();
//
//     let (pool, pool_handlers_rx) = TestPool::new();
//     let mut router = SwimRouter::new(Default::default(), pool.clone());
//
//     let (sink, mut stream) = open_connection(&mut router, &url, "foo", "bar").await;
//
//     let envelope = Envelope::sync(String::from("foo"), String::from("bar"));
//     let _ = sink.send(envelope.clone()).await.unwrap();
//
//     let mut pool_handlers: HashMap<_, _> = ReceiverStream::new(pool_handlers_rx)
//         .take(1)
//         .collect()
//         .await;
//
//     send_message(
//         &mut pool_handlers,
//         &url,
//         "@event(node:foo,lane:bar){\"First!\"}",
//     )
//     .await;
//     send_message(
//         &mut pool_handlers,
//         &url,
//         "@event(node:foo,lane:bar){\"Second!\"}",
//     )
//     .await;
//
//     let first_env = Envelope::make_event(
//         String::from("foo"),
//         String::from("bar"),
//         Some(Value::text("First!")),
//     );
//
//     let second_env = Envelope::make_event(
//         String::from("foo"),
//         String::from("bar"),
//         Some(Value::text("Second!")),
//     );
//
//     assert_eq!(
//         stream.recv().await.unwrap(),
//         RouterEvent::Message(first_env.into_incoming().unwrap())
//     );
//
//     assert_eq!(
//         stream.recv().await.unwrap(),
//         RouterEvent::Message(second_env.into_incoming().unwrap())
//     );
//
//     assert!(router.close().await.is_ok());
//     assert_eq!(get_request_count(&pool), 1);
//
//     let mut expected_requests = HashMap::new();
//     expected_requests.insert((url.clone(), false), 1);
//
//     assert_eq!(get_requests(&pool), expected_requests);
//     assert_eq!(pool.connections.lock().unwrap().len(), 1);
//
//     assert_eq!(stream.recv().await.unwrap(), RouterEvent::Stopping);
// }
//
// #[tokio::test]
// async fn test_route_multiple_incoming_messages_to_multiple_downlinks_same_host_same_path() {
//     let url = url::Url::parse("ws://192.168.0.1/").unwrap();
//
//     let (pool, pool_handlers_rx) = TestPool::new();
//     let mut router = SwimRouter::new(Default::default(), pool.clone());
//
//     let (first_sink, mut first_stream) = open_connection(&mut router, &url, "room", "five").await;
//     let (_, mut second_stream) = open_connection(&mut router, &url, "room", "five").await;
//
//     let envelope = Envelope::sync(String::from("foo"), String::from("bar"));
//     let _ = first_sink.send(envelope).await.unwrap();
//
//     let mut pool_handlers: HashMap<_, _> = ReceiverStream::new(pool_handlers_rx)
//         .take(1)
//         .collect()
//         .await;
//
//     send_message(
//         &mut pool_handlers,
//         &url,
//         "@event(node:room,lane:five){\"John Doe\"}",
//     )
//     .await;
//     send_message(
//         &mut pool_handlers,
//         &url,
//         "@event(node:room,lane:five){\"Jane Doe\"}",
//     )
//     .await;
//
//     let first_env = Envelope::make_event(
//         String::from("room"),
//         String::from("five"),
//         Some(Value::text("John Doe")),
//     );
//
//     let second_env = Envelope::make_event(
//         String::from("room"),
//         String::from("five"),
//         Some(Value::text("Jane Doe")),
//     );
//
//     assert_eq!(
//         first_stream.recv().await.unwrap(),
//         RouterEvent::Message(first_env.clone().into_incoming().unwrap())
//     );
//
//     assert_eq!(
//         first_stream.recv().await.unwrap(),
//         RouterEvent::Message(second_env.clone().into_incoming().unwrap())
//     );
//
//     assert_eq!(
//         second_stream.recv().await.unwrap(),
//         RouterEvent::Message(first_env.into_incoming().unwrap())
//     );
//
//     assert_eq!(
//         second_stream.recv().await.unwrap(),
//         RouterEvent::Message(second_env.into_incoming().unwrap())
//     );
//
//     assert!(router.close().await.is_ok());
//     assert_eq!(get_request_count(&pool), 1);
//
//     let mut expected_requests = HashMap::new();
//     expected_requests.insert((url.clone(), false), 1);
//
//     assert_eq!(get_requests(&pool), expected_requests);
//     assert_eq!(pool.connections.lock().unwrap().len(), 1);
//
//     assert_eq!(first_stream.recv().await.unwrap(), RouterEvent::Stopping);
//     assert_eq!(second_stream.recv().await.unwrap(), RouterEvent::Stopping);
// }
//
// #[tokio::test]
// async fn test_route_multiple_incoming_messages_to_multiple_downlinks_same_host_different_paths() {
//     let url = url::Url::parse("ws://127.0.0.1/").unwrap();
//
//     let (pool, pool_handlers_rx) = TestPool::new();
//     let mut router = SwimRouter::new(Default::default(), pool.clone());
//
//     let (first_sink, mut first_stream) = open_connection(&mut router, &url, "room", "five").await;
//     let (_, mut second_stream) = open_connection(&mut router, &url, "room", "six").await;
//
//     let envelope = Envelope::sync(String::from("room"), String::from("seven"));
//     let _ = first_sink.send(envelope.clone()).await.unwrap();
//
//     let mut pool_handlers: HashMap<_, _> = ReceiverStream::new(pool_handlers_rx)
//         .take(1)
//         .collect()
//         .await;
//
//     send_message(
//         &mut pool_handlers,
//         &url,
//         "@event(node:room,lane:five){\"John Doe\"}",
//     )
//     .await;
//
//     send_message(
//         &mut pool_handlers,
//         &url,
//         "@event(node:room,lane:six){\"Jane Doe\"}",
//     )
//     .await;
//
//     let first_env = Envelope::make_event(
//         String::from("room"),
//         String::from("five"),
//         Some(Value::text("John Doe")),
//     );
//
//     let second_env = Envelope::make_event(
//         String::from("room"),
//         String::from("six"),
//         Some(Value::text("Jane Doe")),
//     );
//
//     assert_eq!(
//         first_stream.recv().await.unwrap(),
//         RouterEvent::Message(first_env.into_incoming().unwrap())
//     );
//
//     assert!(timeout(Duration::from_secs(1), first_stream.recv())
//         .await
//         .is_err());
//
//     assert_eq!(
//         second_stream.recv().await.unwrap(),
//         RouterEvent::Message(second_env.into_incoming().unwrap())
//     );
//
//     assert!(timeout(Duration::from_secs(1), second_stream.recv())
//         .await
//         .is_err());
//
//     assert!(router.close().await.is_ok());
//     assert_eq!(get_request_count(&pool), 1);
//
//     let mut expected_requests = HashMap::new();
//     expected_requests.insert((url.clone(), false), 1);
//
//     assert_eq!(get_requests(&pool), expected_requests);
//     assert_eq!(pool.connections.lock().unwrap().len(), 1);
//
//     assert_eq!(first_stream.recv().await.unwrap(), RouterEvent::Stopping);
//     assert_eq!(second_stream.recv().await.unwrap(), RouterEvent::Stopping);
// }
//
// #[tokio::test]
// async fn test_route_multiple_incoming_message_to_multiple_downlinks_different_hosts_same_path() {
//     let first_url = url::Url::parse("ws://127.0.0.1/").unwrap();
//     let second_url = url::Url::parse("ws://127.0.0.2/").unwrap();
//
//     let (pool, pool_handlers_rx) = TestPool::new();
//     let mut router = SwimRouter::new(Default::default(), pool.clone());
//
//     let (first_sink, mut first_stream) =
//         open_connection(&mut router, &first_url, "building", "1").await;
//
//     let (second_sink, mut second_stream) =
//         open_connection(&mut router, &second_url, "building", "1").await;
//
//     let envelope = Envelope::sync(String::from("building"), String::from("1"));
//     let _ = first_sink.send(envelope.clone()).await.unwrap();
//     let _ = second_sink.send(envelope).await.unwrap();
//
//     let mut pool_handlers: HashMap<_, _> = ReceiverStream::new(pool_handlers_rx)
//         .take(2)
//         .collect()
//         .await;
//
//     send_message(
//         &mut pool_handlers,
//         &first_url,
//         "@event(node:building,lane:\"1\"){\"Room 101\"}",
//     )
//     .await;
//
//     send_message(
//         &mut pool_handlers,
//         &first_url,
//         "@event(node:building,lane:\"1\"){\"Room 102\"}",
//     )
//     .await;
//
//     send_message(
//         &mut pool_handlers,
//         &second_url,
//         "@event(node:building,lane:\"1\"){\"Room 201\"}",
//     )
//     .await;
//
//     send_message(
//         &mut pool_handlers,
//         &second_url,
//         "@event(node:building,lane:\"1\"){\"Room 202\"}",
//     )
//     .await;
//
//     let env_101 = Envelope::make_event(
//         String::from("building"),
//         String::from("1"),
//         Some(Value::text("Room 101")),
//     );
//
//     let env_102 = Envelope::make_event(
//         String::from("building"),
//         String::from("1"),
//         Some(Value::text("Room 102")),
//     );
//
//     let env_201 = Envelope::make_event(
//         String::from("building"),
//         String::from("1"),
//         Some(Value::text("Room 201")),
//     );
//
//     let env_202 = Envelope::make_event(
//         String::from("building"),
//         String::from("1"),
//         Some(Value::text("Room 202")),
//     );
//
//     assert_eq!(
//         first_stream.recv().await.unwrap(),
//         RouterEvent::Message(env_101.into_incoming().unwrap())
//     );
//
//     assert_eq!(
//         first_stream.recv().await.unwrap(),
//         RouterEvent::Message(env_102.into_incoming().unwrap())
//     );
//
//     assert_eq!(
//         second_stream.recv().await.unwrap(),
//         RouterEvent::Message(env_201.into_incoming().unwrap())
//     );
//
//     assert_eq!(
//         second_stream.recv().await.unwrap(),
//         RouterEvent::Message(env_202.into_incoming().unwrap())
//     );
//
//     assert!(router.close().await.is_ok());
//     assert_eq!(get_request_count(&pool), 2);
//
//     let mut expected_requests = HashMap::new();
//     expected_requests.insert((first_url.clone(), false), 1);
//     expected_requests.insert((second_url.clone(), false), 1);
//
//     assert_eq!(get_requests(&pool), expected_requests);
//     assert_eq!(pool.connections.lock().unwrap().len(), 2);
//
//     assert_eq!(first_stream.recv().await.unwrap(), RouterEvent::Stopping);
//     assert_eq!(second_stream.recv().await.unwrap(), RouterEvent::Stopping);
// }
//
// #[tokio::test]
// async fn test_route_multiple_incoming_message_to_multiple_downlinks_different_hosts_different_paths(
// ) {
//     let first_url = url::Url::parse("ws://127.0.0.1/").unwrap();
//     let second_url = url::Url::parse("ws://127.0.0.2/").unwrap();
//     let third_url = url::Url::parse("ws://127.0.0.3/").unwrap();
//
//     let (pool, pool_handlers_rx) = TestPool::new();
//     let mut router = SwimRouter::new(Default::default(), pool.clone());
//
//     let (first_sink, mut first_stream) =
//         open_connection(&mut router, &first_url, "building", "1").await;
//
//     let (second_sink, mut second_stream) =
//         open_connection(&mut router, &second_url, "room", "2").await;
//
//     let (third_sink, mut third_stream) =
//         open_connection(&mut router, &third_url, "building", "3").await;
//
//     let envelope = Envelope::sync(String::from("foo"), String::from("bar"));
//     let _ = first_sink.send(envelope.clone()).await.unwrap();
//     let _ = second_sink.send(envelope.clone()).await.unwrap();
//     let _ = third_sink.send(envelope).await.unwrap();
//
//     let mut pool_handlers: HashMap<_, _> = ReceiverStream::new(pool_handlers_rx)
//         .take(3)
//         .collect()
//         .await;
//
//     send_message(
//         &mut pool_handlers,
//         &first_url,
//         "@event(node:building,lane:\"1\"){\"Building 101\"}",
//     )
//     .await;
//
//     send_message(
//         &mut pool_handlers,
//         &first_url,
//         "@event(node:building,lane:\"1\"){\"Building 102\"}",
//     )
//     .await;
//
//     send_message(
//         &mut pool_handlers,
//         &second_url,
//         "@event(node:room,lane:\"2\"){\"Room 201\"}",
//     )
//     .await;
//
//     send_message(
//         &mut pool_handlers,
//         &second_url,
//         "@event(node:room,lane:\"2\"){\"Room 202\"}",
//     )
//     .await;
//
//     send_message(
//         &mut pool_handlers,
//         &second_url,
//         "@event(node:room,lane:\"2\"){\"Room 203\"}",
//     )
//     .await;
//
//     send_message(
//         &mut pool_handlers,
//         &third_url,
//         "@event(node:building,lane:\"3\"){\"Building 301\"}",
//     )
//     .await;
//
//     send_message(
//         &mut pool_handlers,
//         &third_url,
//         "@event(node:building,lane:\"3\"){\"Building 302\"}",
//     )
//     .await;
//
//     let building_101 = Envelope::make_event(
//         String::from("building"),
//         String::from("1"),
//         Some(Value::text("Building 101")),
//     );
//
//     let building_102 = Envelope::make_event(
//         String::from("building"),
//         String::from("1"),
//         Some(Value::text("Building 102")),
//     );
//
//     let room_201 = Envelope::make_event(
//         String::from("room"),
//         String::from("2"),
//         Some(Value::text("Room 201")),
//     );
//
//     let room_202 = Envelope::make_event(
//         String::from("room"),
//         String::from("2"),
//         Some(Value::text("Room 202")),
//     );
//
//     let room_203 = Envelope::make_event(
//         String::from("room"),
//         String::from("2"),
//         Some(Value::text("Room 203")),
//     );
//
//     let building_301 = Envelope::make_event(
//         String::from("building"),
//         String::from("3"),
//         Some(Value::text("Building 301")),
//     );
//
//     let building_302 = Envelope::make_event(
//         String::from("building"),
//         String::from("3"),
//         Some(Value::text("Building 302")),
//     );
//
//     assert_eq!(
//         first_stream.recv().await.unwrap(),
//         RouterEvent::Message(building_101.into_incoming().unwrap())
//     );
//
//     assert_eq!(
//         first_stream.recv().await.unwrap(),
//         RouterEvent::Message(building_102.into_incoming().unwrap())
//     );
//
//     assert_eq!(
//         second_stream.recv().await.unwrap(),
//         RouterEvent::Message(room_201.into_incoming().unwrap())
//     );
//
//     assert_eq!(
//         second_stream.recv().await.unwrap(),
//         RouterEvent::Message(room_202.into_incoming().unwrap())
//     );
//
//     assert_eq!(
//         second_stream.recv().await.unwrap(),
//         RouterEvent::Message(room_203.into_incoming().unwrap())
//     );
//
//     assert_eq!(
//         third_stream.recv().await.unwrap(),
//         RouterEvent::Message(building_301.into_incoming().unwrap())
//     );
//
//     assert_eq!(
//         third_stream.recv().await.unwrap(),
//         RouterEvent::Message(building_302.into_incoming().unwrap())
//     );
//
//     assert!(router.close().await.is_ok());
//     assert_eq!(get_request_count(&pool), 3);
//
//     let mut expected_requests = HashMap::new();
//     expected_requests.insert((first_url.clone(), false), 1);
//     expected_requests.insert((second_url.clone(), false), 1);
//     expected_requests.insert((third_url.clone(), false), 1);
//
//     assert_eq!(get_requests(&pool), expected_requests);
//     assert_eq!(pool.connections.lock().unwrap().len(), 3);
//
//     assert_eq!(first_stream.recv().await.unwrap(), RouterEvent::Stopping);
//     assert_eq!(second_stream.recv().await.unwrap(), RouterEvent::Stopping);
//     assert_eq!(third_stream.recv().await.unwrap(), RouterEvent::Stopping);
// }
//
// #[tokio::test]
// async fn test_route_incoming_unsopported_message() {
//     let url = url::Url::parse("ws://127.0.0.1/").unwrap();
//
//     let (pool, pool_handlers_rx) = TestPool::new();
//     let mut router = SwimRouter::new(Default::default(), pool.clone());
//
//     let (sink, mut stream) = open_connection(&mut router, &url, "foo", "bar").await;
//
//     let envelope = Envelope::sync(String::from("foo"), String::from("bar"));
//     let _ = sink.send(envelope.clone()).await.unwrap();
//
//     let mut pool_handlers: HashMap<_, _> = ReceiverStream::new(pool_handlers_rx)
//         .take(1)
//         .collect()
//         .await;
//
//     send_message(&mut pool_handlers, &url, "@auth()").await;
//
//     assert!(timeout(Duration::from_secs(1), stream.recv())
//         .await
//         .is_err());
//
//     send_message(&mut pool_handlers, &url, "@event(node:foo,lane:bar){Hello}").await;
//
//     let expected_env = Envelope::make_event(
//         String::from("foo"),
//         String::from("bar"),
//         Some(Value::text("Hello")),
//     );
//
//     assert_eq!(
//         stream.recv().await.unwrap(),
//         RouterEvent::Message(expected_env.into_incoming().unwrap())
//     );
//
//     assert!(router.close().await.is_ok());
//     assert_eq!(get_request_count(&pool), 1);
//
//     let mut expected_requests = HashMap::new();
//     expected_requests.insert((url.clone(), false), 1);
//
//     assert_eq!(get_requests(&pool), expected_requests);
//     assert_eq!(pool.connections.lock().unwrap().len(), 1);
//
//     assert_eq!(stream.recv().await.unwrap(), RouterEvent::Stopping);
// }
//
// #[tokio::test]
// async fn test_route_incoming_message_of_no_interest() {
//     let url = url::Url::parse("ws://127.0.0.1/").unwrap();
//
//     let (pool, pool_handlers_rx) = TestPool::new();
//     let mut router = SwimRouter::new(Default::default(), pool.clone());
//
//     let (sink, mut stream) = open_connection(&mut router, &url, "foo", "bar").await;
//
//     let envelope = Envelope::sync(String::from("foo"), String::from("bar"));
//     let _ = sink.send(envelope.clone()).await.unwrap();
//
//     let mut pool_handlers: HashMap<_, _> = ReceiverStream::new(pool_handlers_rx)
//         .take(1)
//         .collect()
//         .await;
//
//     send_message(
//         &mut pool_handlers,
//         &url,
//         "@event(node:building,lane:swim){Second}",
//     )
//     .await;
//
//     assert!(timeout(Duration::from_secs(1), stream.recv())
//         .await
//         .is_err());
//
//     send_message(&mut pool_handlers, &url, "@event(node:foo,lane:bar){Hello}").await;
//
//     let expected_env = Envelope::make_event(
//         String::from("foo"),
//         String::from("bar"),
//         Some(Value::text("Hello")),
//     );
//
//     assert_eq!(
//         stream.recv().await.unwrap(),
//         RouterEvent::Message(expected_env.into_incoming().unwrap())
//     );
//
//     assert!(router.close().await.is_ok());
//     assert_eq!(get_request_count(&pool), 1);
//
//     let mut expected_requests = HashMap::new();
//     expected_requests.insert((url.clone(), false), 1);
//
//     assert_eq!(get_requests(&pool), expected_requests);
//     assert_eq!(pool.connections.lock().unwrap().len(), 1);
//
//     assert_eq!(stream.recv().await.unwrap(), RouterEvent::Stopping);
// }
//
// #[tokio::test]
// async fn test_single_direct_message_existing_connection() {
//     let url = url::Url::parse("ws://127.0.0.1/").unwrap();
//
//     let (pool, pool_handlers_rx) = TestPool::new();
//     let mut router = SwimRouter::new(Default::default(), pool.clone());
//
//     let (sink, _) = open_connection(&mut router, &url, "foo", "bar").await;
//
//     let sync_env = Envelope::sync(String::from("room"), String::from("seven"));
//     let _ = sink.send(sync_env).await.unwrap();
//
//     let command_env = Envelope::make_event(
//         String::from("room"),
//         String::from("seven"),
//         Some(Value::text("Test Command")),
//     );
//
//     let general_sink = router.general_sink();
//
//     assert!(general_sink.send((url.clone(), command_env)).await.is_ok());
//
//     let mut pool_handlers: HashMap<_, _> = ReceiverStream::new(pool_handlers_rx)
//         .take(1)
//         .collect()
//         .await;
//
//     assert!(router.close().await.is_ok());
//     assert_eq!(get_request_count(&pool), 2);
//
//     let mut expected_requests = HashMap::new();
//     expected_requests.insert((url.clone(), false), 2);
//
//     assert_eq!(get_requests(&pool), expected_requests);
//
//     assert_eq!(
//         get_message(&mut pool_handlers, &url).await.unwrap(),
//         "@sync(node:room,lane:seven)".into()
//     );
//
//     assert_eq!(
//         get_message(&mut pool_handlers, &url).await.unwrap(),
//         "@event(node:room,lane:seven){\"Test Command\"}".into()
//     );
// }
//
// #[tokio::test]
// async fn test_single_direct_message_new_connection() {
//     let url = url::Url::parse("ws://127.0.0.1/").unwrap();
//
//     let (pool, pool_handlers_rx) = TestPool::new();
//     let mut router = SwimRouter::new(Default::default(), pool.clone());
//
//     let command_env = Envelope::make_event(
//         String::from("room"),
//         String::from("seven"),
//         Some(Value::text("Test Command")),
//     );
//
//     let general_sink = router.general_sink();
//
//     assert!(general_sink.send((url.clone(), command_env)).await.is_ok());
//
//     let mut pool_handlers: HashMap<_, _> = ReceiverStream::new(pool_handlers_rx)
//         .take(1)
//         .collect()
//         .await;
//
//     assert!(router.close().await.is_ok());
//     assert_eq!(get_request_count(&pool), 1);
//
//     let mut expected_requests = HashMap::new();
//     expected_requests.insert((url.clone(), false), 1);
//
//     assert_eq!(get_requests(&pool), expected_requests);
//     assert_eq!(
//         get_message(&mut pool_handlers, &url).await.unwrap(),
//         "@event(node:room,lane:seven){\"Test Command\"}".into()
//     );
// }
//
// #[tokio::test]
// async fn test_multiple_direct_messages_existing_connection() {
//     let url = url::Url::parse("ws://127.0.0.1/").unwrap();
//
//     let (pool, pool_handlers_rx) = TestPool::new();
//     let mut router = SwimRouter::new(Default::default(), pool.clone());
//
//     let (sink, _) = open_connection(&mut router, &url, "building", "swim").await;
//
//     let sync_env = Envelope::sync(String::from("building"), String::from("swim"));
//     let _ = sink.send(sync_env).await.unwrap();
//
//     let first_env = Envelope::make_event(
//         String::from("building"),
//         String::from("swim"),
//         Some(Value::text("First")),
//     );
//
//     let second_env = Envelope::make_event(
//         String::from("building"),
//         String::from("swim"),
//         Some(Value::text("Second")),
//     );
//
//     let general_sink = router.general_sink();
//
//     assert!(general_sink.send((url.clone(), first_env)).await.is_ok());
//
//     assert!(general_sink.send((url.clone(), second_env)).await.is_ok());
//
//     let mut pool_handlers: HashMap<_, _> = ReceiverStream::new(pool_handlers_rx)
//         .take(1)
//         .collect()
//         .await;
//
//     assert!(router.close().await.is_ok());
//     assert_eq!(get_request_count(&pool), 3);
//
//     let mut expected_requests = HashMap::new();
//     expected_requests.insert((url.clone(), false), 3);
//
//     assert_eq!(get_requests(&pool), expected_requests);
//
//     assert_eq!(
//         get_message(&mut pool_handlers, &url).await.unwrap(),
//         "@sync(node:building,lane:swim)".into()
//     );
//
//     assert_eq!(
//         get_message(&mut pool_handlers, &url).await.unwrap(),
//         "@event(node:building,lane:swim){First}".into()
//     );
//
//     assert_eq!(
//         get_message(&mut pool_handlers, &url).await.unwrap(),
//         "@event(node:building,lane:swim){Second}".into()
//     );
// }
//
// #[tokio::test]
// async fn test_multiple_direct_messages_new_connection() {
//     let url = url::Url::parse("ws://127.0.0.1/").unwrap();
//
//     let (pool, pool_handlers_rx) = TestPool::new();
//     let mut router = SwimRouter::new(Default::default(), pool.clone());
//
//     let first_env = Envelope::make_event(
//         String::from("building"),
//         String::from("swim"),
//         Some(Value::text("First")),
//     );
//
//     let second_env = Envelope::make_event(
//         String::from("building"),
//         String::from("swim"),
//         Some(Value::text("Second")),
//     );
//
//     let third_env = Envelope::make_event(
//         String::from("building"),
//         String::from("swim"),
//         Some(Value::text("Third")),
//     );
//
//     let general_sink = router.general_sink();
//
//     assert!(general_sink.send((url.clone(), first_env)).await.is_ok());
//
//     assert!(general_sink.send((url.clone(), second_env)).await.is_ok());
//
//     assert!(general_sink.send((url.clone(), third_env)).await.is_ok());
//
//     let mut pool_handlers: HashMap<_, _> = ReceiverStream::new(pool_handlers_rx)
//         .take(1)
//         .collect()
//         .await;
//
//     assert!(router.close().await.is_ok());
//     assert_eq!(get_request_count(&pool), 3);
//
//     let mut expected_requests = HashMap::new();
//     expected_requests.insert((url.clone(), false), 3);
//
//     assert_eq!(get_requests(&pool), expected_requests);
//
//     assert_eq!(
//         get_message(&mut pool_handlers, &url).await.unwrap(),
//         "@event(node:building,lane:swim){First}".into()
//     );
//
//     assert_eq!(
//         get_message(&mut pool_handlers, &url).await.unwrap(),
//         "@event(node:building,lane:swim){Second}".into()
//     );
//
//     assert_eq!(
//         get_message(&mut pool_handlers, &url).await.unwrap(),
//         "@event(node:building,lane:swim){Third}".into()
//     );
// }
//
// #[tokio::test]
// async fn test_multiple_direct_messages_different_connections() {
//     let first_url = url::Url::parse("ws://127.0.0.1/").unwrap();
//     let second_url = url::Url::parse("ws://127.0.0.2/").unwrap();
//
//     let (pool, pool_handlers_rx) = TestPool::new();
//     let mut router = SwimRouter::new(Default::default(), pool.clone());
//
//     let first_env = Envelope::make_event(
//         String::from("building"),
//         String::from("swim"),
//         Some(Value::text("First")),
//     );
//
//     let second_env = Envelope::make_event(
//         String::from("building"),
//         String::from("swim"),
//         Some(Value::text("Second")),
//     );
//
//     let third_env = Envelope::make_event(
//         String::from("building"),
//         String::from("swim"),
//         Some(Value::text("Third")),
//     );
//
//     let general_sink = router.general_sink();
//
//     assert!(general_sink
//         .send((first_url.clone(), first_env))
//         .await
//         .is_ok());
//
//     assert!(general_sink
//         .send((second_url.clone(), second_env))
//         .await
//         .is_ok());
//
//     assert!(general_sink
//         .send((first_url.clone(), third_env))
//         .await
//         .is_ok());
//
//     let mut pool_handlers: HashMap<_, _> = ReceiverStream::new(pool_handlers_rx)
//         .take(2)
//         .collect()
//         .await;
//
//     assert!(router.close().await.is_ok());
//     assert_eq!(get_request_count(&pool), 3);
//
//     let mut expected_requests = HashMap::new();
//     expected_requests.insert((first_url.clone(), false), 2);
//     expected_requests.insert((second_url.clone(), false), 1);
//
//     assert_eq!(get_requests(&pool), expected_requests);
//
//     assert_eq!(
//         get_message(&mut pool_handlers, &first_url).await.unwrap(),
//         "@event(node:building,lane:swim){First}".into()
//     );
//
//     assert_eq!(
//         get_message(&mut pool_handlers, &second_url).await.unwrap(),
//         "@event(node:building,lane:swim){Second}".into()
//     );
//
//     assert_eq!(
//         get_message(&mut pool_handlers, &&first_url).await.unwrap(),
//         "@event(node:building,lane:swim){Third}".into()
//     );
// }
//
// #[tokio::test]
// async fn test_router_close_ok() {
//     let (pool, _) = TestPool::new();
//     let router = SwimRouter::new(Default::default(), pool.clone());
//
//     assert!(router.close().await.is_ok());
// }
//
// #[tokio::test]
// async fn test_router_close_error() {
//     let (pool, _) = TestPool::new();
//     let router = SwimRouter::new(Default::default(), pool.clone());
//
//     let SwimRouter {
//         router_connection_request_tx,
//         router_sink_tx,
//         task_manager_handle,
//         connection_pool,
//         close_tx: _,
//         configuration,
//     } = router;
//
//     let (close_tx, close_rx) = promise::promise();
//
//     drop(close_rx);
//
//     let router = SwimRouter {
//         router_connection_request_tx,
//         router_sink_tx,
//         task_manager_handle,
//         connection_pool,
//         close_tx,
//         configuration,
//     };
//
//     assert!(router.close().await.is_err());
// }
//
// #[tokio::test]
// async fn test_route_incoming_parse_message_error() {
//     let url = url::Url::parse("ws://127.0.0.1/").unwrap();
//
//     let (pool, pool_handlers_rx) = TestPool::new();
//     let mut router = SwimRouter::new(Default::default(), pool.clone());
//
//     let (sink, mut stream) = open_connection(&mut router, &url, "foo", "bar").await;
//
//     let envelope = Envelope::sync(String::from("foo"), String::from("bar"));
//     let _ = sink.send(envelope.clone()).await.unwrap();
//
//     let mut pool_handlers: HashMap<_, _> = ReceiverStream::new(pool_handlers_rx)
//         .take(1)
//         .collect()
//         .await;
//
//     send_message(&mut pool_handlers, &url, "|").await;
//
//     assert!(timeout(Duration::from_secs(1), stream.recv())
//         .await
//         .is_err());
//
//     send_message(&mut pool_handlers, &url, "@event(node:foo,lane:bar){Hello}").await;
//
//     let expected_env = Envelope::make_event(
//         String::from("foo"),
//         String::from("bar"),
//         Some(Value::text("Hello")),
//     );
//
//     assert_eq!(
//         stream.recv().await.unwrap(),
//         RouterEvent::Message(expected_env.into_incoming().unwrap())
//     );
//
//     assert!(router.close().await.is_ok());
//     assert_eq!(get_request_count(&pool), 1);
//
//     let mut expected_requests = HashMap::new();
//     expected_requests.insert((url.clone(), false), 1);
//
//     assert_eq!(get_requests(&pool), expected_requests);
//     assert_eq!(pool.connections.lock().unwrap().len(), 1);
//
//     assert_eq!(stream.recv().await.unwrap(), RouterEvent::Stopping);
// }
//
// #[tokio::test]
// async fn test_route_incoming_parse_envelope_error() {
//     let url = url::Url::parse("ws://127.0.0.1/").unwrap();
//
//     let (pool, pool_handlers_rx) = TestPool::new();
//     let mut router = SwimRouter::new(Default::default(), pool.clone());
//
//     let (sink, mut stream) = open_connection(&mut router, &url, "foo", "bar").await;
//
//     let envelope = Envelope::sync(String::from("foo"), String::from("bar"));
//     let _ = sink.send(envelope.clone()).await.unwrap();
//
//     let mut pool_handlers: HashMap<_, _> = ReceiverStream::new(pool_handlers_rx)
//         .take(1)
//         .collect()
//         .await;
//
//     send_message(&mut pool_handlers, &url, "@invalid(node:oof,lane:rab){bye}").await;
//
//     assert!(timeout(Duration::from_secs(1), stream.recv())
//         .await
//         .is_err());
//
//     assert!(router.close().await.is_ok());
//     assert_eq!(get_request_count(&pool), 1);
//
//     let mut expected_requests = HashMap::new();
//     expected_requests.insert((url.clone(), false), 1);
//
//     assert_eq!(get_requests(&pool), expected_requests);
//     assert_eq!(pool.connections.lock().unwrap().len(), 1);
//
//     assert_eq!(stream.recv().await.unwrap(), RouterEvent::Stopping);
// }
//
// #[tokio::test]
// async fn test_route_incoming_unreachable_host() {
//     let url = url::Url::parse("ws://unreachable/").unwrap();
//
//     let (pool, _) = TestPool::new();
//     let mut router = SwimRouter::new(Default::default(), pool.clone());
//
//     let (sink, mut stream) = open_connection(&mut router, &url, "foo", "bar").await;
//
//     let envelope = Envelope::sync(String::from("foo"), String::from("bar"));
//     let _ = sink.send(envelope.clone()).await.unwrap();
//
//     assert_eq!(
//         stream.recv().await.unwrap(),
//         RouterEvent::Unreachable("Malformatted URI. ws://unreachable/".to_string())
//     );
//
//     assert_eq!(get_request_count(&pool), 1);
//     let mut expected_requests = HashMap::new();
//     expected_requests.insert((url.clone(), false), 1);
//     assert_eq!(get_requests(&pool), expected_requests);
// }
//
// #[tokio::test]
// async fn test_route_incoming_connection_closed_single() {
//     let url = url::Url::parse("ws://127.0.0.1/").unwrap();
//
//     let (pool, pool_handlers_rx) = TestPool::new();
//     let mut router = SwimRouter::new(Default::default(), pool.clone());
//
//     let (sink, mut stream) = open_connection(&mut router, &url, "foo", "bar").await;
//
//     let envelope = Envelope::sync(String::from("foo"), String::from("bar"));
//     let _ = sink.send(envelope.clone()).await.unwrap();
//
//     let mut pool_handlers: HashMap<_, _> = ReceiverStream::new(pool_handlers_rx)
//         .take(1)
//         .collect()
//         .await;
//
//     drop(pool_handlers.remove(&url).unwrap());
//
//     assert_eq!(stream.recv().await.unwrap(), RouterEvent::ConnectionClosed);
//
//     assert!(router.close().await.is_ok());
//     assert_eq!(get_request_count(&pool), 1);
//
//     let mut expected_requests = HashMap::new();
//     expected_requests.insert((url.clone(), false), 1);
//
//     assert_eq!(get_requests(&pool), expected_requests);
//     assert_eq!(pool.connections.lock().unwrap().len(), 1);
//
//     assert_eq!(stream.recv().await.unwrap(), RouterEvent::Stopping);
// }
//
// #[tokio::test]
// async fn test_route_incoming_connection_closed_multiple_same_host() {
//     let url = url::Url::parse("ws://127.0.0.1/").unwrap();
//
//     let (pool, pool_handlers_rx) = TestPool::new();
//     let mut router = SwimRouter::new(Default::default(), pool.clone());
//
//     let (first_sink, mut first_stream) = open_connection(&mut router, &url, "foo", "bar").await;
//     let (_, mut second_stream) = open_connection(&mut router, &url, "oof", "rab").await;
//
//     let envelope = Envelope::sync(String::from("foo"), String::from("bar"));
//     let _ = first_sink.send(envelope.clone()).await.unwrap();
//
//     let mut pool_handlers: HashMap<_, _> = ReceiverStream::new(pool_handlers_rx)
//         .take(1)
//         .collect()
//         .await;
//
//     drop(pool_handlers.remove(&url).unwrap());
//
//     assert_eq!(
//         first_stream.recv().await.unwrap(),
//         RouterEvent::ConnectionClosed
//     );
//     assert_eq!(
//         second_stream.recv().await.unwrap(),
//         RouterEvent::ConnectionClosed
//     );
//
//     assert!(router.close().await.is_ok());
//     assert_eq!(get_request_count(&pool), 1);
//
//     let mut expected_requests = HashMap::new();
//     expected_requests.insert((url.clone(), false), 1);
//
//     assert_eq!(get_requests(&pool), expected_requests);
//     assert_eq!(pool.connections.lock().unwrap().len(), 1);
//
//     assert_eq!(first_stream.recv().await.unwrap(), RouterEvent::Stopping);
//     assert_eq!(second_stream.recv().await.unwrap(), RouterEvent::Stopping);
// }
//
// #[tokio::test]
// async fn test_route_incoming_connection_closed_multiple_different_hosts() {
//     let first_url = url::Url::parse("ws://127.0.0.1/").unwrap();
//     let second_url = url::Url::parse("ws://127.0.0.2/").unwrap();
//
//     let (pool, pool_handlers_rx) = TestPool::new();
//     let mut router = SwimRouter::new(Default::default(), pool.clone());
//
//     let (first_sink, mut first_stream) =
//         open_connection(&mut router, &first_url, "foo", "bar").await;
//     let (second_sink, mut second_stream) =
//         open_connection(&mut router, &second_url, "foo", "bar").await;
//
//     let envelope = Envelope::sync(String::from("foo"), String::from("bar"));
//     let _ = first_sink.send(envelope.clone()).await.unwrap();
//     let _ = second_sink.send(envelope.clone()).await.unwrap();
//
//     let mut pool_handlers: HashMap<_, _> = ReceiverStream::new(pool_handlers_rx)
//         .take(2)
//         .collect()
//         .await;
//
//     drop(pool_handlers.remove(&first_url).unwrap());
//
//     assert_eq!(
//         first_stream.recv().await.unwrap(),
//         RouterEvent::ConnectionClosed
//     );
//
//     assert!(timeout(Duration::from_secs(1), second_stream.recv())
//         .await
//         .is_err());
//
//     assert!(router.close().await.is_ok());
//     assert_eq!(get_request_count(&pool), 2);
//
//     let mut expected_requests = HashMap::new();
//     expected_requests.insert((first_url.clone(), false), 1);
//     expected_requests.insert((second_url.clone(), false), 1);
//
//     assert_eq!(get_requests(&pool), expected_requests);
//     assert_eq!(pool.connections.lock().unwrap().len(), 2);
//
//     assert_eq!(first_stream.recv().await.unwrap(), RouterEvent::Stopping);
//     assert_eq!(second_stream.recv().await.unwrap(), RouterEvent::Stopping);
// }
