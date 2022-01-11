use crate::error::ConnectionDropped;
use crate::remote::router::{
    DownlinkRoutingRequest, PlaneRoutingRequest, RemoteRoutingRequest, Router,
};
use crate::remote::RawRoute;
use crate::routing::TaggedEnvelope;
use futures_util::future::BoxFuture;
use futures_util::FutureExt;
use std::fmt::Debug;
use std::future::{ready, Future};
use swim_model::path::{Addressable, Path};
use swim_utilities::trigger::promise;
use tokio::select;
use tokio::sync::mpsc;
use tokio::task::JoinHandle;
use tokio_stream::wrappers::ReceiverStream;
use tokio_stream::StreamExt;

enum Event<Path> {
    Plane(PlaneRoutingRequest),
    Remote(RemoteRoutingRequest),
    Client(DownlinkRoutingRequest<Path>),
}

pub struct RouterService<Path, PF, RF, CF> {
    plane_cb: PF,
    remote_cb: RF,
    client_cb: CF,
    plane_rx: mpsc::Receiver<PlaneRoutingRequest>,
    remote_rx: mpsc::Receiver<RemoteRoutingRequest>,
    client_rx: mpsc::Receiver<DownlinkRoutingRequest<Path>>,
}

impl<Path, PF, RF, CF> RouterService<Path, PF, RF, CF>
where
    Path: Addressable,
    PF: RouterCallback<PlaneRoutingRequest>,
    RF: RouterCallback<RemoteRoutingRequest>,
    CF: RouterCallback<DownlinkRoutingRequest<Path>>,
{
    async fn run(self) {
        let RouterService {
            mut plane_cb,
            mut remote_cb,
            mut client_cb,
            plane_rx,
            remote_rx,
            client_rx,
        } = self;

        let mut plane_stream = ReceiverStream::new(plane_rx).fuse();
        let mut remote_stream = ReceiverStream::new(remote_rx).fuse();
        let mut client_stream = ReceiverStream::new(client_rx).fuse();

        loop {
            let item: Option<Event<Path>> = select! {
                it = plane_stream.next() => it.map(Event::Plane),
                it = remote_stream.next() => it.map(Event::Remote),
                it = client_stream.next() => it.map(Event::Client),
            };

            match item {
                Some(Event::Plane(request)) => plane_cb.call(request).await,
                Some(Event::Remote(request)) => remote_cb.call(request).await,
                Some(Event::Client(request)) => client_cb.call(request).await,
                None => break,
            }
        }
    }
}

pub trait RouterCallback<A>: Send + Sync + 'static {
    fn call(&mut self, arg: A) -> BoxFuture<()>;
}

impl<A> RouterCallback<A> for () {
    fn call(&mut self, _arg: A) -> BoxFuture<()> {
        Box::pin(ready(()))
    }
}

impl<F, A, R> RouterCallback<A> for F
where
    F: Fn(A) -> R + Send + Sync + 'static,
    R: Future<Output = ()> + Send + Sync + 'static,
{
    fn call(&mut self, arg: A) -> BoxFuture<()> {
        Box::pin((self)(arg))
    }
}

pub async fn invalid<A>(arg: A)
where
    A: Debug,
{
    panic!("Received an unexpected request: {:?}", arg);
}

pub fn empty() -> (Router<Path>, JoinHandle<()>) {
    router_fixture(
        |arg| async move { panic!("Plane router received an unexpected request: {:?}", arg) },
        |arg| async move { panic!("Remote router received an unexpected request: {:?}", arg) },
        |arg| async move { panic!("Client router received an unexpected request: {:?}", arg) },
    )
}

pub fn router_fixture<Path, PF, RF, CF>(
    plane_cb: PF,
    remote_cb: RF,
    client_cb: CF,
) -> (Router<Path>, JoinHandle<()>)
where
    Path: Addressable,
    PF: RouterCallback<PlaneRoutingRequest>,
    RF: RouterCallback<RemoteRoutingRequest>,
    CF: RouterCallback<DownlinkRoutingRequest<Path>>,
{
    let (plane_tx, plane_rx) = mpsc::channel(8);
    let (remote_tx, remote_rx) = mpsc::channel(8);
    let (client_tx, client_rx) = mpsc::channel(8);

    let service = RouterService {
        plane_rx,
        remote_rx,
        client_rx,
        plane_cb,
        remote_cb,
        client_cb,
    };

    let router = Router::server(client_tx, plane_tx, remote_tx);
    let jh = tokio::spawn(service.run());
    (router, jh)
}

struct PlaneRouterResolver {
    sender: mpsc::Sender<TaggedEnvelope>,
    drop_rx: promise::Receiver<ConnectionDropped>,
}

impl RouterCallback<PlaneRoutingRequest> for PlaneRouterResolver {
    fn call(&mut self, arg: PlaneRoutingRequest) -> BoxFuture<()> {
        match arg {
            PlaneRoutingRequest::Endpoint { request, .. } => {
                let PlaneRouterResolver { sender, drop_rx } = self;
                let _ = request.send(Ok(RawRoute::new(sender.clone(), drop_rx.clone())));
            }
            req => {
                panic!("Plane router received an unexpected request: {:?}", req)
            }
        }

        ready(()).boxed()
    }
}

pub fn plane_router_resolver(
    sender: mpsc::Sender<TaggedEnvelope>,
    drop_rx: promise::Receiver<ConnectionDropped>,
) -> (Router<Path>, JoinHandle<()>) {
    router_fixture(
        PlaneRouterResolver { sender, drop_rx },
        |arg| async move { panic!("Remote router received an unexpected request: {:?}", arg) },
        |arg| async move { panic!("Client router received an unexpected request: {:?}", arg) },
    )
}

struct ClientRouterResolver {
    sender: mpsc::Sender<TaggedEnvelope>,
    drop_rx: promise::Receiver<ConnectionDropped>,
}

impl RouterCallback<DownlinkRoutingRequest<Path>> for ClientRouterResolver {
    fn call(&mut self, arg: DownlinkRoutingRequest<Path>) -> BoxFuture<()> {
        match arg {
            DownlinkRoutingRequest::Endpoint { request, .. } => {
                let ClientRouterResolver { sender, drop_rx } = self;
                let _ = request.send(Ok(RawRoute::new(sender.clone(), drop_rx.clone())));
            }
            req => {
                panic!("Downlink router received an unexpected request: {:?}", req)
            }
        }

        ready(()).boxed()
    }
}

pub fn client_router_resolver(
    sender: mpsc::Sender<TaggedEnvelope>,
    drop_rx: promise::Receiver<ConnectionDropped>,
) -> (Router<Path>, JoinHandle<()>) {
    router_fixture(
        |arg| async move { panic!("Remote router received an unexpected request: {:?}", arg) },
        |arg| async move { panic!("Client router received an unexpected request: {:?}", arg) },
        ClientRouterResolver { sender, drop_rx },
    )
}

struct RemoteRouterResolver {
    sender: mpsc::Sender<TaggedEnvelope>,
    drop_rx: promise::Receiver<ConnectionDropped>,
}

impl RouterCallback<RemoteRoutingRequest> for RemoteRouterResolver {
    fn call(&mut self, arg: RemoteRoutingRequest) -> BoxFuture<()> {
        match arg {
            RemoteRoutingRequest::Endpoint { request, .. } => {
                let RemoteRouterResolver { sender, drop_rx } = self;
                let _ = request.send(Ok(RawRoute::new(sender.clone(), drop_rx.clone())));
            }
            req => {
                panic!("Remote router received an unexpected request: {:?}", req)
            }
        }

        ready(()).boxed()
    }
}

pub fn remote_router_resolver(
    sender: mpsc::Sender<TaggedEnvelope>,
    drop_rx: promise::Receiver<ConnectionDropped>,
) -> (Router<Path>, JoinHandle<()>) {
    router_fixture(
        |arg| async move { panic!("Remote router received an unexpected request: {:?}", arg) },
        RemoteRouterResolver { sender, drop_rx },
        |arg| async move { panic!("Client router received an unexpected request: {:?}", arg) },
    )
}
