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

use crate::error::{
    ConnectionDropped, ConnectionError, ResolutionError, RouterError, RoutingError,
};
use std::convert::TryFrom;
use std::error::Error;

use crate::compat::{
    ClientMessageDecoder, EnvelopeEncoder, MessageDecodeError, Notification, ResponseMessage,
};
use crate::remote::RawOutRoute;
use bytes::Buf;
use futures::future::BoxFuture;
use futures::future::{ready, select, Either};
use futures::stream::unfold;
use futures::StreamExt;
use futures::{FutureExt, Stream};
use futures_util::SinkExt;
use std::fmt::{Display, Formatter};
use std::io::ErrorKind;
use swim_form::structural::read::recognizer::RecognizerReadable;
use swim_model::Value;
use swim_utilities::future::item_sink::{ItemSink, SendError};
use swim_utilities::io::byte_channel::{ByteReader, ByteWriter};
use swim_utilities::routing::uri::RelativeUri;
use swim_utilities::trigger::promise;
use swim_warp::envelope::{Envelope, RequestEnvelope, ResponseEnvelope};
use tokio::sync::mpsc;
use tokio_stream::wrappers::ReceiverStream;
use tokio_util::codec::{FramedRead, FramedWrite};
use url::Url;
use uuid::Uuid;

pub type CloseReceiver = promise::Receiver<mpsc::Sender<Result<(), RoutingError>>>;
pub type CloseSender = promise::Sender<mpsc::Sender<Result<(), RoutingError>>>;

#[cfg(test)]
mod tests;

/// A key into the server routing table specifying an endpoint to which [`Envelope`]s can be sent.
/// This is deliberately non-descriptive to allow it to be [`Copy`] and so very cheap to use as a
/// key.
type Location = Uuid;

/// An opaque routing address.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct RoutingAddr(Location);

const REMOTE: u8 = 0;
const PLANE: u8 = 1;
const CLIENT: u8 = 2;

impl RoutingAddr {
    const fn new(tag: u8, id: u32) -> Self {
        let mut uuid_as_int = id as u128;
        uuid_as_int |= (tag as u128) << 120;
        RoutingAddr(Uuid::from_u128(uuid_as_int))
    }

    pub const fn remote(id: u32) -> Self {
        RoutingAddr::new(REMOTE, id)
    }

    pub const fn plane(id: u32) -> Self {
        RoutingAddr::new(PLANE, id)
    }

    pub const fn client(id: u32) -> Self {
        RoutingAddr::new(CLIENT, id)
    }

    pub fn is_plane(&self) -> bool {
        let RoutingAddr(inner) = self;
        inner.as_bytes()[0] == PLANE
    }

    pub fn is_remote(&self) -> bool {
        let RoutingAddr(inner) = self;
        inner.as_bytes()[0] == REMOTE
    }

    pub fn is_client(&self) -> bool {
        let RoutingAddr(inner) = self;
        inner.as_bytes()[0] == CLIENT
    }

    pub fn uuid(&self) -> &Uuid {
        &self.0
    }

    fn get_location(&self) -> u32 {
        let mut slice = &self.0.as_bytes()[12..];
        slice.get_u32()
    }
}

impl Display for RoutingAddr {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        let location = self.get_location();
        match self.0.as_bytes()[0] {
            REMOTE => write!(f, "Remote({})", location),
            PLANE => write!(f, "Plane({})", location),
            _ => write!(f, "Client({})", location),
        }
    }
}

impl From<RoutingAddr> for Uuid {
    fn from(addr: RoutingAddr) -> Self {
        addr.0
    }
}

#[derive(Debug)]
pub struct InvalidRoutingAddr(Uuid);

impl Display for InvalidRoutingAddr {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "{} is not a valid routing address.", &self.0)
    }
}

impl std::error::Error for InvalidRoutingAddr {}

impl TryFrom<Uuid> for RoutingAddr {
    type Error = InvalidRoutingAddr;

    fn try_from(value: Uuid) -> Result<Self, Self::Error> {
        if value.as_bytes()[0] <= CLIENT {
            Ok(RoutingAddr(value))
        } else {
            Err(InvalidRoutingAddr(value))
        }
    }
}

/// An [`Envelope`] tagged with the key of the endpoint into routing table from which it originated.
#[derive(Debug, Clone, PartialEq)]
pub struct TaggedEnvelope(pub RoutingAddr, pub Envelope);

/// An [`RequestEnvelope`] tagged with the key of the endpoint into routing table from which it
/// originated.
#[derive(Debug, Clone, PartialEq)]
pub struct TaggedClientEnvelope(pub RoutingAddr, pub RequestEnvelope);

impl TaggedClientEnvelope {
    pub fn lane(&self) -> &str {
        self.1.path().lane.as_str()
    }
}

#[derive(Debug)]
pub struct TaggedByteChannel {
    channel: FramedWrite<ByteWriter, EnvelopeEncoder>,
}

impl TaggedByteChannel {
    pub fn new(addr: RoutingAddr, channel: ByteWriter) -> Self {
        TaggedByteChannel {
            channel: FramedWrite::new(channel, EnvelopeEncoder::new(addr)),
        }
    }

    pub fn is_closed(&self) -> bool {
        self.channel.get_ref().is_closed()
    }

    pub async fn send(&mut self, envelope: Envelope) -> Result<(), std::io::Error> {
        let TaggedByteChannel { channel } = self;
        channel.send(envelope).await
    }
}

#[derive(Debug)]
enum RouteSender {
    Mpsc(TaggedSender),
    ByteChannel(TaggedByteChannel),
}

/// A single entry in the router consisting of a sender that will push envelopes to the endpoint
/// and a promise that will be satisfied when the endpoint closes.
#[derive(Debug)]
pub struct Route {
    sender: RouteSender,
    on_drop: promise::Receiver<ConnectionDropped>,
}

/// A extended router entry for a client (downlink). In addition to the [`Route`] for sending
/// messages, there is also a receiver to which incoming messages for the downlink will be routed.
#[derive(Debug)]
pub struct ClientRoute {
    // Route to which outgoingmessages to are sent.
    route: Route,
    // Incoming messages for the client.
    receiver: ClientEndpoint,
}

#[derive(Debug)]
enum ClientReceiver {
    Mpsc(mpsc::Receiver<TaggedEnvelope>),
    ByteChannel(ByteReader),
}

/// Additional component for a router endpoint associated with a client.
#[derive(Debug)]
pub struct ClientEndpoint {
    // Receiver for incoming messages for this client.
    receiver: ClientReceiver,
    // Promise that will be satisfied after the channel corresponding to the receiver is dropped.
    rx_on_dropped: promise::Receiver<ConnectionDropped>,
    // When this handle is dropped, the task with the responsibility of routing messages to this
    // client will be informed that it is no longer active.
    handle_drop: ClientRouteMonitor,
}

/// The Router events are emitted by the connection streams of the router and indicate
/// messages or errors from the remote host.
#[derive(Debug, Clone, PartialEq)]
pub enum RouterEvent {
    // Incoming message from a remote host.
    Message(ResponseEnvelope),
    // There was an error in the connection. If a retry strategy exists this will trigger it.
    ConnectionClosed,
    /// The remote host is unreachable. This will not trigger the retry system.
    Unreachable(String),
    // The router is stopping.
    Stopping,
}

struct RouteStream<S> {
    host: Option<Url>,
    stream: S,
    on_dropped: Option<promise::Receiver<ConnectionDropped>>,
    _monitor: ClientRouteMonitor,
    stop_trigger: CloseReceiver,
}

impl<S> RouteStream<S> {
    fn new(
        host: Option<Url>,
        stream: S,
        on_dropped: promise::Receiver<ConnectionDropped>,
        monitor: ClientRouteMonitor,
        stop_trigger: CloseReceiver,
    ) -> Self {
        RouteStream {
            host,
            stream,
            on_dropped: Some(on_dropped),
            _monitor: monitor,
            stop_trigger,
        }
    }
}

impl ClientEndpoint {
    pub fn into_stream(
        self,
        host: Option<Url>,
        stop_trigger: CloseReceiver,
    ) -> impl Stream<Item = RouterEvent> {
        let ClientEndpoint {
            receiver,
            rx_on_dropped,
            handle_drop,
            ..
        } = self;
        match receiver {
            ClientReceiver::Mpsc(receiver) => Either::Left(mpsc_client_stream(
                receiver,
                rx_on_dropped,
                handle_drop,
                host,
                stop_trigger,
            )),
            ClientReceiver::ByteChannel(receiver) => Either::Right(bytes_channel_client_stream(
                receiver,
                rx_on_dropped,
                handle_drop,
                host,
                stop_trigger,
            )),
        }
    }
}

enum ChannelFailure {
    ChannelBroken,
    InvalidFrame,
}

fn bytes_channel_client_stream(
    receiver: ByteReader,
    rx_on_dropped: promise::Receiver<ConnectionDropped>,
    handle_drop: ClientRouteMonitor,
    host: Option<Url>,
    stop_trigger: CloseReceiver,
) -> impl Stream<Item = RouterEvent> {
    let decoder = ClientMessageDecoder::new(Value::make_recognizer());
    let framed = FramedRead::new(receiver, decoder);
    let stream = framed.map(|result| match result {
        Ok(message) => {
            let ResponseMessage { path, envelope, .. } = message;

            let envelope = match envelope {
                Notification::Linked => ResponseEnvelope::Linked(path, Default::default(), None),
                Notification::Synced => ResponseEnvelope::Synced(path, None),
                Notification::Unlinked(_) => ResponseEnvelope::Unlinked(path, None),
                Notification::Event(body) => ResponseEnvelope::Event(path, Some(body)),
            };
            Ok(envelope)
        }
        Err(MessageDecodeError::Io(_)) => Err(ChannelFailure::ChannelBroken),
        Err(_) => Err(ChannelFailure::InvalidFrame),
    });
    client_stream(stream, rx_on_dropped, handle_drop, host, stop_trigger)
}

fn mpsc_client_stream(
    receiver: mpsc::Receiver<TaggedEnvelope>,
    rx_on_dropped: promise::Receiver<ConnectionDropped>,
    handle_drop: ClientRouteMonitor,
    host: Option<Url>,
    stop_trigger: CloseReceiver,
) -> impl Stream<Item = RouterEvent> {
    let stream = ReceiverStream::new(receiver)
        .filter_map(|TaggedEnvelope(_, env)| ready(env.into_response().map(Ok)));
    client_stream(stream, rx_on_dropped, handle_drop, host, stop_trigger)
}

fn client_stream<S>(
    stream: S,
    rx_on_dropped: promise::Receiver<ConnectionDropped>,
    handle_drop: ClientRouteMonitor,
    host: Option<Url>,
    stop_trigger: CloseReceiver,
) -> impl Stream<Item = RouterEvent>
where
    S: Stream<Item = Result<ResponseEnvelope, ChannelFailure>> + Unpin,
{
    let seed = RouteStream::new(host, stream, rx_on_dropped, handle_drop, stop_trigger);

    unfold(seed, |mut rs| async move {
        let RouteStream {
            host,
            stream,
            on_dropped,
            stop_trigger,
            ..
        } = &mut rs;
        if let Some(dropped) = on_dropped {
            let event = match select(stream.next(), stop_trigger).await {
                Either::Left((Some(Ok(env)), _)) => RouterEvent::Message(env),
                Either::Left((Some(Err(ChannelFailure::InvalidFrame)), _)) => {
                    *on_dropped = None;
                    RouterEvent::ConnectionClosed
                }
                Either::Left(_) => {
                    let result = dropped.await;
                    *on_dropped = None;
                    if let Ok(reason) = result {
                        match &*reason {
                            ConnectionDropped::Failed(ConnectionError::Resolution(name)) => {
                                RouterEvent::Unreachable(name.clone())
                            }
                            ConnectionDropped::Failed(ConnectionError::Io(e)) => {
                                match (e.kind(), host) {
                                    (ErrorKind::NotFound, Some(host)) => {
                                        RouterEvent::Unreachable(host.to_string())
                                    }
                                    _ => RouterEvent::ConnectionClosed,
                                }
                            }
                            ConnectionDropped::Failed(_) => RouterEvent::ConnectionClosed,
                            _ => RouterEvent::ConnectionClosed,
                        }
                    } else {
                        RouterEvent::ConnectionClosed
                    }
                }
                Either::Right(_) => {
                    *on_dropped = None;
                    RouterEvent::Stopping
                }
            };
            Some((event, rs))
        } else {
            None
        }
    })
}

/// A client route that cannot be directly route to. This type of client route is attached to a
/// remote socket and so does not have a routing address of its own. (By contrast, a client
/// connected to a local lane can be routed to directly by the agent to which it is connected).
#[derive(Debug)]
pub struct UnroutableClient {
    route: RawOutRoute,
    receiver: mpsc::Receiver<TaggedEnvelope>,
    rx_on_dropped: promise::Receiver<ConnectionDropped>,
    handle_drop: ClientRouteMonitor,
}

impl ClientRoute {
    pub fn new(
        route: Route,
        receiver: mpsc::Receiver<TaggedEnvelope>,
        rx_on_dropped: promise::Receiver<ConnectionDropped>,
        handle_drop: ClientRouteMonitor,
    ) -> Self {
        ClientRoute {
            route,
            receiver: ClientEndpoint {
                receiver: ClientReceiver::Mpsc(receiver),
                rx_on_dropped,
                handle_drop,
            },
        }
    }

    pub fn new_bytes(
        route: Route,
        receiver: ByteReader,
        rx_on_dropped: promise::Receiver<ConnectionDropped>,
        handle_drop: ClientRouteMonitor,
    ) -> Self {
        ClientRoute {
            route,
            receiver: ClientEndpoint {
                receiver: ClientReceiver::ByteChannel(receiver),
                rx_on_dropped,
                handle_drop,
            },
        }
    }

    pub fn split(self) -> (Route, ClientEndpoint) {
        let ClientRoute { route, receiver } = self;
        (route, receiver)
    }
}

/// A client route monitor keeps track of whether a client route is being used. A downlink that
/// has a client route should make sure that it its monitor is not dropped until it has stopped.
/// This is used to notify that task that is routing messags to the downlink that it can stop.
#[derive(Debug)]
pub struct ClientRouteMonitor(Option<promise::Sender<ConnectionDropped>>);

impl ClientRouteMonitor {
    pub fn new(sender: promise::Sender<ConnectionDropped>) -> Self {
        ClientRouteMonitor(Some(sender))
    }
}

impl Drop for ClientRouteMonitor {
    fn drop(&mut self) {
        if let Some(tx) = self.0.take() {
            let _ = tx.provide(ConnectionDropped::Closed);
        }
    }
}

impl UnroutableClient {
    pub fn new(
        route: RawOutRoute,
        receiver: mpsc::Receiver<TaggedEnvelope>,
        rx_on_dropped: promise::Receiver<ConnectionDropped>,
        handle_drop: promise::Sender<ConnectionDropped>,
    ) -> Self {
        UnroutableClient {
            route,
            receiver,
            rx_on_dropped,
            handle_drop: ClientRouteMonitor(Some(handle_drop)),
        }
    }

    pub fn make_client(self, addr: RoutingAddr) -> ClientRoute {
        let UnroutableClient {
            route: RawOutRoute { sender, on_drop },
            receiver,
            rx_on_dropped,
            handle_drop,
        } = self;
        ClientRoute::new(
            Route::new(TaggedSender::new(addr, sender), on_drop),
            receiver,
            rx_on_dropped,
            handle_drop,
        )
    }
}

#[derive(Debug, Copy, Clone, PartialEq, Eq)]
pub struct SendFailed;

impl Display for SendFailed {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "Envelope could not be routed.")
    }
}

impl Error for SendFailed {}

impl Route {
    pub fn new(sender: TaggedSender, on_drop: promise::Receiver<ConnectionDropped>) -> Self {
        Route {
            sender: RouteSender::Mpsc(sender),
            on_drop,
        }
    }

    pub fn new_bytes(
        sender: TaggedByteChannel,
        on_drop: promise::Receiver<ConnectionDropped>,
    ) -> Self {
        Route {
            sender: RouteSender::ByteChannel(sender),
            on_drop,
        }
    }

    pub fn is_closed(&self) -> bool {
        let Route { sender, .. } = self;
        match sender {
            RouteSender::Mpsc(tx) => tx.is_closed(),
            RouteSender::ByteChannel(tx) => tx.is_closed(),
        }
    }

    pub async fn send_item(&mut self, envelope: Envelope) -> Result<(), SendFailed> {
        let Route { sender, .. } = self;
        match sender {
            RouteSender::Mpsc(tx) => tx.send_item(envelope).await.map_err(|_| SendFailed),
            RouteSender::ByteChannel(tx) => tx.send(envelope).await.map_err(|_| SendFailed),
        }
    }

    pub async fn terminated(self) -> ConnectionDropped {
        let Route { on_drop, .. } = self;
        on_drop
            .await
            .map(|reason| (*reason).clone())
            .unwrap_or(ConnectionDropped::Unknown)
    }
}

/// Trait for routers capable of resolving addresses and returning connections to them.
/// The connections can only be used to send [`Envelope`]s to the corresponding addresses.
pub trait Router: Send + Sync {
    /// Given a routing address, resolve the corresponding router entry
    /// consisting of a sender that will push envelopes to the endpoint.
    fn resolve_sender(&mut self, addr: RoutingAddr) -> BoxFuture<Result<Route, ResolutionError>>;

    /// Find and return the corresponding routing address of an endpoint for a given route.
    fn lookup(
        &mut self,
        host: Option<Url>,
        route: RelativeUri,
    ) -> BoxFuture<Result<RoutingAddr, RouterError>>;
}

/// Create router instances bound to particular routing addresses.
pub trait RouterFactory: Send + Sync {
    type Router: Router + 'static;

    /// Create a new router for a given routing address.
    fn create_for(&self, addr: RoutingAddr) -> Self::Router;

    /// Find and return the corresponding routing address of an endpoint for a given route.
    fn lookup(
        &mut self,
        host: Option<Url>,
        route: RelativeUri,
    ) -> BoxFuture<Result<RoutingAddr, RouterError>>;
}

/// Sender that attaches a [`RoutingAddr`] to received envelopes before sending them over a channel.
#[derive(Debug, Clone)]
pub struct TaggedSender {
    tag: RoutingAddr,
    inner: mpsc::Sender<TaggedEnvelope>,
}

impl TaggedSender {
    pub fn new(tag: RoutingAddr, inner: mpsc::Sender<TaggedEnvelope>) -> Self {
        TaggedSender { tag, inner }
    }

    pub fn is_closed(&self) -> bool {
        self.inner.is_closed()
    }

    pub async fn send_item(&mut self, envelope: Envelope) -> Result<(), SendError<Envelope>> {
        Ok(self
            .inner
            .send(TaggedEnvelope(self.tag, envelope))
            .await
            .map_err(|e| {
                let TaggedEnvelope(_addr, env) = e.0;
                SendError(env)
            })?)
    }
}

impl<'a> ItemSink<'a, Envelope> for TaggedSender {
    type Error = SendError<Envelope>;
    type SendFuture = BoxFuture<'a, Result<(), Self::Error>>;

    fn send_item(&'a mut self, value: Envelope) -> Self::SendFuture {
        self.send_item(value).boxed()
    }
}

impl<'a> ItemSink<'a, Envelope> for Route {
    type Error = SendFailed;
    type SendFuture = BoxFuture<'a, Result<(), Self::Error>>;

    fn send_item(&'a mut self, value: Envelope) -> Self::SendFuture {
        async move {
            let Route { sender, .. } = self;
            match sender {
                RouteSender::Mpsc(tx) => tx.send_item(value).await.map_err(|_| SendFailed),
                RouteSender::ByteChannel(tx) => tx.send(value).await.map_err(|_| SendFailed),
            }
        }
        .boxed()
    }
}

#[derive(Debug, Clone, Copy)]
pub struct NoRoutes;

impl Router for NoRoutes {
    fn resolve_sender(&mut self, addr: RoutingAddr) -> BoxFuture<Result<Route, ResolutionError>> {
        async move { Err(ResolutionError::Unresolvable(addr)) }.boxed()
    }

    fn lookup(
        &mut self,
        host: Option<Url>,
        route: RelativeUri,
    ) -> BoxFuture<Result<RoutingAddr, RouterError>> {
        async move {
            if let Some(url) = host {
                Err(RouterError::ConnectionFailure(ConnectionError::Resolution(
                    url.to_string(),
                )))
            } else {
                Err(RouterError::NoAgentAtRoute(route))
            }
        }
        .boxed()
    }
}

impl RouterFactory for NoRoutes {
    type Router = NoRoutes;

    fn create_for(&self, _addr: RoutingAddr) -> Self::Router {
        NoRoutes
    }

    fn lookup(
        &mut self,
        host: Option<Url>,
        route: RelativeUri,
    ) -> BoxFuture<Result<RoutingAddr, RouterError>> {
        Router::lookup(self, host, route)
    }
}
