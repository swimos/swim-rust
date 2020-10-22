// Copyright 2015-2020 SWIM.AI inc.
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

use crate::routing::error::{ResolutionError, RouterError};
use crate::routing::remote::RoutingRequest;
use crate::routing::{error, Route, RoutingAddr, ServerRouter, TaggedSender};
use futures::future::{BoxFuture, Either};
use futures::FutureExt;
use swim_common::request::Request;
use swim_common::sink::item::{ItemSender, ItemSink};
use swim_common::warp::envelope::Envelope;
use tokio::sync::{mpsc, oneshot};
use url::Url;
use utilities::uri::RelativeUri;

#[derive(Debug)]
pub struct RemoteRouter<Delegate> {
    tag: RoutingAddr,
    delegate_router: Delegate,
    request_tx: mpsc::Sender<RoutingRequest>,
}

impl<Delegate> RemoteRouter<Delegate> {
    pub fn new(
        tag: RoutingAddr,
        delegate_router: Delegate,
        request_tx: mpsc::Sender<RoutingRequest>,
    ) -> Self {
        RemoteRouter {
            tag,
            delegate_router,
            request_tx,
        }
    }
}

enum RemoteRouterSenderInner<D> {
    Remote(TaggedSender),
    Delegated(D),
}

pub struct RemoteRouterSender<D>(RemoteRouterSenderInner<D>);

impl<D: ItemSender<Envelope, error::SendError>> RemoteRouterSender<D> {
    fn new(sender: TaggedSender) -> Self {
        RemoteRouterSender(RemoteRouterSenderInner::Remote(sender))
    }

    fn delegate(sender: D) -> Self {
        RemoteRouterSender(RemoteRouterSenderInner::Delegated(sender))
    }
}

impl<'a, D: ItemSink<'a, Envelope, Error = error::SendError>> ItemSink<'a, Envelope>
    for RemoteRouterSender<D>
{
    type Error = error::SendError;
    type SendFuture = Either<<TaggedSender as ItemSink<'a, Envelope>>::SendFuture, D::SendFuture>;

    fn send_item(&'a mut self, envelope: Envelope) -> Self::SendFuture {
        match self {
            RemoteRouterSender(RemoteRouterSenderInner::Remote(sender)) => {
                Either::Left(sender.send_item(envelope))
            }
            RemoteRouterSender(RemoteRouterSenderInner::Delegated(delegate)) => {
                Either::Right(delegate.send_item(envelope))
            }
        }
    }
}

impl<Delegate: ServerRouter> ServerRouter for RemoteRouter<Delegate> {
    type Sender = RemoteRouterSender<Delegate::Sender>;

    fn resolve_sender(
        &mut self,
        addr: RoutingAddr,
    ) -> BoxFuture<'_, Result<Route<Self::Sender>, ResolutionError>> {
        async move {
            let RemoteRouter {
                tag,
                delegate_router,
                request_tx,
            } = self;
            let (tx, rx) = oneshot::channel();
            let request = Request::new(tx);
            let routing_req = RoutingRequest::Endpoint { addr, request };
            if request_tx.send(routing_req).await.is_err() {
                Err(ResolutionError::RouterDropped)
            } else {
                match rx.await {
                    Ok(Ok(Route { sender, on_drop })) => Ok(Route::new(
                        RemoteRouterSender::new(TaggedSender::new(*tag, sender)),
                        on_drop,
                    )),
                    Ok(Err(_)) => match delegate_router.resolve_sender(addr).await {
                        Ok(Route { sender, on_drop }) => {
                            Ok(Route::new(RemoteRouterSender::delegate(sender), on_drop))
                        }
                        Err(err) => Err(err),
                    },
                    Err(_) => Err(ResolutionError::RouterDropped),
                }
            }
        }
        .boxed()
    }

    fn lookup(
        &mut self,
        host: Option<Url>,
        route: RelativeUri,
    ) -> BoxFuture<'_, Result<RoutingAddr, RouterError>> {
        async move {
            let RemoteRouter {
                request_tx,
                delegate_router,
                ..
            } = self;
            if let Some(url) = host {
                let (tx, rx) = oneshot::channel();
                let request = Request::new(tx);
                let routing_req = RoutingRequest::ResolveUrl { host: url, request };
                if request_tx.send(routing_req).await.is_err() {
                    Err(RouterError::RouterDropped)
                } else {
                    match rx.await {
                        Ok(Ok(addr)) => Ok(addr),
                        Ok(Err(err)) => Err(RouterError::ConnectionFailure(err)),
                        Err(_) => Err(RouterError::RouterDropped),
                    }
                }
            } else {
                delegate_router.lookup(host, route).await
            }
        }
        .boxed()
    }
}
