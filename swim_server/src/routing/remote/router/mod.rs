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

use crate::plane::error::ResolutionError;
use crate::routing::remote::RoutingRequest;
use crate::routing::{error, Route, RoutingAddr, ServerRouter, TaggedEnvelope};
use futures::future::BoxFuture;
use futures::FutureExt;
use swim_common::request::Request;
use swim_common::routing::RoutingError;
use swim_common::sink::item::{ItemSink, MpscSend};
use swim_common::warp::envelope::Envelope;
use swim_common::ws::error::ConnectionError;
use tokio::sync::{mpsc, oneshot};
use url::Url;
use utilities::uri::RelativeUri;

#[derive(Debug)]
pub struct RemoteRouter {
    tag: RoutingAddr,
    request_tx: mpsc::Sender<RoutingRequest>,
}

impl RemoteRouter {
    pub fn new(tag: RoutingAddr, request_tx: mpsc::Sender<RoutingRequest>) -> Self {
        RemoteRouter { tag, request_tx }
    }
}

pub struct RemoteRouterSender {
    tag: RoutingAddr,
    inner: mpsc::Sender<TaggedEnvelope>,
}

impl RemoteRouterSender {
    fn new(tag: RoutingAddr, inner: mpsc::Sender<TaggedEnvelope>) -> Self {
        RemoteRouterSender { tag, inner }
    }
}

impl<'a> ItemSink<'a, Envelope> for RemoteRouterSender {
    type Error = error::SendError;
    type SendFuture = MpscSend<'a, TaggedEnvelope, error::SendError>;

    fn send_item(&'a mut self, envelope: Envelope) -> Self::SendFuture {
        let RemoteRouterSender { tag, inner } = self;
        MpscSend::new(inner, TaggedEnvelope(*tag, envelope))
    }
}

impl ServerRouter for RemoteRouter {
    type Sender = RemoteRouterSender;

    fn get_sender(
        &mut self,
        addr: RoutingAddr,
    ) -> BoxFuture<'_, Result<Route<Self::Sender>, RoutingError>> {
        async move {
            let RemoteRouter { tag, request_tx } = self;
            let (tx, rx) = oneshot::channel();
            let request = Request::new(tx);
            let routing_req = RoutingRequest::Endpoint { addr, request };
            if request_tx.send(routing_req).await.is_err() {
                Err(RoutingError::RouterDropped)
            } else {
                match rx.await {
                    Ok(Ok(Route { sender, on_drop })) => {
                        Ok(Route::new(RemoteRouterSender::new(*tag, sender), on_drop))
                    }
                    Ok(Err(_)) => Err(RoutingError::PoolError(ConnectionError::Closed)),
                    Err(_) => Err(RoutingError::RouterDropped),
                }
            }
        }
        .boxed()
    }

    fn resolve(
        &mut self,
        host: Option<Url>,
        _route: RelativeUri,
    ) -> BoxFuture<'_, Result<RoutingAddr, ResolutionError>> {
        async move {
            let RemoteRouter { request_tx, .. } = self;
            if let Some(url) = host {
                let (tx, rx) = oneshot::channel();
                let request = Request::new(tx);
                let routing_req = RoutingRequest::ResolveUrl { host: url, request };
                if request_tx.send(routing_req).await.is_err() {
                    Err(ResolutionError::NoRoute(RoutingError::RouterDropped))
                } else {
                    match rx.await {
                        Ok(Ok(addr)) => Ok(addr),
                        Ok(Err(err)) => Err(ResolutionError::NoRoute(RoutingError::PoolError(err))),
                        Err(_) => Err(ResolutionError::NoRoute(RoutingError::RouterDropped)),
                    }
                }
            } else {
                //TODO Add a more appropriate error variant or delegate.
                Err(ResolutionError::NoRoute(RoutingError::ConnectionError))
            }
        }
        .boxed()
    }
}
